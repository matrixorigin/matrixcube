// Copyright 2020 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package raftstore

import (
	"encoding/hex"
	"math"
	"sync"
	"time"

	"github.com/fagongzi/util/protoc"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/storage"
)

type stateMachine struct {
	shardID uint64
	peerID  uint64
	ctx     *applyContext
	pr      *peerReplica
	store   *store

	metadataMu struct {
		sync.Mutex
		shard   bhmetapb.Shard
		removed bool
		index   uint64
		term    uint64
	}
}

func (d *stateMachine) updateShard(shard bhmetapb.Shard) {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	d.metadataMu.shard = shard
}

func (d *stateMachine) getShard() bhmetapb.Shard {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	return d.metadataMu.shard
}

func (d *stateMachine) applyCommittedEntries(commitedEntries []raftpb.Entry) {
	if len(commitedEntries) <= 0 {
		return
	}

	start := time.Now()
	req := pb.AcquireRaftCMDRequest()

	for _, entry := range commitedEntries {
		if d.isPendingRemove() {
			// This peer is about to be destroyed, skip everything.
			break
		}
		d.checkEntryIndexTerm(entry)

		d.ctx.reset()
		req.Reset()

		d.ctx.req = req
		d.ctx.index = entry.Index
		d.ctx.term = entry.Term

		var result *adminExecResult
		switch entry.Type {
		case raftpb.EntryNormal:
			result = d.applyEntry(entry)
		case raftpb.EntryConfChange:
			result = d.applyConfChange(entry)
		case raftpb.EntryConfChangeV2:
			result = d.applyConfChange(entry)
		}

		asyncResult := asyncApplyResult{}
		asyncResult.shardID = d.shardID
		asyncResult.result = result
		asyncResult.index = entry.Index

		if d.ctx != nil {
			asyncResult.metrics = d.ctx.metrics
		}

		d.pr.addApplyResult(asyncResult)
	}

	// only release RaftCMDRequest. Header and Requests fields is pb created in Unmarshal
	pb.ReleaseRaftCMDRequest(req)
	metric.ObserveRaftLogApplyDuration(start)
}

func (d *stateMachine) applyEntry(entry raftpb.Entry) *adminExecResult {
	// noop entry with empty payload proposed by the raft leader at the beginning
	// of its term
	if len(entry.Data) == 0 {
		d.updateAppliedIndexTerm(entry.Index, entry.Term)
		return nil
	}

	protoc.MustUnmarshal(d.ctx.req, entry.Data)
	return d.doApplyRaftCMD()
}

func (d *stateMachine) checkEntryIndexTerm(entry raftpb.Entry) {
	index, term := d.getAppliedIndexTerm()
	if index+1 != entry.Index {
		logger.Fatalf("shard %d peer %d index not match, expect=<%d> get=<%d> entry=<%+v>",
			d.shardID,
			d.peerID,
			index,
			entry.Index,
			entry)
	}
	if term > entry.Term {
		logger.Fatalf("shard %d peer %d term moving backwards, d.term %d, entry.term %d",
			d.shardID,
			d.peerID,
			term,
			entry.Term)
	}
}

func (d *stateMachine) applyConfChange(entry raftpb.Entry) *adminExecResult {
	var v2cc raftpb.ConfChangeV2
	if entry.Type == raftpb.EntryConfChange {
		cc := raftpb.ConfChange{}
		protoc.MustUnmarshal(&cc, entry.Data)
		protoc.MustUnmarshal(d.ctx.req, cc.Context)
		v2cc = cc.AsV2()
	} else {
		protoc.MustUnmarshal(&v2cc, entry.Data)
		protoc.MustUnmarshal(d.ctx.req, v2cc.Context)
	}

	result := d.doApplyRaftCMD()
	if nil == result {
		return &adminExecResult{
			adminType:        raftcmdpb.AdminCmdType_ChangePeer,
			changePeerResult: &changePeerResult{},
		}
	}

	result.changePeerResult.confChange = v2cc
	return result
}

func (d *stateMachine) doApplyRaftCMD() *adminExecResult {
	if sc, ok := d.store.cfg.Test.Shards[d.shardID]; ok && sc.SkipApply {
		return nil
	}

	if d.isPendingRemove() {
		logger.Fatalf("shard %d apply raft comand can not pending remove",
			d.shardID)
	}

	var err error
	var resp *raftcmdpb.RaftCMDResponse
	var result *adminExecResult
	var writeBytes uint64
	var diffBytes int64
	if !d.checkEpoch(d.ctx.req) {
		resp = errorStaleEpochResp(d.ctx.req.Header.ID, d.getShard())
	} else {
		if d.ctx.req.AdminRequest != nil {
			resp, result, err = d.execAdminRequest(d.ctx)
			if err != nil {
				resp = errorStaleEpochResp(d.ctx.req.Header.ID, d.getShard())
			}
		} else {
			writeBytes, diffBytes, resp = d.execWriteRequest(d.ctx)
		}
	}

	if logger.DebugEnabled() {
		for _, req := range d.ctx.req.Requests {
			logger.Debugf("%s exec completed", hex.EncodeToString(req.ID))
		}
	}
	d.updateMetrics(writeBytes, diffBytes)
	d.updateAppliedIndexTerm(d.ctx.index, d.ctx.term)

	// TODO: remove the following write op once the storage refactoring is done
	// such write should really be issued by the KV storage layer.

	// eventually we would like the KV engine to drop the requirement of
	// persistent data storage. For now, for simplicity, we always use
	// fsync write when updating the KV data engine.
	ds := d.store.DataStorageByGroup(d.getShard().Group, d.shardID)
	if kv, ok := ds.(storage.KVStorage); ok {
		err = kv.Write(d.ctx.dataWB, true)
		if err != nil {
			logger.Fatalf("shard %d commit apply result failed with %+v",
				d.shardID,
				err)
		}
	}

	d.pr.pendings.notify(d.ctx.req.Header.ID, resp, isChangePeerCMD(d.ctx.req))

	return result
}

func (d *stateMachine) updateMetrics(writeBytes uint64, diffBytes int64) {
	d.ctx.metrics.writtenBytes += writeBytes
	if diffBytes < 0 {
		v := uint64(math.Abs(float64(diffBytes)))
		if v >= d.ctx.metrics.sizeDiffHint {
			d.ctx.metrics.sizeDiffHint = 0
		} else {
			d.ctx.metrics.sizeDiffHint -= v
		}
	} else {
		d.ctx.metrics.sizeDiffHint += uint64(diffBytes)
	}
}

func (d *stateMachine) destroy() {
	d.pr.pendings.destroy()
}

func (d *stateMachine) setPendingRemove() {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	d.metadataMu.removed = true
}

func (d *stateMachine) isPendingRemove() bool {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	return d.metadataMu.removed
}

func (d *stateMachine) setShardState(st metapb.ResourceState) {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	d.metadataMu.shard.State = st
}

func (d *stateMachine) updateAppliedIndexTerm(index uint64, term uint64) {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	d.metadataMu.index = index
	d.metadataMu.term = term
}

func (d *stateMachine) getAppliedIndexTerm() (uint64, uint64) {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	return d.metadataMu.index, d.metadataMu.term
}

func (d *stateMachine) checkEpoch(req *raftcmdpb.RaftCMDRequest) bool {
	return checkEpoch(d.getShard(), req)
}

func isChangePeerCMD(req *raftcmdpb.RaftCMDRequest) bool {
	return nil != req.AdminRequest &&
		(req.AdminRequest.CmdType == raftcmdpb.AdminCmdType_ChangePeer ||
			req.AdminRequest.CmdType == raftcmdpb.AdminCmdType_ChangePeerV2)
}
