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
	"sync"
	"time"

	"github.com/fagongzi/util/protoc"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/storage"
)

type stateMachine struct {
	shardID     uint64
	replicaID   uint64
	executorCtx *applyContext
	pr          *replica
	store       *store
	dataStorage storage.DataStorage

	metadataMu struct {
		sync.Mutex
		shard   Shard
		removed bool
		index   uint64
		term    uint64
	}
}

func (d *stateMachine) updateShard(shard Shard) {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	d.metadataMu.shard = shard
}

func (d *stateMachine) getShard() Shard {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	return d.metadataMu.shard
}

func (d *stateMachine) applyCommittedEntries(commitedEntries []raftpb.Entry) {
	if len(commitedEntries) <= 0 {
		return
	}

	start := time.Now()
	req := pb.AcquireRequestBatch()

	for _, entry := range commitedEntries {
		if d.isPendingRemove() {
			// This replica is about to be destroyed, skip everything.
			break
		}
		d.checkEntryIndexTerm(entry)

		d.executorCtx.reset(d.getShard(), entry)
		switch entry.Type {
		case raftpb.EntryNormal:
			d.applyEntry(d.executorCtx)
		case raftpb.EntryConfChange:
			d.applyConfChange(d.executorCtx)
		case raftpb.EntryConfChangeV2:
			d.applyConfChange(d.executorCtx)
		}

		asyncResult := asyncApplyResult{}
		asyncResult.shardID = d.shardID
		asyncResult.result = d.executorCtx.adminResult
		asyncResult.index = entry.Index

		if d.executorCtx != nil {
			asyncResult.metrics = d.executorCtx.metrics
		}

		d.pr.addApplyResult(asyncResult)
	}

	// only release RequestBatch. Header and Requests fields is pb created in Unmarshal
	pb.ReleaseRequestBatch(req)
	metric.ObserveRaftLogApplyDuration(start)
}

func (d *stateMachine) applyEntry(ctx *applyContext) {
	// noop entry with empty payload proposed by the raft leader at the beginning
	// of its term
	if len(ctx.entry.Data) == 0 {
		d.updateAppliedIndexTerm(ctx.entry.Index, ctx.entry.Term)
		return
	}

	protoc.MustUnmarshal(ctx.req, ctx.entry.Data)
	d.doApplyRaftCMD(ctx)
}

func (d *stateMachine) checkEntryIndexTerm(entry raftpb.Entry) {
	index, term := d.getAppliedIndexTerm()
	if index+1 != entry.Index {
		logger.Fatalf("shard %d replica %d index not match, expect=<%d> get=<%d> entry=<%+v>",
			d.shardID,
			d.replicaID,
			index,
			entry.Index,
			entry)
	}
	if term > entry.Term {
		logger.Fatalf("shard %d replica %d term moving backwards, d.term %d, entry.term %d",
			d.shardID,
			d.replicaID,
			term,
			entry.Term)
	}
}

func (d *stateMachine) applyConfChange(ctx *applyContext) {
	var v2cc raftpb.ConfChangeV2
	if ctx.entry.Type == raftpb.EntryConfChange {
		cc := raftpb.ConfChange{}
		protoc.MustUnmarshal(&cc, ctx.entry.Data)
		protoc.MustUnmarshal(ctx.req, cc.Context)
		v2cc = cc.AsV2()
	} else {
		protoc.MustUnmarshal(&v2cc, ctx.entry.Data)
		protoc.MustUnmarshal(ctx.req, v2cc.Context)
	}

	d.doApplyRaftCMD(ctx)
	if nil == ctx.adminResult {
		ctx.adminResult = &adminExecResult{
			adminType:          rpc.AdminCmdType_ConfigChange,
			configChangeResult: &configChangeResult{},
		}
		return
	}
	ctx.adminResult.configChangeResult.confChange = v2cc
}

func (d *stateMachine) doApplyRaftCMD(ctx *applyContext) {
	if sc, ok := d.store.cfg.Test.Shards[d.shardID]; ok && sc.SkipApply {
		return
	}

	if d.isPendingRemove() {
		logger.Fatalf("shard %d apply raft comand can not pending remove",
			d.shardID)
	}

	var err error
	var resp *rpc.ResponseBatch
	if !d.checkEpoch(ctx.req) {
		resp = errorStaleEpochResp(ctx.req.Header.ID, d.getShard())
	} else {
		if ctx.req.AdminRequest != nil {
			resp, err = d.execAdminRequest(ctx)
			if err != nil {
				resp = errorStaleEpochResp(ctx.req.Header.ID, d.getShard())
			}
		} else {
			resp = d.execWriteRequest(ctx)
		}
	}

	if logger.DebugEnabled() {
		for _, req := range ctx.req.Requests {
			logger2.Debug("write request completed",
				log.HexField("id", req.ID))
		}
	}

	d.updateAppliedIndexTerm(ctx.entry.Index, ctx.entry.Term)
	d.pr.pendings.notify(ctx.req.Header.ID, resp, isConfigChangeCMD(ctx.req))
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

func (d *stateMachine) checkEpoch(req *rpc.RequestBatch) bool {
	return checkEpoch(d.getShard(), req)
}

func isConfigChangeCMD(req *rpc.RequestBatch) bool {
	return nil != req.AdminRequest &&
		(req.AdminRequest.CmdType == rpc.AdminCmdType_ConfigChange ||
			req.AdminRequest.CmdType == rpc.AdminCmdType_ConfigChangeV2)
}
