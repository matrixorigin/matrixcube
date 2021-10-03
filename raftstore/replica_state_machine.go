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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/storage"
)

type stateMachine struct {
	logger      *zap.Logger
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

func newStateMachine(replica *replica, shard Shard) *stateMachine {
	storage := replica.store.DataStorageByGroup(shard.Group)
	sm := &stateMachine{
		logger:      replica.logger,
		pr:          replica,
		store:       replica.store,
		replicaID:   replica.replica.ID,
		executorCtx: newApplyContext(storage),
		dataStorage: storage,
	}
	sm.metadataMu.shard = shard
	return sm
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

func (d *stateMachine) applyCommittedEntries(entries []raftpb.Entry) {
	if len(entries) <= 0 {
		return
	}

	start := time.Now()
	// FIXME: the initial idea is to batch multiple entries into the same
	// executeContext so they can be applied into the stateMachine together.
	// in the loop below, we are still applying entries one by one.
	for _, entry := range entries {
		if d.isRemoved() {
			// replica is about to be destroyed, skip
			break
		}

		d.checkEntryIndexTerm(entry)
		d.executorCtx.initialize(d.getShard(), entry)
		switch entry.Type {
		case raftpb.EntryNormal:
			d.applyEntry(d.executorCtx)
		case raftpb.EntryConfChange:
			d.applyConfChange(d.executorCtx)
		case raftpb.EntryConfChangeV2:
			d.applyConfChange(d.executorCtx)
		}
		d.updateAppliedIndexTerm(entry.Index, entry.Term)

		result := applyResult{
			shardID:     d.shardID,
			adminResult: d.executorCtx.adminResult,
			index:       entry.Index,
		}
		if d.executorCtx != nil {
			result.metrics = d.executorCtx.metrics
		}
		d.pr.handleApplyResult(result)
	}
	metric.ObserveRaftLogApplyDuration(start)
}

func (d *stateMachine) applyEntry(ctx *applyContext) {
	// noop entry with empty payload proposed by the leader at the beginning
	// of its term
	if len(ctx.entry.Data) == 0 {
		return
	}

	protoc.MustUnmarshal(&ctx.req, ctx.entry.Data)
	d.applyRequestBatch(ctx)
}

func (d *stateMachine) checkEntryIndexTerm(entry raftpb.Entry) {
	index, term := d.getAppliedIndexTerm()
	if index+1 != entry.Index {
		d.logger.Fatal("unexpected committed entry index",
			zap.Uint64("applied", index),
			zap.Uint64("entry", entry.Index))
	}
	if term > entry.Term {
		d.logger.Fatal("term moving backwards",
			zap.Uint64("applied", term),
			zap.Uint64("entry", entry.Term))
	}
}

func (d *stateMachine) applyConfChange(ctx *applyContext) {
	var v2cc raftpb.ConfChangeV2
	if ctx.entry.Type == raftpb.EntryConfChange {
		cc := raftpb.ConfChange{}
		protoc.MustUnmarshal(&cc, ctx.entry.Data)
		protoc.MustUnmarshal(&ctx.req, cc.Context)
		v2cc = cc.AsV2()
	} else {
		protoc.MustUnmarshal(&v2cc, ctx.entry.Data)
		protoc.MustUnmarshal(&ctx.req, v2cc.Context)
	}

	d.applyRequestBatch(ctx)
	if nil == ctx.adminResult {
		ctx.adminResult = &adminResult{
			adminType:          rpc.AdminCmdType_ConfigChange,
			configChangeResult: &configChangeResult{},
		}
		return
	}
	ctx.adminResult.configChangeResult.confChange = v2cc
}

func (d *stateMachine) applyRequestBatch(ctx *applyContext) {
	if sc, ok := d.store.cfg.Test.Shards[d.shardID]; ok && sc.SkipApply {
		return
	}
	if d.isRemoved() {
		d.logger.Fatal("applying entries on remove replica")
	}

	var err error
	var resp rpc.ResponseBatch
	if !d.checkEpoch(ctx.req) {
		resp = errorStaleEpochResp(ctx.req.Header.ID, d.getShard())
	} else {
		if ctx.req.IsAdmin() {
			resp, err = d.execAdminRequest(ctx)
			if err != nil {
				resp = errorStaleEpochResp(ctx.req.Header.ID, d.getShard())
			}
		} else {
			resp = d.execWriteRequest(ctx)
		}
	}
	for _, req := range ctx.req.Requests {
		if ce := d.logger.Check(zapcore.DebugLevel, "apply write/admin req completed"); ce != nil {
			ce.Write(log.HexField("id", req.ID))
		}
	}
	// TODO: this implies that we can't have more than one batch in the executeContext
	d.pr.pendingProposals.notify(ctx.req.Header.ID, resp, isConfigChangeRequestBatch(ctx.req))
}

// FIXME: move this out of the state machine
func (d *stateMachine) destroy() {
	d.pr.pendingProposals.destroy()
	d.executorCtx.close()
}

func (d *stateMachine) setRemoved() {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	d.metadataMu.removed = true
}

func (d *stateMachine) isRemoved() bool {
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

func (d *stateMachine) checkEpoch(req rpc.RequestBatch) bool {
	return checkEpoch(d.getShard(), req)
}

func isConfigChangeRequestBatch(req rpc.RequestBatch) bool {
	return req.IsAdmin() &&
		(req.AdminRequest.CmdType == rpc.AdminCmdType_ConfigChange ||
			req.AdminRequest.CmdType == rpc.AdminCmdType_ConfigChangeV2)
}
