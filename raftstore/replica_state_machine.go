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

type applyContext struct {
	index       uint64
	req         rpc.RequestBatch
	v2cc        raftpb.ConfChangeV2
	adminResult *adminResult
	metrics     applyMetrics
}

func newApplyContext() *applyContext {
	return &applyContext{
		req: rpc.RequestBatch{},
	}
}

func (ctx *applyContext) initialize(shard Shard, entry raftpb.Entry) {
	ctx.index = entry.Index
	ctx.req = rpc.RequestBatch{}
	ctx.adminResult = nil
	ctx.metrics = applyMetrics{}
	ctx.v2cc = raftpb.ConfChangeV2{}

	switch entry.Type {
	case raftpb.EntryNormal:
		// TODO: according to my current understanding, admin requests like splits
		// are marked as EntryNormal. need to confirm this.
		protoc.MustUnmarshal(&ctx.req, entry.Data)
	case raftpb.EntryConfChange:
		cc := raftpb.ConfChange{}
		protoc.MustUnmarshal(&cc, entry.Data)
		protoc.MustUnmarshal(&ctx.req, cc.Context)
		ctx.v2cc = cc.AsV2()
	case raftpb.EntryConfChangeV2:
		protoc.MustUnmarshal(&ctx.v2cc, entry.Data)
		protoc.MustUnmarshal(&ctx.req, ctx.v2cc.Context)
	default:
		panic("unknown entry type")
	}
}

type stateMachine struct {
	logger      *zap.Logger
	shardID     uint64
	replicaID   uint64
	applyCtx    *applyContext
	writeCtx    *writeContext
	replica     *replica
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
		replica:     replica,
		store:       replica.store,
		replicaID:   replica.replica.ID,
		applyCtx:    newApplyContext(),
		writeCtx:    newWriteContext(storage),
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
		if len(entry.Data) == 0 {
			// noop entry with empty payload proposed by the leader at the beginning
			// of its term
			d.updateAppliedIndexTerm(entry.Index, entry.Term)
			continue
		}

		d.applyCtx.initialize(d.getShard(), entry)
		d.applyRequestBatch(d.applyCtx)
		result := applyResult{
			shardID:     d.shardID,
			adminResult: d.applyCtx.adminResult,
			index:       entry.Index,
			metrics:     d.applyCtx.metrics,
		}
		if isConfigChangeEntry(entry) {
			if result.adminResult == nil {
				result.adminResult = &adminResult{
					adminType:          rpc.AdminCmdType_ConfigChange,
					configChangeResult: &configChangeResult{},
				}
			} else {
				result.adminResult.configChangeResult.confChange = d.applyCtx.v2cc
			}
		}
		d.updateAppliedIndexTerm(entry.Index, entry.Term)
		d.replica.handleApplyResult(result)
	}
	metric.ObserveRaftLogApplyDuration(start)
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
	d.replica.pendingProposals.notify(ctx.req.Header.ID, resp, isConfigChangeRequestBatch(ctx.req))
}

// FIXME: move this out of the state machine
func (d *stateMachine) destroy() {
	d.replica.pendingProposals.destroy()
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

func isConfigChangeEntry(entry raftpb.Entry) bool {
	return entry.Type == raftpb.EntryConfChange ||
		entry.Type == raftpb.EntryConfChangeV2
}

func isConfigChangeRequestBatch(req rpc.RequestBatch) bool {
	return req.IsAdmin() &&
		(req.AdminRequest.CmdType == rpc.AdminCmdType_ConfigChange ||
			req.AdminRequest.CmdType == rpc.AdminCmdType_ConfigChangeV2)
}
