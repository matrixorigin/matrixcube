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

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/errorpb"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/storage"
)

type applyContext struct {
	index       uint64
	term        uint64
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

func (ctx *applyContext) initialize(entry raftpb.Entry) {
	ctx.index = entry.Index
	ctx.term = entry.Term
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

type replicaResultHandler interface {
	handleApplyResult(applyResult)
	notifyPendingProposal(id []byte, resp rpc.ResponseBatch, isConfChange bool)
}

var _ replicaResultHandler = (*replica)(nil)

type stateMachine struct {
	logger                *zap.Logger
	shardID               uint64
	replica               Replica
	applyCtx              *applyContext
	writeCtx              *writeContext
	dataStorage           storage.DataStorage
	logdb                 logdb.LogDB
	wc                    *logdb.WorkerContext
	replicaCreatorFactory replicaCreatorFactory
	resultHandler         replicaResultHandler

	metadataMu struct {
		sync.Mutex
		shard   Shard
		removed bool
		splited bool
		index   uint64
		term    uint64
		// TODO: maybe should move to replica struct
		firstIndex uint64
	}
}

func newStateMachine(l *zap.Logger, ds storage.DataStorage, ldb logdb.LogDB,
	shard Shard, replica Replica, h replicaResultHandler,
	replicaCreatorFactory replicaCreatorFactory) *stateMachine {
	sm := &stateMachine{
		logger:                l,
		shardID:               shard.ID,
		replica:               replica,
		applyCtx:              newApplyContext(),
		writeCtx:              newWriteContext(ds),
		dataStorage:           ds,
		logdb:                 ldb,
		resultHandler:         h,
		replicaCreatorFactory: replicaCreatorFactory,
	}
	if ldb != nil {
		sm.wc = ldb.NewWorkerContext()
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

func (d *stateMachine) getConfState() raftpb.ConfState {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	cs := raftpb.ConfState{}
	for _, r := range d.metadataMu.shard.Replicas {
		if r.Role == metapb.ReplicaRole_Voter {
			cs.Voters = append(cs.Voters, r.ID)
		} else if r.Role == metapb.ReplicaRole_Learner {
			cs.Learners = append(cs.Learners, r.ID)
		} else {
			panic("unknown replica role")
		}
	}
	return cs
}

func (d *stateMachine) applyCommittedEntries(entries []raftpb.Entry) {
	if len(entries) <= 0 {
		return
	}

	d.logger.Debug("apply committed logs",
		zap.Int("count", len(entries)))
	start := time.Now()
	// FIXME: the initial idea is to batch multiple entries into the same
	// executeContext so they can be applied into the stateMachine together.
	// in the loop below, we are still applying entries one by one.
	for _, entry := range entries {
		d.applyCtx.initialize(entry)
		d.checkEntryIndexTerm(entry)
		// notify all clients that current shard has been removed or splitted
		if !d.canApply(entry) {
			if ce := d.logger.Check(zap.DebugLevel, "apply committed log skipped"); ce != nil {
				ce.Write(log.IndexField(entry.Index),
					zap.String("type", entry.Type.String()),
					log.ReasonField("continue check failed"))
			}
			d.notifyShardRemoved(d.applyCtx)
			d.updateAppliedIndexTerm(entry.Index, entry.Term)
			continue
		}
		if len(entry.Data) == 0 {
			// noop entry with empty payload proposed by the leader at the beginning
			// of its term
			d.updateAppliedIndexTerm(entry.Index, entry.Term)
			d.resultHandler.handleApplyResult(applyResult{
				index:         entry.Index,
				ignoreMetrics: true,
			})
			continue
		}

		ignoreMetrics := d.applyRequestBatch(d.applyCtx)
		result := applyResult{
			shardID:       d.shardID,
			adminResult:   d.applyCtx.adminResult,
			index:         entry.Index,
			ignoreMetrics: ignoreMetrics,
			metrics:       d.applyCtx.metrics,
		}
		if isConfigChangeEntry(entry) {
			if result.adminResult == nil {
				result.adminResult = &adminResult{
					adminType:          rpc.AdminCmdType_ConfigChange,
					configChangeResult: configChangeResult{},
				}
			} else {
				result.adminResult.configChangeResult.confChange = d.applyCtx.v2cc
			}
		}
		d.updateAppliedIndexTerm(entry.Index, entry.Term)
		d.resultHandler.handleApplyResult(result)
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

func (d *stateMachine) notifyShardRemoved(ctx *applyContext) {
	resp := errorPbResp(ctx.req.Header.ID, errorpb.Error{
		Message: errShardNotFound.Error(),
		ShardNotFound: &errorpb.ShardNotFound{
			ShardID: d.shardID,
		},
	})
	d.resultHandler.notifyPendingProposal(ctx.req.Header.ID,
		resp, isConfigChangeRequestBatch(ctx.req))
}

// applyRequestBatch returns a boolean value indicating whether to skip
// updating the metrics.
func (d *stateMachine) applyRequestBatch(ctx *applyContext) bool {
	// FIXME: update impacted tests
	// if sc, ok := d.store.cfg.Test.Shards[d.shardID]; ok && sc.SkipApply {
	//	return
	// }
	if d.isRemoved() {
		d.logger.Fatal("applying entries on removed replica")
	}
	var err error
	var resp rpc.ResponseBatch
	ignoreMetrics := true
	if !d.checkEpoch(ctx.req) {
		if ce := d.logger.Check(zap.DebugLevel, "apply committed log skipped"); ce != nil {
			ce.Write(log.IndexField(ctx.index),
				log.ReasonField("epoch check failed"),
				log.EpochField("current-epoch", d.getShard().Epoch),
				log.IndexField(ctx.index))
		}
		resp = errorStaleEpochResp(ctx.req.Header.ID, d.getShard())
	} else {
		if ce := d.logger.Check(zap.DebugLevel, "begin to apply committed log"); ce != nil {
			ce.Write(log.IndexField(ctx.index),
				log.RequestBatchField("requests", ctx.req))
		}

		if ctx.req.IsAdmin() {
			if ce := d.logger.Check(zap.DebugLevel, "apply admin request"); ce != nil {
				ce.Write(log.IndexField(ctx.index),
					zap.String("type", ctx.req.GetAdminCmdType().String()))
			}
			resp, err = d.execAdminRequest(ctx)
			if err != nil {
				resp = errorStaleEpochResp(ctx.req.Header.ID, d.getShard())
			}
		} else {
			if ce := d.logger.Check(zap.DebugLevel, "apply write requests"); ce != nil {
				ce.Write(log.IndexField(ctx.index))
			}
			ignoreMetrics = false
			resp = d.execWriteRequest(ctx)
		}

		if ce := d.logger.Check(zap.DebugLevel, "apply committed log completed"); ce != nil {
			ce.Write(log.IndexField(ctx.index),
				log.ResponseBatchField("responses", resp))
		}
	}

	// TODO: this implies that we can't have more than one batch in the
	// executeContext
	d.resultHandler.notifyPendingProposal(ctx.req.Header.ID,
		resp, isConfigChangeRequestBatch(ctx.req))
	return ignoreMetrics
}

func (d *stateMachine) close() {
	d.writeCtx.close()
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

func (d *stateMachine) setSplited() {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()
	d.metadataMu.splited = true
}

func (d *stateMachine) canApply(entry raftpb.Entry) bool {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()

	if d.metadataMu.removed {
		return false
	}

	// After restart, the removed and splited in stateMachine is false.
	// so we need check shard state.
	if !d.metadataMu.removed &&
		!d.metadataMu.splited &&
		d.metadataMu.shard.State != metapb.ResourceState_Destroying {
		return true
	}

	// In some scenarios, we need to remove the replica that is not online,
	// so that the deletion task can be completed.
	return isConfigChangeEntry(entry) &&
		d.metadataMu.shard.State == metapb.ResourceState_Destroying
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

func (d *stateMachine) getFirstIndex() uint64 {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()

	return d.metadataMu.firstIndex
}

func (d *stateMachine) setFirstIndex(index uint64) {
	d.metadataMu.Lock()
	defer d.metadataMu.Unlock()

	d.metadataMu.firstIndex = index
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
		req.GetAdminCmdType() == rpc.AdminCmdType_ConfigChange
}
