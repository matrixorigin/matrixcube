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
	"fmt"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpc"
)

type applyResult struct {
	shardID     uint64
	index       uint64
	adminResult *adminResult
	metrics     applyMetrics
}

func (res *applyResult) hasSplitResult() bool {
	return nil != res.adminResult && res.adminResult.splitResult != nil
}

// TODO: should probably use values not pointers
type adminResult struct {
	adminType          rpc.AdminCmdType
	configChangeResult *configChangeResult
	splitResult        *splitResult
	compactionResult   *compactionResult
}

type configChangeResult struct {
	index      uint64
	confChange raftpb.ConfChangeV2
	changes    []rpc.ConfigChangeRequest
	shard      Shard
}

type splitResult struct {
	newShards []Shard
}

type compactionResult struct {
	index uint64
}

func (pr *replica) notifyPendingProposal(id []byte,
	resp rpc.ResponseBatch, isConfChange bool) {
	pr.pendingProposals.notify(id, resp, isConfChange)
}

func (pr *replica) handleApplyResult(result applyResult) {
	pr.updateAppliedIndex(result)
	pr.updateMetricsHints(result)
	if result.adminResult != nil {
		pr.handleAdminResult(result)
	}
}

func (pr *replica) updateAppliedIndex(result applyResult) {
	pr.appliedIndex = result.index
	pr.maybeExecRead()
}

func (pr *replica) updateMetricsHints(result applyResult) {
	pr.metrics.admin.incBy(result.metrics.admin)

	pr.stats.writtenBytes += result.metrics.writtenBytes
	pr.stats.writtenKeys += result.metrics.writtenKeys
	if result.hasSplitResult() {
		pr.stats.deleteKeysHint = result.metrics.deleteKeysHint
		pr.stats.approximateSize = result.metrics.approximateDiffHint
	} else {
		pr.stats.deleteKeysHint += result.metrics.deleteKeysHint
		pr.stats.approximateSize = result.metrics.approximateDiffHint
	}
}

func (pr *replica) handleAdminResult(result applyResult) {
	switch result.adminResult.adminType {
	case rpc.AdminCmdType_ConfigChange:
		pr.applyConfChange(result.adminResult.configChangeResult)
	case rpc.AdminCmdType_BatchSplit:
		pr.applySplit(result.adminResult.splitResult)
	case rpc.AdminCmdType_CompactLog:
		pr.applyCompactionResult(result.adminResult.compactionResult)
	}
}

func (pr *replica) applyCompactionResult(r *compactionResult) {
	// update LogReader's range info to make the compacted entries invisible to
	// raft.
	if err := pr.lr.Compact(r.index); err != nil {
		if err != raft.ErrCompacted {
			// TODO: check whether any error should be tolerated.
			panic(err)
		}
	}
	if err := pr.logdb.RemoveEntriesTo(pr.shardID, pr.replicaID, r.index); err != nil {
		panic(err)
	}
}

func (pr *replica) applyConfChange(cp *configChangeResult) {
	if cp.index == 0 {
		// TODO: when the entry was treated as a NoOP, configChangeResult should be
		// nil and applyConfChange() should be called in the first place.
		// Apply failed, skip.
		return
	}

	pr.rn.ApplyConfChange(cp.confChange)

	needPing := false
	now := time.Now()
	for _, change := range cp.changes {
		changeType := change.ChangeType
		replica := change.Replica
		replicaID := replica.ID

		switch changeType {
		case metapb.ConfigChangeType_AddNode, metapb.ConfigChangeType_AddLearnerNode:
			if replica.ContainerID == pr.storeID {
				pr.replica = replica
			}
			pr.replicaHeartbeatsMap.Store(replicaID, now)
			pr.store.replicaRecords.Store(replicaID, replica)
			if pr.isLeader() {
				needPing = true
			}
		case metapb.ConfigChangeType_RemoveNode:
			pr.replicaHeartbeatsMap.Delete(replicaID)
			pr.store.replicaRecords.Delete(replicaID)
		}
	}

	if pr.isLeader() {
		// Notify prophet immediately.
		pr.logger.Info("notify conf changes to prophet",
			log.ConfigChangesField("changes-v2", cp.changes),
			log.EpochField("epoch", pr.getShard().Epoch))
		pr.prophetHeartbeat()
		// Remove or demote leader will cause this raft group unavailable
		// until new leader elected, but we can't revert this operation
		// because its result is already persisted in apply worker
		// TODO: should we transfer leader here?
		demoteSelf := pr.replica.Role == metapb.ReplicaRole_Learner
		if demoteSelf {
			pr.logger.Warn("removing or demoting leader",
				zap.Bool("demote-self", demoteSelf))

			if demoteSelf {
				pr.rn.BecomeFollower(pr.rn.Status().Term, 0)
			}
			// Don't ping to speed up leader election
			needPing = false
		}

		if needPing {
			// Speed up snapshot instead of waiting another heartbeat.
			pr.rn.Ping()
		}
	}

	if pr.store.aware != nil {
		pr.store.aware.Updated(pr.getShard())
	}

	pr.logger.Info("applied changes completed",
		log.ReplicaField("replica", pr.replica),
		log.ShardField("shard", pr.getShard()))
}

func (pr *replica) applySplit(result *splitResult) {
	pr.logger.Info("shard split applied, current shard will destory",
		zap.Int("new-shards-count", len(result.newShards)))

	if ce := pr.logger.Check(zap.DebugLevel, "shard split detail"); ce != nil {
		var fields []zap.Field
		fields = append(fields, log.ShardField("old", pr.getShard()))
		for idx, s := range result.newShards {
			fields = append(fields, log.ShardField(fmt.Sprintf("new-%d", idx), s))
		}
		ce.Write(fields...)
	}

	// we consider the split to be roughly even, so we calculate the current estimated size of the shard
	// based on the number of new shards.
	estimatedSize := pr.stats.approximateSize / uint64(len(result.newShards))
	estimatedKeys := pr.stats.approximateKeys / uint64(len(result.newShards))

	isLeader := pr.isLeader()
	reason := fmt.Sprintf("create by shard %d splitted", pr.shardID)
	newReplicaCreator(pr.store).
		withReason(reason).
		withStartReplica(func(r *replica) {
			r.stats.approximateKeys = estimatedKeys
			r.stats.approximateSize = estimatedSize
		}, func(r *replica) {
			shard := r.getShard()
			if isLeader && len(shard.Replicas) > 1 {
				r.addAction(action{actionType: campaignAction})
			}
			if !isLeader {
				if vote, ok := pr.store.removeDroppedVoteMsg(r.shardID); ok {
					r.addMessage(vote)
				}
			}

			pr.logger.Info("new shard added",
				log.ShardField("new-shard", shard))
		}).
		create(result.newShards)

	if pr.aware != nil {
		pr.aware.Splited(pr.getShard())
	}

	pr.startDestoryReplicaTaskAfterSplitted(pr.appliedIndex)
}
