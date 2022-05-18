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

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

type applyResult struct {
	shardID       uint64
	index         uint64
	adminResult   *adminResult
	ignoreMetrics bool
	metrics       applyMetrics
}

func (res *applyResult) hasSplitResult() bool {
	return nil != res.adminResult &&
		res.adminResult.adminType == rpcpb.CmdBatchSplit
}

type adminResult struct {
	adminType            rpcpb.InternalCmd
	configChangeResult   configChangeResult
	splitResult          splitResult
	compactionResult     compactionResult
	updateMetadataResult updateMetadataResult
	updateLabelsResult   updateLabelsResult
}

type updateLabelsResult struct {
}

type updateMetadataResult struct {
	changes []raftpb.ConfChangeV2
}

type configChangeResult struct {
	index      uint64
	confChange raftpb.ConfChangeV2
	changes    []rpcpb.ConfigChangeRequest
	shard      Shard
}

type splitResult struct {
	newShards []Shard
	newLeases []*metapb.EpochLease
}

type compactionResult struct {
	index uint64
}

func (pr *replica) notifyPendingProposal(id []byte,
	resp rpcpb.ResponseBatch, isConfChange bool) {
	pr.pendingProposals.notify(id, resp, isConfChange)
}

func (pr *replica) handleApplyResult(result applyResult) {
	pr.updateAppliedIndex(result)
	if !result.ignoreMetrics {
		pr.updateMetricsHints(result)
	}
	if result.adminResult != nil {
		pr.handleAdminResult(result)
	}
}

func (pr *replica) updateAppliedIndex(result applyResult) {
	pr.appliedIndex = result.index
	pr.maybeSetLeaseReadReady()
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
		pr.stats.approximateSize += result.metrics.approximateDiffHint
	}
}

func (pr *replica) handleAdminResult(result applyResult) {
	switch result.adminResult.adminType {
	case rpcpb.CmdConfigChange:
		pr.applyConfChange(result.adminResult.configChangeResult)
	case rpcpb.CmdBatchSplit:
		pr.applySplit(result.adminResult.splitResult)
	case rpcpb.CmdCompactLog:
		pr.applyCompactionResult(result.adminResult.compactionResult)
	case rpcpb.CmdUpdateMetadata:
		pr.applyUpdateMetadataResult(result.adminResult.updateMetadataResult)
	case rpcpb.CmdUpdateLabels:
		pr.applyUpdateLabels(result.adminResult.updateLabelsResult)
	}
}

func (pr *replica) applyUpdateMetadataResult(cp updateMetadataResult) {
	for _, cc := range cp.changes {
		pr.rn.ApplyConfChange(cc)
	}
	if pr.aware != nil {
		pr.aware.Updated(pr.getShard())
	}
}

func (pr *replica) applyUpdateLabels(result updateLabelsResult) {
	if pr.aware != nil {
		pr.aware.Updated(pr.getShard())
	}
}

func (pr *replica) applyCompactionResult(r compactionResult) {
	if r.index > 0 {
		pr.addAction(action{
			actionType:  logCompactionAction,
			targetIndex: r.index,
		})
	}
}

func (pr *replica) applyConfChange(cp configChangeResult) {
	if cp.index == 0 {
		// TODO: when the entry was treated as a NoOP, configChangeResult should be
		// nil and applyConfChange() should be called in the first place.
		// Apply failed, skip.
		return
	}

	pr.logger.Debug("apply conf change result to raft",
		log.ConfigChangesField("changes-v2", cp.changes),
		log.ShardField("shard", pr.getShard()))
	pr.rn.ApplyConfChange(cp.confChange)

	needPing := false
	now := time.Now()
	for _, change := range cp.changes {
		changeType := change.ChangeType
		replica := change.Replica
		replicaID := replica.ID

		switch changeType {
		case metapb.ConfigChangeType_AddNode, metapb.ConfigChangeType_AddLearnerNode:
			if replica.StoreID == pr.storeID {
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

func (pr *replica) applySplit(result splitResult) {
	pr.logger.Info("shard split applied, current shard will destroy",
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
		withStartReplica(false, func(r *replica) {
			r.stats.approximateKeys = estimatedKeys
			r.stats.approximateSize = estimatedSize
			r.sm.updateLease(result.newLeases[0])
			result.newLeases = result.newLeases[1:]
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

	pr.startDestroyReplicaTaskAfterSplitted(pr.appliedIndex)
}
