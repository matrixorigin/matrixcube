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

type adminResult struct {
	adminType          rpc.AdminCmdType
	configChangeResult *configChangeResult
	splitResult        *splitResult
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
	pr.logger.Debug("apply committied entries finished",
		zap.Uint64("applied", pr.appliedIndex),
		zap.Uint64("last", pr.rn.LastIndex()))

	pr.metrics.admin.incBy(result.metrics.admin)

	pr.writtenBytes += result.metrics.writtenBytes
	pr.writtenKeys += result.metrics.writtenKeys
	if result.hasSplitResult() {
		pr.deleteKeysHint = result.metrics.deleteKeysHint
		pr.sizeDiffHint = result.metrics.sizeDiffHint
	} else {
		pr.deleteKeysHint += result.metrics.deleteKeysHint
		pr.sizeDiffHint += result.metrics.sizeDiffHint
	}
}

func (pr *replica) handleAdminResult(result applyResult) {
	switch result.adminResult.adminType {
	case rpc.AdminCmdType_ConfigChange:
		pr.applyConfChange(result.adminResult.configChangeResult)
	case rpc.AdminCmdType_BatchSplit:
		pr.applySplit(result.adminResult.splitResult)
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

	pr.logger.Info("applied changes completed",
		log.ShardField("epoch", pr.getShard()))
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

	estimatedSize := pr.approximateSize / uint64(len(result.newShards))
	estimatedKeys := pr.approximateKeys / uint64(len(result.newShards))
	for _, shard := range result.newShards {
		// add new shard replicas to cache
		for _, p := range shard.Replicas {
			pr.store.replicaRecords.Store(p.ID, p)
		}

		newShardID := shard.ID
		newReplica := pr.store.getReplica(newShardID, false)
		if newReplica != nil {
			// Suppose the Shard s1 has three replicas A, B and C, and needs to be split into s2+s3.
			// Consider the processing logic:
			// t1: A complete the split operation. Shard s2 and s3 created.
			// t2: B receives a raft vote message of s2 from A before completing the split.
			// t3: B apply split s1.

			// Although the store will dynamically creating a replica of the Shard when receive a raft message
			// but due to we haven't updated the key range metadata, so there must be a conflict in the range
			// of the new shard and old shard.
			pr.logger.Fatal("duplicated shard split to new shard",
				log.ShardIDField(newShardID))
		}

		reason := fmt.Sprintf("split from shard %d", pr.shardID)
		newReplica, err := createReplica(pr.store, shard, reason)
		if err != nil {
			// replica information is already written into db, can't recover.
			// there is probably a bug.
			pr.logger.Fatal("fail to create new shard replica",
				log.ShardIDField(newShardID),
				zap.Error(err))
		}

		newReplica.approximateKeys = estimatedKeys
		newReplica.approximateSize = estimatedSize
		newReplica.sizeDiffHint = estimatedSize
		if !pr.store.addReplica(newReplica) {
			pr.logger.Fatal("fail to create new shard replica",
				log.ShardIDField(newShardID))
		}

		newReplica.start()
		// If this replica was the leader of the shard before split, it is expected
		// to become the leader of the new split shard.
		// The ticks are accelerated here, so that the replica for the new split shard
		// comes to campaign earlier than the other follower replicas. And then it's
		// more likely for this replica to become the leader of the new split shard.
		// If the other follower replicas applies logs too slowly, they may fail to
		// vote the `MsgRequestVote` from this replica on its campaign. In this worst
		// case scenario, the new split raft group will not be available since there
		// is no leader established during one election timeout after the split.
		if pr.isLeader() && len(shard.Replicas) > 1 {
			newReplica.addAction(action{actionType: campaignAction})
		}
		if !pr.isLeader() {
			if vote, ok := pr.store.removeDroppedVoteMsg(newReplica.shardID); ok {
				newReplica.addMessage(vote)
			}
		}
		pr.logger.Info("new shard added",
			log.ShardField("new-shard", shard))
	}

	// all new shards started, update key range atomic
	pr.store.updateShardKeyRange(result.newShards[0].Group, result.newShards...)

	// current shard not used
	pr.store.destroyReplica(pr.shardID, true, "complete split")

	if pr.aware != nil {
		pr.aware.Splited(pr.getShard())
	}
}
