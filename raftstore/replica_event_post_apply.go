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

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/rpc"
)

func (pr *replica) handleApplyResult(items []interface{}) {
	for {
		size := pr.applyResults.Len()
		if size == 0 {
			metric.SetRaftApplyResultQueueMetric(size)
			break
		}

		n, err := pr.applyResults.Get(readyBatch, items)
		if err != nil {
			return
		}

		for i := int64(0); i < n; i++ {
			result := items[i].(asyncApplyResult)
			pr.doPollApply(result)
		}

		if n < readyBatch {
			break
		}
	}
}

func (pr *replica) doPollApply(result asyncApplyResult) {
	pr.doPostApply(result)
	if result.result != nil {
		pr.doPostApplyResult(result)
	}
}

func (pr *replica) doPostApply(result asyncApplyResult) {
	pr.appliedIndex = result.index
	pr.rn.AdvanceApply(result.index)

	logger.Debugf("shard %d async apply committied entries finished at %d, last %d",
		pr.shardID,
		result.index,
		pr.rn.LastIndex())

	pr.metrics.admin.incBy(result.metrics.admin)

	pr.writtenBytes += result.metrics.writtenBytes
	pr.writtenKeys += result.metrics.writtenKeys

	if result.hasSplitExecResult() {
		pr.deleteKeysHint = result.metrics.deleteKeysHint
		pr.sizeDiffHint = result.metrics.sizeDiffHint
	} else {
		pr.deleteKeysHint += result.metrics.deleteKeysHint
		pr.sizeDiffHint += result.metrics.sizeDiffHint
	}

	pr.maybeExecRead()
}

func (pr *replica) doPostApplyResult(result asyncApplyResult) {
	switch result.result.adminType {
	case rpc.AdminCmdType_ConfigChange:
		pr.doApplyConfChange(result.result.configChangeResult)
	case rpc.AdminCmdType_BatchSplit:
		pr.doApplySplit(result.result.splitResult)
	}
}

func (pr *replica) doApplyConfChange(cp *configChangeResult) {
	if cp.index == 0 {
		// Apply failed, skip.
		return
	}

	pr.rn.ApplyConfChange(cp.confChange)

	needPing := false
	now := time.Now()
	for _, change := range cp.changes {
		changeType := change.ChangeType
		peer := change.Replica
		peerID := peer.ID

		switch changeType {
		case metapb.ConfigChangeType_AddNode, metapb.ConfigChangeType_AddLearnerNode:
			pr.replicaHeartbeatsMap.Store(peerID, now)
			pr.store.peers.Store(peerID, peer)
			if pr.isLeader() {
				needPing = true
			}
		case metapb.ConfigChangeType_RemoveNode:
			pr.replicaHeartbeatsMap.Delete(peerID)
			pr.store.peers.Delete(peerID)
		}
	}

	if pr.isLeader() {
		// Notify pd immediately.
		logger.Infof("shard %d notify pd with %+v with changes %+v at epoch %+v",
			pr.shardID,
			cp.confChange,
			cp.changes,
			pr.getShard().Epoch)
		pr.addAction(action{actionType: heartbeatAction})

		// Remove or demote leader will cause this raft group unavailable
		// until new leader elected, but we can't revert this operation
		// because its result is already persisted in apply worker
		// TODO: should we transfer leader here?
		demoteSelf := pr.replica.Role == metapb.ReplicaRole_Learner
		if demoteSelf {
			logger.Warningf("shard-%d removing or demoting leader, demote",
				pr.shardID,
				demoteSelf)

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

	shard := pr.getShard()
	logger.Infof("shard %d peer %d applied changes %+v at epoch %+v, new peers %+v",
		pr.shardID,
		pr.replica.ID,
		cp.changes,
		shard.Epoch,
		shard.Replicas)
}

func (pr *replica) doApplySplit(result *splitResult) {
	logger.Infof("shard %d update to %+v by post applt split",
		pr.shardID,
		result.derived)

	estimatedSize := pr.approximateSize / uint64(len(result.shards)+1)
	estimatedKeys := pr.approximateKeys / uint64(len(result.shards)+1)
	pr.sizeDiffHint = 0
	pr.approximateKeys = 0
	pr.approximateSize = 0
	pr.store.updateShardKeyRange(result.derived)

	if pr.isLeader() {
		pr.approximateSize = estimatedSize
		pr.approximateKeys = estimatedKeys
		pr.addAction(action{actionType: heartbeatAction})
	}

	for _, shard := range result.shards {
		// add new shard peers to cache
		for _, p := range shard.Replicas {
			pr.store.peers.Store(p.ID, p)
		}

		newShardID := shard.ID
		newPR := pr.store.getPR(newShardID, false)
		if newPR != nil {
			for _, p := range shard.Replicas {
				pr.store.peers.Store(p.ID, p)
			}

			// If the store received a raft msg with the new shard raft group
			// before splitting, it will creates a uninitialized peer.
			// We can remove this uninitialized peer directly.
			if len(newPR.getShard().Replicas) > 0 {
				logger.Fatalf("shard %d duplicated shard split to new shard %d",
					pr.shardID,
					newShardID)
			}
		}

		newPR, err := createPeerReplica(pr.store, &shard, fmt.Sprintf("split by shard %d", pr.shardID))
		if err != nil {
			// peer information is already written into db, can't recover.
			// there is probably a bug.
			logger.Fatalf("shard %d create new split shard failed with %+v",
				pr.shardID,
				err)
		}

		pr.store.updateShardKeyRange(shard)

		newPR.approximateKeys = estimatedKeys
		newPR.approximateSize = estimatedSize
		newPR.sizeDiffHint = uint64(newPR.store.cfg.Replication.ShardSplitCheckBytes)
		if !pr.store.addPR(newPR) {
			logger.Fatalf("shard %d peer %d, created by split, must add sucessful", newPR.shardID, newPR.replica.ID)
		}

		newPR.start()

		// If this peer is the leader of the shard before split, it's intuitional for
		// it to become the leader of new split shard.
		// The ticks are accelerated here, so that the peer for the new split shard
		// comes to campaign earlier than the other follower peers. And then it's more
		// likely for this peer to become the leader of the new split shard.
		// If the other follower peers applies logs too slowly, they may fail to vote the
		// `MsgRequestVote` from this peer on its campaign.
		// In this worst case scenario, the new split raft group will not be available
		// since there is no leader established during one election timeout after the split.
		if pr.isLeader() && len(shard.Replicas) > 1 {
			newPR.addAction(action{actionType: doCampaignAction})
		}

		if !pr.isLeader() {
			if vote, ok := pr.store.removeDroppedVoteMsg(newPR.shardID); ok {
				newPR.step(vote)
			}
		}
	}

	if pr.store.aware != nil {
		pr.store.aware.Splited(pr.getShard())
	}

	logger.Infof("shard %d new shard added, new shards %+v",
		pr.shardID,
		result.shards)
}
