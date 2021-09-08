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
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
)

func (pr *peerReplica) handleApplyResult(items []interface{}) {
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

func (pr *peerReplica) doPollApply(result asyncApplyResult) {
	pr.doPostApply(result)
	if result.result != nil {
		pr.doPostApplyResult(result)
	}
}

func (pr *peerReplica) doPostApply(result asyncApplyResult) {
	pr.appliedIndex = result.applyState.AppliedIndex
	pr.rn.AdvanceApply(result.applyState.AppliedIndex)

	logger.Debugf("shard %d async apply committied entries finished at %d, last %d",
		pr.shardID,
		result.applyState.AppliedIndex,
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

func (pr *peerReplica) doPostApplyResult(result asyncApplyResult) {
	switch result.result.adminType {
	case raftcmdpb.AdminCmdType_ChangePeer:
		pr.doApplyConfChange(result.result.changePeer)
	case raftcmdpb.AdminCmdType_BatchSplit:
		pr.doApplySplit(result.result.splitResult)
	case raftcmdpb.AdminCmdType_CompactLog:
		pr.doApplyCompactRaftLog(result.result.raftGCResult)
	}
}

func (pr *peerReplica) doApplyConfChange(cp *changePeer) {
	if cp.index == 0 {
		// Apply failed, skip.
		return
	}

	pr.rn.ApplyConfChange(cp.confChange)
	pr.shard = cp.shard

	remove_self := false
	need_ping := false
	now := time.Now()
	for _, change := range cp.changes {
		change_type := change.ChangeType
		peer := change.Peer
		store_id := peer.ContainerID
		peer_id := peer.ID

		switch change_type {
		case metapb.ChangePeerType_AddNode, metapb.ChangePeerType_AddLearnerNode:
			pr.peerHeartbeatsMap.Store(peer_id, now)
			pr.store.peers.Store(peer_id, peer)
			if pr.isLeader() {
				need_ping = true
			}
		case metapb.ChangePeerType_RemoveNode:
			pr.peerHeartbeatsMap.Delete(peer_id)
			pr.store.peers.Delete(peer_id)

			// We only care remove itself now.
			if pr.store.meta.meta.ID == store_id {
				if pr.peer.ID == peer_id {
					remove_self = true
				} else {
					logger.Fatalf("shard-%d trying to remove unknown peer %+v",
						pr.shardID,
						peer)
				}
			}
		}
	}

	if pr.isLeader() {
		// Notify pd immediately.
		logger.Infof("shard %d notify pd with %+v with changes %+v at epoch %+v",
			pr.shardID,
			cp.confChange,
			cp.changes,
			pr.shard.Epoch)
		pr.addAction(action{actionType: heartbeatAction})

		// Remove or demote leader will cause this raft group unavailable
		// until new leader elected, but we can't revert this operation
		// because its result is already persisted in apply worker
		// TODO: should we transfer leader here?
		demote_self := pr.peer.Role == metapb.PeerRole_Learner
		if remove_self || demote_self {
			logger.Warningf("shard-%d removing or demoting leader, remove %+v, demote",
				pr.shardID,
				remove_self,
				demote_self)

			if demote_self {
				pr.rn.BecomeFollower(pr.rn.Status().Term, 0)
			}

			// Don't ping to speed up leader election
			need_ping = false
		}

		if need_ping {
			// Speed up snapshot instead of waiting another heartbeat.
			pr.rn.Ping()
		}

		if remove_self {
			pr.mustDestroy("conf change")
		}
	}

	logger.Infof("shard %d peer %d applied changes %+v at epoch %+v, new peers %+v",
		pr.shardID,
		pr.peer.ID,
		cp.changes,
		pr.shard.Epoch,
		pr.shard.Peers)
}

func (pr *peerReplica) doApplySplit(result *splitResult) {
	logger.Infof("shard %d update to %+v by post applt split",
		pr.shard.ID,
		result.derived)

	estimatedSize := pr.approximateSize / uint64(len(result.shards)+1)
	estimatedKeys := pr.approximateKeys / uint64(len(result.shards)+1)
	pr.shard = result.derived
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
		for _, p := range shard.Peers {
			pr.store.peers.Store(p.ID, p)
		}

		newShardID := shard.ID
		newPR := pr.store.getPR(newShardID, false)
		if newPR != nil {
			for _, p := range shard.Peers {
				pr.store.peers.Store(p.ID, p)
			}

			// If the store received a raft msg with the new shard raft group
			// before splitting, it will creates a uninitialized peer.
			// We can remove this uninitialized peer directly.
			if len(newPR.shard.Peers) > 0 {
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
			logger.Fatalf("shard %d peer %d, created by split, must add sucessful", newPR.shardID, newPR.peer.ID)
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
		if pr.isLeader() && len(shard.Peers) > 1 {
			newPR.addAction(action{actionType: doCampaignAction})
		}

		if !pr.isLeader() {
			if vote, ok := pr.store.removeDroppedVoteMsg(newPR.shardID); ok {
				newPR.step(vote)
			}
		}
	}

	if pr.store.aware != nil {
		pr.store.aware.Splited(pr.shard)
	}

	logger.Infof("shard %d new shard added, new shards %+v",
		pr.shardID,
		result.shards)
}

func (pr *peerReplica) doApplyCompactRaftLog(result *raftGCResult) {
	total := pr.lastReadyIndex - result.firstIndex
	remain := pr.lastReadyIndex - result.state.Index - 1
	pr.raftLogSizeHint = pr.raftLogSizeHint * remain / total

	startIndex := uint64(0)
	endIndex := result.state.Index + 1

	logger.Debugf("shard %d start to compact raft log, start=<%d> end=<%d>",
		pr.shardID,
		startIndex,
		endIndex)
	err := pr.startCompactRaftLogJob(pr.shardID, startIndex, endIndex)
	if err != nil {
		logger.Errorf("shard %s add raft gc job failed with %+v",
			pr.shardID,
			err)
	}
}
