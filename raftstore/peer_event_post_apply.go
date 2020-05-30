package raftstore

import (
	"time"

	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/beehive/metric"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
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
	if pr.ps.isApplyingSnapshot() {
		logger.Fatalf("shard %d should not applying snapshot, when do post apply.",
			pr.shardID)
	}

	pr.ps.applyState = result.applyState
	pr.ps.appliedIndexTerm = result.appliedIndexTerm
	pr.rn.AdvanceApply(result.applyState.AppliedIndex)

	logger.Debugf("shard %d async apply committied entries finished at %d",
		pr.shardID,
		result.applyState.AppliedIndex)

	pr.metrics.admin.incBy(result.metrics.admin)

	pr.writtenBytes += uint64(result.metrics.writtenBytes)
	pr.writtenKeys += result.metrics.writtenKeys

	if result.hasSplitExecResult() {
		pr.deleteKeysHint = result.metrics.deleteKeysHint
		pr.sizeDiffHint = result.metrics.sizeDiffHint
	} else {
		pr.deleteKeysHint += result.metrics.deleteKeysHint
		pr.sizeDiffHint += result.metrics.sizeDiffHint
	}

	readyCnt := int(pr.pendingReads.getReadyCnt())
	if readyCnt > 0 && pr.readyToHandleRead() {
		for index := 0; index < readyCnt; index++ {
			if c, ok := pr.pendingReads.pop(); ok {
				pr.doExecReadCmd(c)
			}
		}

		pr.pendingReads.resetReadyCnt()
	}
}

func (pr *peerReplica) doPostApplyResult(result asyncApplyResult) {
	switch result.result.adminType {
	case raftcmdpb.ChangePeer:
		pr.doApplyConfChange(result.result.changePeer)
	case raftcmdpb.Split:
		pr.doApplySplit(result.result.splitResult)
	case raftcmdpb.CompactRaftLog:
		pr.doApplyCompactRaftLog(result.result.raftGCResult)
	}
}

func (pr *peerReplica) doApplyConfChange(cp *changePeer) {
	pr.rn.ApplyConfChange(cp.confChange)
	if cp.confChange.NodeID == 0 {
		// Apply failed, skip.
		return
	}

	pr.ps.shard = cp.shard
	if pr.isLeader() {
		// Notify pd immediately.
		logger.Infof("shard %d notify pd with %s with peer %+v at epoch %+v",
			pr.shardID,
			cp.confChange.Type.String(),
			cp.peer,
			pr.ps.shard.Epoch)
		pr.store.pd.GetRPC().TiggerResourceHeartbeat(pr.shardID)
	}

	switch cp.confChange.Type {
	case etcdraftpb.ConfChangeAddNode:
		// Add this peer to cache.
		pr.peerHeartbeatsMap.Store(cp.peer.ID, time.Now())
		pr.store.peers.Store(cp.peer.ID, cp.peer)
	case etcdraftpb.ConfChangeRemoveNode:
		// Remove this peer from cache.
		pr.peerHeartbeatsMap.Delete(cp.peer.ID)
		pr.store.peers.Delete(cp.peer.ID)

		// We only care remove itself now.
		if cp.peer.StoreID == pr.store.meta.meta.ID {
			if cp.peer.ID == pr.peer.ID {
				pr.mustDestroy()
			} else {
				logger.Fatalf("shard %d trying to remove unknown peer, peer=<%+v>",
					pr.shardID,
					cp.peer)
			}
		}
	}

	logger.Infof("shard %d applied %s with peer %+v at epoch %+v, new peers %+v",
		pr.shardID,
		cp.confChange.Type.String(),
		cp.peer,
		pr.ps.shard.Epoch,
		pr.ps.shard.Peers)
}

func (pr *peerReplica) doApplySplit(result *splitResult) {
	pr.ps.shard = result.left

	// add new shard peers to cache
	for _, p := range result.right.Peers {
		pr.store.peers.Store(p.ID, p)
	}

	newShardID := result.right.ID
	newPR := pr.store.getPR(newShardID, false)
	if newPR != nil {
		for _, p := range result.right.Peers {
			pr.store.peers.Store(p.ID, p)
		}

		// If the store received a raft msg with the new shard raft group
		// before splitting, it will creates a uninitialized peer.
		// We can remove this uninitialized peer directly.
		if newPR.ps.isInitialized() {
			logger.Fatalf("shard %d duplicated shard split to new shard %d",
				pr.shardID,
				newShardID)
		}
	}

	newPR, err := createPeerReplica(pr.store, &result.right)
	if err != nil {
		// peer information is already written into db, can't recover.
		// there is probably a bug.
		logger.Fatalf("shard %d create new split shard failed, new shard=<%+v> errors:\n %+v",
			pr.shardID,
			result.right,
			err)
	}

	pr.store.updateShardKeyRange(result.left)
	pr.store.updateShardKeyRange(result.right)

	newPR.sizeDiffHint = newPR.store.opts.shardSplitCheckBytes
	newPR.startRegistrationJob()
	pr.store.addPR(newPR)

	// If this peer is the leader of the shard before split, it's intuitional for
	// it to become the leader of new split shard.
	// The ticks are accelerated here, so that the peer for the new split shard
	// comes to campaign earlier than the other follower peers. And then it's more
	// likely for this peer to become the leader of the new split shard.
	// If the other follower peers applies logs too slowly, they may fail to vote the
	// `MsgRequestVote` from this peer on its campaign.
	// In this worst case scenario, the new split raft group will not be available
	// since there is no leader established during one election timeout after the split.
	if pr.isLeader() && len(result.right.Peers) > 1 {
		newPR.addAction(doCampaignAction)
	}

	if pr.isLeader() {
		logger.Infof("shard %d notify pd with split, left=<%+v> right=<%+v>, state=<%s>, apply=<%s>",
			pr.shardID,
			result.left,
			result.right,
			pr.ps.raftState.String(),
			pr.ps.applyState.String())

		pr.store.pd.GetRPC().TiggerResourceHeartbeat(pr.shardID)
	} else {
		if vote, ok := pr.store.removeDroppedVoteMsg(newPR.shardID); ok {
			newPR.step(vote)
		}
	}

	pr.store.opts.shardStateAware.Splited(pr.ps.shard)
	logger.Infof("shard %d new shard added, left=<%+v> right=<%+v>",
		pr.shardID,
		result.left,
		result.right)
}

func (pr *peerReplica) doApplyCompactRaftLog(result *raftGCResult) {
	total := pr.ps.lastReadyIndex - result.firstIndex
	remain := pr.ps.lastReadyIndex - result.state.Index - 1
	pr.raftLogSizeHint = pr.raftLogSizeHint * remain / total

	startIndex := pr.ps.lastCompactIndex
	endIndex := result.state.Index + 1
	pr.ps.lastCompactIndex = endIndex

	logger.Debugf("shard %d start to compact raft log, start=<%d> end=<%d>",
		pr.shardID,
		startIndex,
		endIndex)
	err := pr.startCompactRaftLogJob(pr.shardID, startIndex, endIndex)
	if err != nil {
		logger.Errorf("shard %s add raft gc job failed, errors:\n %+v",
			pr.shardID,
			err)
	}
}
