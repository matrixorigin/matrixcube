package raftstore

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
	"github.com/pilosa/pilosa/roaring"
	"go.etcd.io/etcd/raft/raftpb"
)

func (s *store) handleCompactRaftLog() {
	s.foreachPR(func(pr *peerReplica) bool {
		if pr.isLeader() {
			pr.addAction(action{actionType: checkCompactAction})
		}
		return true
	})
}

func (s *store) handleSplitCheck() {
	if s.runner.IsNamedWorkerBusy(splitCheckWorkerName) {
		return
	}

	s.foreachPR(func(pr *peerReplica) bool {
		if pr.supportSplit() &&
			pr.isLeader() &&
			(s.handledCustomSplitCheck(pr.ps.shard.Group) ||
				pr.sizeDiffHint >= uint64(s.cfg.Replication.ShardSplitCheckBytes)) {
			pr.addAction(action{actionType: checkSplitAction})
		}

		return true
	})
}

func (s *store) handledCustomSplitCheck(group uint64) bool {
	return s.cfg.Customize.CustomSplitCheckFuncFactory != nil && s.cfg.Customize.CustomSplitCheckFuncFactory(group) != nil
}

func (s *store) handleShardStateCheck() {
	bm := roaring.NewBitmap()
	s.foreachPR(func(pr *peerReplica) bool {
		bm.Add(pr.shardID)
		return true
	})

	if bm.Count() > 0 {
		rsp, err := s.pd.GetClient().CheckResourceState(bm)
		if err != nil {
			logger.Errorf("check shards state failed with %+v", err)
		}

		for _, id := range rsp.Removed {
			s.doDestroy(id)
		}
	}
}

// all raft message entrypoint
func (s *store) handle(value interface{}) {
	if msg, ok := value.(*bhraftpb.RaftMessage); ok {
		s.onRaftMessage(msg)
		pb.ReleaseRaftMessage(msg)
	} else if msg, ok := value.(*bhraftpb.SnapshotMessage); ok {
		s.onSnapshotMessage(msg)
	}
}

func (s *store) onSnapshotMessage(msg *bhraftpb.SnapshotMessage) {
	pr := s.getPR(msg.Header.Shard.ID, false)
	if pr != nil {
		s.addApplyJob(pr.applyWorker, "onSnapshotData", func() error {
			err := s.snapshotManager.ReceiveSnapData(msg)
			if err != nil {
				logger.Fatalf("shard %s received snap data failed, errors:\n%+v",
					msg.Header.Shard.ID,
					err)
			}

			return err
		}, nil)
	}

}

func (s *store) onRaftMessage(msg *bhraftpb.RaftMessage) {
	if !s.isRaftMsgValid(msg) {
		return
	}

	if msg.IsTombstone {
		// we receive a message tells us to remove ourself.
		s.handleGCPeerMsg(msg)
		return
	}

	yes, err := s.isMsgStale(msg)
	if err != nil || yes {
		return
	}

	if !s.tryToCreatePeerReplicate(msg) {
		return
	}

	s.peers.Store(msg.From.ID, msg.From)
	pr := s.getPR(msg.ShardID, false)
	pr.step(msg.Message)
}

func (s *store) isRaftMsgValid(msg *bhraftpb.RaftMessage) bool {
	if msg.To.ContainerID != s.meta.meta.ID {
		logger.Warningf("store not match, toPeerStoreID=<%d> mineStoreID=<%d>",
			msg.To.ContainerID,
			s.meta.meta.ID)
		return false
	}

	return true
}

func (s *store) handleGCPeerMsg(msg *bhraftpb.RaftMessage) {
	shardID := msg.ShardID
	needRemove := false

	if value, ok := s.replicas.Load(shardID); ok {
		pr := value.(*peerReplica)
		fromEpoch := msg.ShardEpoch

		if isEpochStale(pr.ps.shard.Epoch, fromEpoch) {
			logger.Infof("shard %d receives gc message, remove. msg=<%+v>",
				shardID,
				msg)
			needRemove = true

			if !pr.ps.isInitialized() {
				needRemove = false
				pr.mustDestroy()
			}
		}
	}

	if needRemove {
		s.startDestroyJob(shardID, msg.To)
	}
}

func (s *store) isMsgStale(msg *bhraftpb.RaftMessage) (bool, error) {
	shardID := msg.ShardID
	fromEpoch := msg.ShardEpoch
	isVoteMsg := msg.Message.Type == raftpb.MsgVote
	fromStoreID := msg.From.ContainerID

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with pd and pd will
	// tell 2 is stale, so 2 can remove itself.
	pr := s.getPR(shardID, false)
	if nil != pr {
		current := pr.ps.shard
		epoch := current.Epoch
		if isEpochStale(fromEpoch, epoch) &&
			findPeer(&current, fromStoreID) == nil {
			s.handleStaleMsg(msg, epoch, true)
			return true, nil
		}

		return false, nil
	}

	// no exist, check with tombstone key.
	localState, err := loadLocalState(shardID, s.MetadataStorage(), true)
	if err != nil {
		return false, err
	}

	if localState != nil {
		if localState.State != bhraftpb.PeerState_Tombstone {
			// Maybe split, but not registered yet.
			s.cacheDroppedVoteMsg(shardID, msg.Message)
			return false, fmt.Errorf("shard<%d> not exist but not tombstone, local state: %s",
				shardID,
				localState.String())
		}

		shardEpoch := localState.Shard.Epoch
		// The shard in this peer is already destroyed
		if isEpochStale(fromEpoch, shardEpoch) {
			logger.Infof("tombstone peer receive a a stale message, epoch=<%s> shard=<%d> msg=<%s>",
				shardEpoch.String(),
				shardID,
				msg.String())
			notExist := findPeer(&localState.Shard, fromStoreID) == nil
			s.handleStaleMsg(msg, shardEpoch, isVoteMsg && notExist)

			return true, nil
		}

		if fromEpoch.ConfVer == shardEpoch.ConfVer {
			return false, fmt.Errorf("tombstone peer receive an invalid message, epoch=<%s> msg=<%s>",
				shardEpoch.String(),
				msg.String())

		}
	}

	return false, nil
}

func (s *store) handleStaleMsg(msg *bhraftpb.RaftMessage, currEpoch metapb.ResourceEpoch, needGC bool) {
	shardID := msg.ShardID
	fromPeer := msg.From
	toPeer := msg.To

	if !needGC {
		logger.Infof("shard %d raft msg is stale, ignore it, msg=<%+v> current=<%+v>",
			shardID,
			msg,
			currEpoch)
		return
	}

	logger.Infof("shard %d raft msg is stale, tell to gc, msg=<%+v> current=<%+v>",
		shardID,
		msg,
		currEpoch)

	gc := new(bhraftpb.RaftMessage)
	gc.ShardID = shardID
	gc.To = fromPeer
	gc.From = toPeer
	gc.ShardEpoch = currEpoch
	gc.IsTombstone = true

	s.trans.Send(gc)
}

func (s *store) tryToCreatePeerReplicate(msg *bhraftpb.RaftMessage) bool {
	// If target peer doesn't exist, create it.
	//
	// return false to indicate that target peer is in invalid state or
	// doesn't exist and can't be created.

	var (
		hasPeer   = false
		stalePeer metapb.Peer
	)

	target := msg.To

	if p := s.getPR(msg.ShardID, false); p != nil {
		hasPeer = true

		// we may encounter a message with larger peer id, which means
		// current peer is stale, then we should remove current peer
		if p.peer.ID < target.ID {
			// cancel snapshotting op
			if p.ps.isApplyingSnapshot() && !p.ps.cancelApplyingSnapJob() {
				logger.Infof("shard %d stale peer is applying snapshot, will destroy next time, peer=<%d>",
					msg.ShardID,
					p.peer.ID)

				return false
			}

			stalePeer = p.peer
			if !p.ps.isInitialized() {
				p.mustDestroy()
				return false
			}
		} else if p.peer.ID > target.ID {
			logger.Infof("shard %d may be from peer is stale, targetID=<%d> currentID=<%d>",
				msg.ShardID,
				target.ID,
				p.peer.ID)
			return false
		}
	}

	// If we found stale peer, we will destory it
	if stalePeer.ID > 0 {
		s.startDestroyJob(msg.ShardID, stalePeer)
		hasPeer = false
	}

	if hasPeer {
		return true
	}

	// arrive here means target peer not found, we will try to create it
	if msg.Message.Type != raftpb.MsgVote &&
		(msg.Message.Type != raftpb.MsgHeartbeat || msg.Message.Commit != invalidIndex) {
		logger.Infof("shard %d target peer doesn't exist, peer=<%+v> message=<%s>",
			msg.ShardID,
			target,
			msg.Message.Type)
		return false
	}

	// check range overlapped
	item := s.searchShard(msg.Group, msg.Start)
	if item.ID > 0 {
		if bytes.Compare(encStartKey(&item), getDataEndKey(msg.Group, msg.End)) < 0 {
			var state string
			if p := s.getPR(item.ID, false); p != nil {
				state = fmt.Sprintf("overlappedShard=<%d> local=<%s> apply=<%s>",
					p.shardID,
					p.ps.raftState.String(),
					p.ps.applyState.String())

				// Maybe split, but not registered yet.
				s.cacheDroppedVoteMsg(msg.ShardID, msg.Message)
			}

			if logger.DebugEnabled() {
				logger.Debugf("shard %d msg is overlapped with shard, shard=<%s> msg=<%s> state=<%s>",
					msg.ShardID,
					item.String(),
					msg.String(),
					state)
			}

			return false
		}
	}

	// now we can create a replicate
	pr, err := createPeerReplicaWithRaftMessage(s, msg, target)
	if err != nil {
		logger.Errorf("shard %d peer replica failure, errors:\n %+v",
			msg.ShardID,
			err)
		return false
	}

	pr.ps.shard.Peers = append(pr.ps.shard.Peers, msg.To)
	pr.ps.shard.Peers = append(pr.ps.shard.Peers, msg.From)
	s.updateShardKeyRange(pr.ps.shard)

	// following snapshot may overlap, should insert into keyRanges after
	// snapshot is applied.
	s.addPR(pr)
	s.peers.Store(msg.From.ID, msg.From)
	s.peers.Store(msg.To.ID, msg.To)
	return true
}
