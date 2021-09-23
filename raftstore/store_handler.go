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
	"bytes"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixcube/components/keys"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func (s *store) handleCompactRaftLog() {
	s.foreachPR(func(pr *replica) bool {
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

	s.foreachPR(func(pr *replica) bool {
		if pr.supportSplit() &&
			pr.isLeader() &&
			(s.handledCustomSplitCheck(pr.getShard().Group) ||
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
	bm := roaring64.NewBitmap()
	s.foreachPR(func(pr *replica) bool {
		bm.Add(pr.shardID)
		return true
	})

	if bm.GetCardinality() > 0 {
		rsp, err := s.pd.GetClient().CheckResourceState(bm)
		if err != nil {
			logger.Errorf("check shards state failed with %+v", err)
			return
		}

		for _, id := range rsp.Removed {
			s.destoryPR(id, true, "shard state check")
		}
	}
}

// all raft message entrypoint
func (s *store) handle(value interface{}) {
	if msg, ok := value.(*meta.RaftMessage); ok {
		s.onRaftMessage(msg)
		pb.ReleaseRaftMessage(msg)
	} else if msg, ok := value.(*meta.SnapshotMessage); ok {
		s.onSnapshotMessage(msg)
	}
}

func (s *store) onSnapshotMessage(msg *meta.SnapshotMessage) {
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

func (s *store) onRaftMessage(msg *meta.RaftMessage) {
	if !s.isRaftMsgValid(msg) {
		return
	}

	if msg.IsTombstone {
		// we receive a message tells us to remove ourself.
		s.handleGCPeerMsg(msg)
		return
	}

	if !s.tryToCreatePeerReplicate(msg) {
		return
	}

	s.peers.Store(msg.From.ID, msg.From)
	pr := s.getPR(msg.ShardID, false)
	pr.step(msg.Message)
}

func (s *store) isRaftMsgValid(msg *meta.RaftMessage) bool {
	if msg.To.ContainerID != s.meta.meta.ID {
		logger.Warningf("store not match, toPeerStoreID=<%d> mineStoreID=<%d>",
			msg.To.ContainerID,
			s.meta.meta.ID)
		return false
	}

	return true
}

func (s *store) handleGCPeerMsg(msg *meta.RaftMessage) {
	shardID := msg.ShardID
	pr := s.getPR(shardID, false)
	if pr != nil {
		fromEpoch := msg.ShardEpoch
		shard := pr.getShard()
		if isEpochStale(shard.Epoch, fromEpoch) {
			logger.Infof("shard %d receives gc message, remove. msg=<%+v>",
				shardID,
				msg)
			s.destoryPR(shardID, false, "gc")
		}
	}
}

func (s *store) tryToCreatePeerReplicate(msg *meta.RaftMessage) bool {
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
			// TODO: check this.
			// cancel snapshotting op

			//if p.ps.isApplyingSnapshot() && !p.ps.cancelApplyingSnapJob() {
			//	logger.Infof("shard %d stale peer is applying snapshot, will destroy next time, peer=<%d>",
			//		msg.ShardID,
			//		p.peer.ID)

			//	return false
			//}

			stalePeer = p.peer
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
		s.destoryPR(msg.ShardID, false, "found stale peer")
		return false
	}

	if hasPeer {
		return true
	}

	// arrive here means target peer not found, we will try to create it
	if msg.Message.Type != raftpb.MsgVote &&
		msg.Message.Type != raftpb.MsgPreVote &&
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
		if bytes.Compare(keys.EncStartKey(&item), keys.GetDataEndKey(msg.Group, msg.End)) < 0 {
			var state string
			if p := s.getPR(item.ID, false); p != nil {
				state = fmt.Sprintf("overlappedShard=<%d> apply index %d",
					p.shardID,
					p.appliedIndex)

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
	pr, err := createPeerReplicaWithRaftMessage(s, msg, target, "receive raft message")
	if err != nil {
		logger.Errorf("shard %d peer replica failed with %+v",
			msg.ShardID,
			err)
		return false
	}

	// Following snapshot may overlap, should insert into keyRanges after snapshot is applied.
	if s.addPR(pr) {
		pr.start()

		// FIXME: this seems to be wrong
		// pr.shard.Peers = append(pr.shard.Peers, msg.To)
		// pr.shard.Peers = append(pr.shard.Peers, msg.From)
		s.updateShardKeyRange(pr.getShard())

		s.peers.Store(msg.From.ID, msg.From)
		s.peers.Store(msg.To.ID, msg.To)
	}

	return true
}
