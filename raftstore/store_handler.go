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

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/keys"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

func (s *store) handleSplitCheck() {
	// FIXME: do we need to do such busy check?
	/*if s.runner.IsNamedWorkerBusy(splitCheckWorkerName) {
		return
	}*/

	s.forEachReplica(func(pr *replica) bool {
		if pr.supportSplit() &&
			pr.isLeader() {
			pr.addAction(action{actionType: checkSplitAction, actionCallback: func(arg interface{}) {
				s.splitChecker.add(arg.(Shard))
			}})
		}

		return true
	})
}

func (s *store) handleShardStateCheck() {
	bm := roaring64.NewBitmap()
	s.forEachReplica(func(pr *replica) bool {
		bm.Add(pr.shardID)
		return true
	})

	if bm.GetCardinality() > 0 {
		rsp, err := s.pd.GetClient().CheckResourceState(bm)
		if err != nil {
			s.logger.Error("fail to check shards state, retry later",
				s.storeField(),
				zap.Error(err))
			return
		}

		for _, id := range rsp.Removed {
			s.destroyReplica(id, true, "shard state check")
		}
	}
}

// all raft message entrypoint
func (s *store) handle(value interface{}) {
	if msg, ok := value.(meta.RaftMessage); ok {
		s.onRaftMessage(msg)
	} else if msg, ok := value.(meta.SnapshotMessage); ok {
		s.onSnapshotMessage(msg)
	}
}

func (s *store) onSnapshotMessage(msg meta.SnapshotMessage) {
	panic("snapshot not implemented")
}

func (s *store) onRaftMessage(msg meta.RaftMessage) {
	if !s.isRaftMsgValid(msg) {
		return
	}

	if msg.IsTombstone {
		// we receive a message tells us to remove ourself.
		s.handleDestroyReplicaMessage(msg)
		return
	}

	if !s.tryToCreateReplicate(msg) {
		return
	}

	s.replicaRecords.Store(msg.From.ID, msg.From)
	pr := s.getReplica(msg.ShardID, false)
	pr.addMessage(msg.Message)
}

func (s *store) isRaftMsgValid(msg meta.RaftMessage) bool {
	if msg.To.ContainerID != s.meta.meta.ID {
		s.logger.Warn("raft msg store not match",
			s.storeField(),
			zap.Uint64("actual", msg.To.ContainerID))
		return false
	}

	return true
}

func (s *store) handleDestroyReplicaMessage(msg meta.RaftMessage) {
	shardID := msg.ShardID
	if pr := s.getReplica(shardID, false); pr != nil {
		fromEpoch := msg.ShardEpoch
		shard := pr.getShard()
		if isEpochStale(shard.Epoch, fromEpoch) {
			s.logger.Info("received destroy message, remove self",
				s.storeField(),
				log.ShardIDField(shardID),
				log.EpochField("self-epoch", shard.Epoch),
				log.EpochField("msg-epoch", fromEpoch))
			s.destroyReplica(shardID, false, "gc")
		}
	}
}

func (s *store) tryToCreateReplicate(msg meta.RaftMessage) bool {
	// If target peer doesn't exist, create it.
	//
	// return false to indicate that target peer is in invalid state or
	// doesn't exist and can't be created.

	var (
		hasPeer   = false
		stalePeer Replica
	)

	target := msg.To

	if p := s.getReplica(msg.ShardID, false); p != nil {
		hasPeer = true

		// we may encounter a message with larger peer id, which means
		// current peer is stale, then we should remove current peer
		if p.replica.ID < target.ID {
			// TODO: check this.
			// cancel snapshotting op

			//if p.ps.isApplyingSnapshot() && !p.ps.cancelApplyingSnapJob() {
			//	logger.Infof("shard %d stale peer is applying snapshot, will destroy next time, peer=<%d>",
			//		msg.ShardID,
			//		p.peer.ID)

			//	return false
			//}

			stalePeer = p.replica
		} else if p.replica.ID > target.ID {
			s.logger.Info("from replica is stale",
				s.storeField(),
				log.ShardIDField(msg.ShardID),
				log.ReplicaField("self-replica", p.replica),
				log.ReplicaField("msg-replica", target))
			return false
		}
	}

	// If we found stale peer, we will destory it
	if stalePeer.ID > 0 {
		s.destroyReplica(msg.ShardID, false, "found stale peer")
		return false
	}

	if hasPeer {
		return true
	}

	// arrive here means target peer not found, we will try to create it
	if msg.Message.Type != raftpb.MsgVote &&
		msg.Message.Type != raftpb.MsgPreVote &&
		(msg.Message.Type != raftpb.MsgHeartbeat || msg.Message.Commit != invalidIndex) {
		s.logger.Info("replica doesn't exist",
			s.storeField(),
			log.ShardIDField(msg.ShardID),
			log.ReplicaField("replica", target))
		return false
	}

	// check range overlapped
	item := s.searchShard(msg.Group, msg.Start)
	if item.ID > 0 {
		if bytes.Compare(keys.EncodeStartKey(item, nil),
			keys.GetDataEndKey(msg.Group, msg.End, nil)) < 0 {
			if p := s.getReplica(item.ID, false); p != nil {
				// Maybe split, but not registered yet.
				s.cacheDroppedVoteMsg(msg.ShardID, msg.Message)

				s.logger.Info("replica has overlapped range",
					s.storeField(),
					log.ShardField("overlapped-replica", item),
					log.ShardField("local-replica", p.getShard()))
			}

			return false
		}
	}

	// now we can create a replicate
	pr, err := createReplicaWithRaftMessage(s, msg, target, "receive raft message")
	if err != nil {
		s.logger.Error("fail to create replica",
			s.storeField(),
			log.ShardIDField(msg.ShardID),
			zap.Error(err))
		return false
	}

	// Following snapshot may overlap, should insert into keyRanges after snapshot is applied.
	if s.addReplica(pr) {
		pr.start()

		// FIXME: this seems to be wrong
		// pr.shard.Peers = append(pr.shard.Peers, msg.To)
		// pr.shard.Peers = append(pr.shard.Peers, msg.From)
		s.updateShardKeyRange(pr.getShard())

		s.replicaRecords.Store(msg.From.ID, msg.From)
		s.replicaRecords.Store(msg.To.ID, msg.To)
	}

	return true
}
