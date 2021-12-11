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

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

// all raft message entrypoint
func (s *store) handle(batch meta.RaftMessageBatch) {
	for _, msg := range batch.Messages {
		s.onRaftMessage(msg)
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

	pr := s.getReplica(msg.ShardID, false)
	if pr != nil {
		s.replicaRecords.Store(msg.From.ID, msg.From)
		pr.addMessage(msg)
	}
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
			s.destroyReplica(shardID, false, true, "gc")
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
		if p.replicaID < target.ID {
			// TODO: check this.
			// cancel snapshotting op

			//if p.ps.isApplyingSnapshot() && !p.ps.cancelApplyingSnapJob() {
			//	logger.Infof("shard %d stale peer is applying snapshot, will destroy next time, peer=<%d>",
			//		msg.ShardID,
			//		p.peer.ID)

			//	return false
			//}

			stalePeer = p.replica
		} else if p.replicaID > target.ID {
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
		s.logger.Info("found stale peer, need to remove self replica",
			s.storeField(),
			log.ShardIDField(msg.ShardID),
			log.ReplicaField("msg-to", msg.To),
			log.ReplicaField("current-stale-replica", stalePeer))
		s.destroyReplica(msg.ShardID, false, true, "found stale peer")
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

	if msg.From.Role == metapb.ReplicaRole_Learner {
		s.logger.Fatal("received a learner vote/pre-vote message",
			s.storeField(),
			log.ShardIDField(msg.ShardID),
			log.ReplicaField("to", target),
			log.ReplicaField("from", msg.From))
	}

	// check range overlapped
	if item := s.searchShard(msg.Group, msg.Start); item.ID > 0 {
		if bytes.Compare(item.Start, msg.End) < 0 {
			if p := s.getReplica(item.ID, false); p != nil {
				// Maybe split, but not registered yet.
				s.cacheDroppedVoteMsg(msg.ShardID, msg)

				s.logger.Info("replica has overlapped range",
					s.storeField(),
					log.ShardField("overlapped-replica", item),
					log.ShardField("local-replica", p.getShard()))
			}

			return false
		}
	}

	if s.createShardsProtector.inDestoryState(msg.ShardID) {
		s.logger.Debug("skip create replica",
			s.storeField(),
			log.ReasonField("shard in destroy state"),
			log.ShardIDField(msg.ShardID))
		return false
	}

	newReplicaCreator(s).
		withReason(fmt.Sprintf("raft %s message from %d/%d/%s",
			msg.Message.Type.String(),
			msg.From.ID,
			msg.From.ContainerID,
			msg.From.Role.String())).
		withStartReplica(nil, nil).
		withReplicaRecordGetter(func(s Shard) Replica { return target }).
		create([]Shard{
			{
				ID:           msg.ShardID,
				Epoch:        msg.ShardEpoch,
				Start:        msg.Start,
				End:          msg.End,
				Group:        msg.Group,
				DisableSplit: msg.DisableSplit,
				Unique:       msg.Unique,
				Replicas:     []Replica{},
			},
		})
	return true
}
