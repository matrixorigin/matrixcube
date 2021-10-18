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
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/keys"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/meta"
)

var (
	ErrRemoveShardKeyRange = errors.New("failed to delete shard key range")
)

// destroyReplica destroys the replica by closing it, removing it from the
// store and finally deleting all its associated data.
func (s *store) destroyReplica(shardID uint64,
	placeTombstone bool, reason string) {
	replica := s.getReplica(shardID, false)
	if replica == nil {
		s.logger.Warn("replica not found",
			log.ShardIDField(shardID))
		return
	}

	s.vacuumCleaner.addTask(vacuumTask{
		shard:          replica.getShard(),
		replica:        replica,
		placeTombstone: placeTombstone,
		reason:         reason,
	})
}

// cleanupTombstones is invoked during restart to cleanup data belongs to those
// shards that have been tombstoned.
func (s *store) cleanupTombstones(shards []Shard) {
	for _, shard := range shards {
		s.vacuumCleaner.addTask(vacuumTask{
			shard: shard,
		})
	}
}

// vacuum is the actual method for handling a vacuum task.
func (s *store) vacuum(t vacuumTask) error {
	if t.replica != nil {
		if err := t.replica.destroy(t.placeTombstone, t.reason); err != nil {
			return err
		}
		t.replica.close()
		// wait for the replica to be fully unloaded before removing it from the
		// store. otherwise the raft worker might not be able to get the replica
		// from the store and mark it as unloaded.
		s.logger.Info("waiting for the replica to be unloaded",
			log.ReplicaIDField(t.shard.ID))
		t.replica.waitUnloaded()
		s.removeReplica(t.replica)
		s.logger.Info("replica unloaded",
			log.ReplicaIDField(t.shard.ID))
	}
	s.removeDroppedVoteMsg(t.shard.ID)
	if len(t.shard.Replicas) > 0 && !s.removeShardKeyRange(t.shard) {
		// TODO: is it possible to not have shard related key range info in store?
		// should this be an error?
		// return ErrRemoveShardKeyRange
		s.logger.Warn("failed to delete shard key range")
	}

	s.logger.Info("deleting shard data",
		s.storeField(),
		log.ShardIDField(t.shard.ID))
	start := keys.EncodeStartKey(t.shard, nil)
	end := keys.EncodeEndKey(t.shard, nil)
	err := s.DataStorageByGroup(t.shard.Group).RemoveShardData(t.shard, start, end)
	s.logger.Info("delete shard data returned",
		s.storeField(),
		log.ShardIDField(t.shard.ID),
		zap.Error(err))
	if t.replica != nil && err == nil {
		t.replica.confirmDestroyed()
	}

	return err
}

func (pr *replica) destroy(placeTombstone bool, reason string) error {
	pr.logger.Info("begin to destory",
		zap.Bool("place-tombstone", placeTombstone),
		log.ReasonField(reason))

	if placeTombstone {
		pr.sm.setShardState(metapb.ResourceState_Removed)
	}
	shard := pr.getShard()
	// FIXME: updating the state of replicated state machine outside of the
	// protocol. should we just use math.MaxUint64 as the index below?
	index, _ := pr.sm.getAppliedIndexTerm()
	return pr.sm.saveShardMetedata(index, shard, meta.ReplicaState_Tombstone)
}

func (pr *replica) confirmDestroyed() {
	pr.logger.Info("going to mark replica as destroyed")
	close(pr.destroyedC)
}

// waitDestroyed is suppose to be only used in tests
func (pr *replica) waitDestroyed() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			pr.logger.Info("slow to be destroyed")
		case <-pr.destroyedC:
			return
		}
	}
}
