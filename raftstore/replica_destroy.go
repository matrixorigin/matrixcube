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
	"math"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/storage"
)

var (
	ErrRemoveShardKeyRange = errors.New("failed to delete shard key range")
)

// destroyShards used to protect shards that need to be deleted from being recreated.
// Shard A has 3 replicas: A1, A2, A3, and destroy shard A.
// 1. A1 removed from node1
// 2. A2 send vote message to A1
// 3. A1 recreate by vote message
// So we need createShardsProtector to avoid A1 recreate. Once Shard A is in the destroying state,
// the nodes where all replicas of Shard A are located cannot be changed.
// Shard is added to createShardsProtector before it is destroyed.
type createShardsProtector struct {
	sync.RWMutex

	destroyedShards *roaring64.Bitmap // all the shards which state in destroying or destroyed in current node.
}

func newCreateShardsProtector() *createShardsProtector {
	return &createShardsProtector{
		destroyedShards: roaring64.New(),
	}
}

func (csp *createShardsProtector) addDestroyed(id uint64) {
	csp.Lock()
	defer csp.Unlock()

	csp.destroyedShards.Add(id)
}

func (csp *createShardsProtector) inDestroyState(id uint64) bool {
	csp.RLock()
	defer csp.RUnlock()

	return csp.destroyedShards.Contains(id)
}

// destroyReplica destroys the replica by closing it, removing it from the
// store and finally deleting all its associated data.
func (s *store) destroyReplica(shardID uint64,
	shardRemoved, removeData bool, reason string) {
	replica := s.getReplica(shardID, false)
	if replica == nil {
		s.logger.Warn("replica not found",
			log.ShardIDField(shardID))
		return
	}

	if shardRemoved {
		s.createShardsProtector.addDestroyed(shardID)
	}
	s.vacuumCleaner.addTask(vacuumTask{
		shard:        replica.getShard(),
		replica:      replica,
		shardRemoved: shardRemoved,
		removeData:   removeData,
		reason:       reason,
	})
}

// cleanupTombstones is invoked during restart to cleanup data belongs to those
// shards that have been tombstoned.
func (s *store) cleanupTombstones(shards []meta.ShardLocalState) {
	for _, sls := range shards {
		s.vacuumCleaner.addTask(vacuumTask{
			shard:      sls.Shard,
			removeData: sls.RemoveData,
			reason:     "restart-clean-tombstone",
		})
	}
}

// vacuum is the actual method for handling a vacuum task.
func (s *store) vacuum(t vacuumTask) error {
	s.logger.Info("begin to destroy replica",
		s.storeField(),
		log.ReasonField(t.reason),
		zap.Bool("remove-data", t.removeData),
		log.ShardIDField(t.shard.ID))

	if t.replica != nil {
		if t.replica.closed() {
			// this can happen when PD request to remove a splitting shard. two vaccum
			// tasks will be created, one for splitting and one for removal.
			t.replica.logger.Info("skip vacuuming already closed replica")
			return nil
		}
		if err := t.replica.destroy(t.shardRemoved, t.reason); err != nil {
			// storage.ErrShardNotFound is returned by the AOE when the shard has
			// already been removed as a result of split. we just ignore such
			// error here.
			if err != storage.ErrShardNotFound {
				return err
			}
		}
		t.replica.close()
		// wait for the replica to be fully unloaded before removing it from the
		// store. otherwise the raft worker might not be able to get the replica
		// from the store and mark it as unloaded.
		t.replica.logger.Info("waiting for the replica to be unloaded",
			log.ReplicaIDField(t.shard.ID))
		t.replica.waitUnloaded()

		t.replica.logger.Info("replica unloaded",
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
	if err := s.logdb.RemoveReplicaData(t.shard.ID); err != nil {
		return err
	}
	err := s.DataStorageByGroup(t.shard.Group).RemoveShard(t.shard, t.removeData)
	s.logger.Info("delete shard data returned",
		s.storeField(),
		log.ShardIDField(t.shard.ID),
		zap.Error(err))
	if err == nil {
		s.removeReplica(t.shard)
		if t.replica != nil {
			t.replica.confirmDestroyed()
		}
	}
	return err
}

func (pr *replica) destroy(shardRemoved bool, reason string) error {
	pr.logger.Info("begin to destroy",
		zap.Bool("shard-removed", shardRemoved),
		log.ShardField("metadata", pr.getShard()),
		log.ReasonField(reason))

	if shardRemoved {
		pr.sm.setShardState(metapb.ResourceState_Destroyed)
	}
	shard := pr.getShard()
	// FIXME: updating the state of replicated state machine outside of the
	// protocol. is it okay to just use math.MaxUint64 as the index below?
	// or maybe we should just propose a new command to move such procedure into
	// the wal/sm?
	//index, _ := pr.sm.getAppliedIndexTerm()
	return pr.sm.saveShardMetedata(math.MaxUint64,
		math.MaxUint64, shard, meta.ReplicaState_Tombstone)
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
