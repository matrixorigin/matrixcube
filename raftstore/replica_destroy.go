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
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"go.uber.org/zap"
)

// TODO: probably no longer need replica_job.go, maybe just add replica_split.go
// and replica_destroy.go

// remove replica from current node.
// 1. In raft rpc thread:        after receiving messages from other nodes, it is found that the current replica is stale.
// 2. In raft event loop thread: after conf change, it is found that the current replica is removed.
// 3.
func (pr *replica) startApplyDestroy(tombstoneInCluster bool, why string) {
	pr.logger.Info("begin to destory",
		zap.Bool("tombstone-in-cluster", tombstoneInCluster),
		log.ReasonField(why))

	pr.store.removeDroppedVoteMsg(pr.shardID)
	// TODO: stop the replica here

	if err := pr.doApplyDestory(tombstoneInCluster); err != nil {
		pr.logger.Fatal("fail to destory",
			zap.Error(err))
	}
}

func (pr *replica) doApplyDestory(tombstoneInCluster bool) error {
	// Shard destory need 2 phase
	// Phase1, update the state to Tombstone
	// Phase2, clean up data asynchronously and remove the state key
	// When we restart store, we can see partially data, because Phase1 and Phase2 are not atomic.
	// We will execute cleanup if we found the Tombstone key.

	if tombstoneInCluster {
		pr.sm.setShardState(metapb.ResourceState_Removed)
	}
	shard := pr.getShard()
	index, _ := pr.sm.getAppliedIndexTerm()
	err := pr.sm.saveShardMetedata(index, shard, meta.ReplicaState_Tombstone)
	if err != nil {
		pr.logger.Fatal("fail to do apply destory",
			zap.Error(err))
	}

	if len(shard.Replicas) > 0 {
		err := pr.store.startClearDataJob(shard)
		if err != nil {
			pr.logger.Fatal("fail to do destroy",
				zap.Error(err))
		}
	}

	pr.cancel()
	if len(shard.Replicas) > 0 && !pr.store.removeShardKeyRange(shard) {
		pr.logger.Warn("fail to remove key range",
			zap.Error(err))
	}
	pr.store.removeReplica(pr)
	pr.sm.destroy()
	pr.logger.Info("destroy self complete.",
		zap.Error(err))
	return nil
}
