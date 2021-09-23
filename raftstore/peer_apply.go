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
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

func (pr *peerReplica) doApplyCommittedEntries(commitedEntries []raftpb.Entry) error {
	logger.Debugf("shard %d peer %d async apply raft log with %d entries",
		pr.shardID,
		pr.peer.ID,
		len(commitedEntries))

	pr.sm.applyCommittedEntries(commitedEntries)
	if pr.sm.isPendingRemove() {
		pr.doApplyDestory(false)
	}

	return nil
}

func (pr *peerReplica) doApplyDestory(tombstoneInCluster bool) error {
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
	err := pr.sm.saveShardMetedata(index, shard, meta.PeerState_Tombstone)
	if err != nil {
		logger2.Fatal("fail to do apply destory",
			pr.field,
			zap.Error(err))
	}

	if len(shard.Peers) > 0 {
		err := pr.store.startClearDataJob(shard)
		if err != nil {
			logger2.Fatal("fail to do destroy",
				pr.field,
				zap.Error(err))
		}
	}

	pr.cancel()
	if len(shard.Peers) > 0 && !pr.store.removeShardKeyRange(shard) {
		logger2.Warn("fail to remove key range",
			pr.field,
			zap.Error(err))
	}
	pr.store.removePR(pr)
	pr.sm.destroy()
	logger2.Info("destroy self complete.",
		pr.field,
		zap.Error(err))
	return nil
}
