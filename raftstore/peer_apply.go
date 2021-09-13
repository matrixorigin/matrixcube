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
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
)

func (s *store) doDestroy(shardID uint64, tombstone bool, why string) {
	pr := s.getPR(shardID, false)
	if pr != nil {
		if pr.sm != nil {
			pr.sm.destroy()
		}
		if tombstone {
			pr.sm.setShardState(metapb.ResourceState_Removed)
		}
		pr.mustDestroy(why)
	}
}

func (pr *peerReplica) doCompactRaftLog(index uint64) error {
	return pr.store.logdb.RemoveEntriesTo(pr.shardID, pr.peer.ID, index)
}

func (pr *peerReplica) doApplyCommittedEntries(commitedEntries []raftpb.Entry) error {
	logger.Debugf("shard %d peer %d async apply raft log with %d entries",
		pr.shardID,
		pr.peer.ID,
		len(commitedEntries))

	pr.sm.applyCommittedEntries(commitedEntries)

	if pr.sm.isPendingRemove() {
		pr.sm.destroy()
	}

	return nil
}
