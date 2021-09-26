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
)

// doCreateDynamically When we call the prophet client to dynamically create a shard,
// the watcher will receive the creation command, and this callback will be triggered.
// Called in prophet event handle goroutine.
func (s *store) doDynamicallyCreate(shard Shard) {
	if _, ok := s.replicas.Load(shard.ID); ok {
		return
	}

	pr, err := createReplica(s, &shard, "event")
	if err != nil {
		return
	}

	// already created by raft message
	if !s.addReplica(pr) {
		return
	}

	// fix issue 166, we must store the initialization state before pr joins the store
	// to avoid data problems caused by concurrent modification of this state.
	if s.cfg.Test.SaveDynamicallyShardInitStateWait > 0 {
		time.Sleep(s.cfg.Test.SaveDynamicallyShardInitStateWait)
	}
	s.mustSaveShards(shard)
	pr.start()

	for _, p := range shard.Replicas {
		s.replicaRecords.Store(p.ID, p)
	}
	s.updateShardKeyRange(shard)
}
