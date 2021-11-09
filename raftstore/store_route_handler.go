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

// doCreateDynamically When we call the prophet client to dynamically create a shard,
// the watcher will receive the creation command, and this callback will be triggered.
// Called in prophet event handle goroutine.
func (s *store) doDynamicallyCreate(shard Shard) bool {
	// Prophet receives a request to dynamically create a Shard, assigns the original information of the Shard,
	// and then broadcasts this message in the cluster, and there is no special logic to control which Store these
	// messages should be sent to, it's just a simple cluster-wide broadcast. So here we need to deal with whether
	// we need to create Replica in the current Store.
	if r := findReplica(shard, s.Meta().ID); r == nil {
		return false
	}

	if _, ok := s.replicas.Load(shard.ID); ok {
		return false
	}

	newReplicaCreator(s).
		withReason("event").
		withStartReplica(nil).
		withSaveMetadata(true).
		create([]Shard{shard})
	return true
}
