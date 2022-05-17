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

package aware

import "github.com/matrixorigin/matrixcube/pb/metapb"

// ShardStateAware shard state aware
type ShardStateAware interface {
	// Created the shard was created on the current store
	Created(metapb.Shard)
	// Updated the shard was updated on the current store
	Updated(metapb.Shard)
	// Splited the shard was splited on the current store
	Splited(metapb.Shard)
	// Destroyed the shard was destroyed on the current store
	Destroyed(metapb.Shard)
	// SnapshotApplied snapshot applied
	SnapshotApplied(metapb.Shard)
	// BecomeLeader the shard was become leader on the current store
	BecomeLeader(metapb.Shard)
	// BecomeLeader the shard was become follower on the current store
	BecomeFollower(metapb.Shard)
	// LeaseChanged the lease of the shard is changed.
	LeaseChanged(shard metapb.Shard, lease *metapb.EpochLease, replica metapb.Replica)
}

// TestShardStateAware just for test
type TestShardStateAware interface {
	ShardStateAware
	SetWrapper(ShardStateAware)
}
