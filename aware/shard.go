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

import (
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
)

// ShardStateAware shard state aware
type ShardStateAware interface {
	// Created the shard was created on the current store
	Created(bhmetapb.Shard)
	// Splited the shard was splited on the current store
	Splited(bhmetapb.Shard)
	// Destory the shard was destoryed on the current store
	Destory(bhmetapb.Shard)
	// BecomeLeader the shard was become leader on the current store
	BecomeLeader(bhmetapb.Shard)
	// BecomeLeader the shard was become follower on the current store
	BecomeFollower(bhmetapb.Shard)
	// SnapshotApplied snapshot applied
	SnapshotApplied(bhmetapb.Shard)
}

// TestShardStateAware just for test
type TestShardStateAware interface {
	ShardStateAware
	SetWrapper(ShardStateAware)
}
