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
	"testing"

	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestIsEpochStale(t *testing.T) {
	defer leaktest.AfterTest(t)()

	assert.True(t, isEpochStale(metapb.ShardEpoch{ConfVer: 1}, metapb.ShardEpoch{ConfVer: 2}))
	assert.True(t, isEpochStale(metapb.ShardEpoch{Version: 1}, metapb.ShardEpoch{Version: 2}))

	assert.False(t, isEpochStale(metapb.ShardEpoch{ConfVer: 2}, metapb.ShardEpoch{ConfVer: 1}))
	assert.False(t, isEpochStale(metapb.ShardEpoch{Version: 2}, metapb.ShardEpoch{Version: 1}))
}

func TestFindReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()

	shard := Shard{
		Replicas: []Replica{
			{ID: 1, StoreID: 10000},
			{ID: 2, StoreID: 20000},
			{ID: 3, StoreID: 30000},
		},
	}

	assert.NotNil(t, findReplica(shard, 10000))
	assert.NotNil(t, findReplica(shard, 20000))
	assert.NotNil(t, findReplica(shard, 30000))
	assert.Nil(t, findReplica(shard, 40000))
}

func TestRemoveReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()

	shard := &Shard{
		Replicas: []Replica{
			{ID: 1, StoreID: 10000},
			{ID: 2, StoreID: 20000},
			{ID: 3, StoreID: 30000},
		},
	}

	v := removeReplica(shard, 10000)
	assert.NotNil(t, v)
	assert.Equal(t, Replica{ID: 1, StoreID: 10000}, *v)
	assert.Equal(t, 2, len(shard.Replicas))

	assert.Nil(t, removeReplica(shard, 10000))
	assert.Equal(t, 2, len(shard.Replicas))
}

func TestRemovedReplicas(t *testing.T) {
	defer leaktest.AfterTest(t)()

	old := Shard{
		Replicas: []Replica{
			{ID: 1, StoreID: 10000},
			{ID: 2, StoreID: 20000},
			{ID: 3, StoreID: 30000},
		},
	}

	new := Shard{
		Replicas: []Replica{
			{ID: 1, StoreID: 10000},
			{ID: 5, StoreID: 50000},
			{ID: 4, StoreID: 40000},
		},
	}

	ids := removedReplicas(new, old)
	assert.Equal(t, 2, len(ids))
	assert.Equal(t, uint64(2), ids[0])
	assert.Equal(t, uint64(3), ids[1])

	old = Shard{
		Replicas: []Replica{
			{ID: 1, StoreID: 10000},
			{ID: 2, StoreID: 20000},
			{ID: 3, StoreID: 30000},
		},
	}
	new = Shard{
		Replicas: []Replica{
			{ID: 6, StoreID: 60000},
			{ID: 5, StoreID: 50000},
			{ID: 4, StoreID: 40000},
		},
	}

	ids = removedReplicas(new, old)
	assert.Equal(t, 3, len(ids))
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, uint64(2), ids[1])
	assert.Equal(t, uint64(3), ids[2])

	ids = removedReplicas(new, new)
	assert.Equal(t, 0, len(ids))
}
