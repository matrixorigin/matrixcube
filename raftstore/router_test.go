// Copyright 2021 MatrixOrigin.
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

	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/prophet/event"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestHandleInitEvent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := NewTestDataBuilder()
	rr, err := newRouterBuilder().build(make(chan rpcpb.EventNotify))
	assert.NoError(t, err)
	r := rr.(*defaultRouter)

	shard := b.CreateShard(1, "100/101,200/201,300/301")
	store := metapb.Store{ID: 101}
	e := rpcpb.EventNotify{}
	e.Type = event.InitEvent
	e.InitEvent = &rpcpb.InitEventData{
		Shards:           [][]byte{protoc.MustMarshal(&shard)},
		Stores:           [][]byte{protoc.MustMarshal(&store)},
		LeaderReplicaIDs: []uint64{100},
		Leases:           []metapb.EpochLease{{Epoch: 1, ReplicaID: 100}},
	}
	r.handleEvent(e)

	assert.Equal(t, shard, r.mu.shards[shard.ID])
	assert.Equal(t, shard, r.mu.keyRanges[0].Search(shard.Start))
	assert.Equal(t, store, r.mu.stores[store.ID])
	assert.Equal(t, store, r.mu.leaders[shard.ID])
	assert.Equal(t, store, r.mu.leases[shard.ID].store)
	_, ok := r.mu.missingLeaderStoreShards[shard.ID]
	assert.False(t, ok)
	_, ok = r.mu.missingLeaseStoreShards[shard.ID]
	assert.False(t, ok)
}

func TestHandleShardEvent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := NewTestDataBuilder()
	rr, err := newRouterBuilder().build(make(chan rpcpb.EventNotify))
	assert.NoError(t, err)
	r := rr.(*defaultRouter)

	shard := b.CreateShard(1, "100/101,200/201,300/301")

	e := rpcpb.EventNotify{}
	e.Type = event.ShardEvent
	e.ShardEvent = &rpcpb.ShardEventData{
		Data: protoc.MustMarshal(&shard),
	}
	r.handleEvent(e)

	assert.Equal(t, shard, r.mu.shards[shard.ID])
	assert.Equal(t, shard, r.mu.keyRanges[0].Search(shard.Start))

	_, ok := r.mu.leaders[shard.ID]
	assert.False(t, ok)

	_, ok = r.mu.leaders[shard.ID]
	assert.False(t, ok)
}

func TestHandleShardEventWithLeader(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := NewTestDataBuilder()
	rr, err := newRouterBuilder().build(make(chan rpcpb.EventNotify))
	assert.NoError(t, err)
	r := rr.(*defaultRouter)

	shard := b.CreateShard(1, "100/101,200/201,300/301")
	store := metapb.Store{ID: 101}

	e := rpcpb.EventNotify{}
	e.Type = event.StoreEvent
	e.StoreEvent = &rpcpb.StoreEventData{
		Data: protoc.MustMarshal(&store),
	}
	r.handleEvent(e)

	e.Type = event.ShardEvent
	e.ShardEvent = &rpcpb.ShardEventData{
		Data:            protoc.MustMarshal(&shard),
		LeaderReplicaID: 100,
	}
	r.handleEvent(e)

	assert.Equal(t, shard, r.mu.shards[shard.ID])
	assert.Equal(t, shard, r.mu.keyRanges[0].Search(shard.Start))
	assert.Equal(t, store, r.mu.stores[store.ID])
	assert.Equal(t, store, r.mu.leaders[shard.ID])
	_, ok := r.mu.missingLeaderStoreShards[shard.ID]
	assert.False(t, ok)
}

func TestHandleShardEventWithLease(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := NewTestDataBuilder()
	rr, err := newRouterBuilder().build(make(chan rpcpb.EventNotify))
	assert.NoError(t, err)
	r := rr.(*defaultRouter)

	shard := b.CreateShard(1, "100/101,200/201,300/301")
	store := metapb.Store{ID: 101}

	e := rpcpb.EventNotify{}
	e.Type = event.StoreEvent
	e.StoreEvent = &rpcpb.StoreEventData{
		Data: protoc.MustMarshal(&store),
	}
	r.handleEvent(e)

	e.Type = event.ShardEvent
	e.ShardEvent = &rpcpb.ShardEventData{
		Data:  protoc.MustMarshal(&shard),
		Lease: &metapb.EpochLease{Epoch: 1, ReplicaID: 100},
	}
	r.handleEvent(e)

	assert.Equal(t, shard, r.mu.shards[shard.ID])
	assert.Equal(t, shard, r.mu.keyRanges[0].Search(shard.Start))
	assert.Equal(t, store, r.mu.stores[store.ID])
	assert.Equal(t, store, r.mu.leases[shard.ID].store)
	_, ok := r.mu.missingLeaseStoreShards[shard.ID]
	assert.False(t, ok)
}

func TestHandleShardEventWithMissingLeaderStore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := NewTestDataBuilder()
	rr, err := newRouterBuilder().build(make(chan rpcpb.EventNotify))
	assert.NoError(t, err)
	r := rr.(*defaultRouter)

	shard := b.CreateShard(1, "100/101,200/201,300/301")
	store := metapb.Store{ID: 101}

	e := rpcpb.EventNotify{}
	e.Type = event.ShardEvent
	e.ShardEvent = &rpcpb.ShardEventData{
		Data:            protoc.MustMarshal(&shard),
		LeaderReplicaID: 100,
	}
	r.handleEvent(e)

	assert.Equal(t, shard, r.mu.shards[shard.ID])
	assert.Equal(t, shard, r.mu.keyRanges[0].Search(shard.Start))
	assert.Equal(t, metapb.Store{}, r.mu.leaders[shard.ID])
	assert.Equal(t, Replica{ID: 100, StoreID: 101}, r.mu.missingLeaderStoreShards[shard.ID])

	e.Type = event.StoreEvent
	e.StoreEvent = &rpcpb.StoreEventData{
		Data: protoc.MustMarshal(&store),
	}
	r.handleEvent(e)

	assert.Equal(t, shard, r.mu.shards[shard.ID])
	assert.Equal(t, shard, r.mu.keyRanges[0].Search(shard.Start))
	assert.Equal(t, store, r.mu.leaders[shard.ID])
	assert.Equal(t, Replica{}, r.mu.missingLeaderStoreShards[shard.ID])
}

func TestHandleShardEventWithMissingLeaseStore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := NewTestDataBuilder()
	rr, err := newRouterBuilder().build(make(chan rpcpb.EventNotify))
	assert.NoError(t, err)
	r := rr.(*defaultRouter)

	shard := b.CreateShard(1, "100/101,200/201,300/301")
	store := metapb.Store{ID: 101}

	e := rpcpb.EventNotify{}
	e.Type = event.ShardEvent
	e.ShardEvent = &rpcpb.ShardEventData{
		Data:  protoc.MustMarshal(&shard),
		Lease: &metapb.EpochLease{Epoch: 1, ReplicaID: 100},
	}
	r.handleEvent(e)

	assert.Equal(t, shard, r.mu.shards[shard.ID])
	assert.Equal(t, shard, r.mu.keyRanges[0].Search(shard.Start))
	assert.Equal(t, leaseInfo{}, r.mu.leases[shard.ID])
	assert.Equal(t, leaseInfo{lease: &metapb.EpochLease{Epoch: 1, ReplicaID: 100}, store: metapb.Store{ID: 101}}, r.mu.missingLeaseStoreShards[shard.ID])

	e.Type = event.StoreEvent
	e.StoreEvent = &rpcpb.StoreEventData{
		Data: protoc.MustMarshal(&store),
	}
	r.handleEvent(e)

	assert.Equal(t, shard, r.mu.shards[shard.ID])
	assert.Equal(t, shard, r.mu.keyRanges[0].Search(shard.Start))
	assert.Equal(t, leaseInfo{lease: &metapb.EpochLease{Epoch: 1, ReplicaID: 100}, store: store}, r.mu.leases[shard.ID])
	assert.Equal(t, leaseInfo{}, r.mu.missingLeaseStoreShards[shard.ID])
}

func TestHandleStoreEvent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rr, err := newRouterBuilder().build(make(chan rpcpb.EventNotify))
	assert.NoError(t, err)
	r := rr.(*defaultRouter)
	store := metapb.Store{ID: 101}

	e := rpcpb.EventNotify{}
	e.Type = event.StoreEvent
	e.StoreEvent = &rpcpb.StoreEventData{
		Data: protoc.MustMarshal(&store),
	}
	r.handleEvent(e)

	assert.Equal(t, store, r.mu.stores[store.ID])
}

func TestSelectShard(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := NewTestDataBuilder()

	cases := []struct {
		key             []byte
		leaderReplicaID uint64
		shard           Shard
		store           metapb.Store
		expectID        uint64
		expectAddr      string
	}{
		{
			key:             b.CreateShard(1, "100/101,200/201,300/301").Start,
			leaderReplicaID: 100,
			shard:           b.CreateShard(1, "100/101,200/201,300/301"),
			store:           metapb.Store{ID: 101, ClientAddress: "101"},
			expectID:        1,
			expectAddr:      "101",
		},
		{
			key:             b.CreateShard(2, "").Start,
			leaderReplicaID: 100,
			shard:           b.CreateShard(1, "100/101,200/201,300/301"),
			store:           metapb.Store{ID: 101, ClientAddress: "101"},
			expectID:        0,
			expectAddr:      "",
		},
		{
			key:             b.CreateShard(1, "").Start,
			leaderReplicaID: 100,
			shard:           b.CreateShard(1, "100/101,200/201,300/301"),
			store:           metapb.Store{ID: 102, ClientAddress: "102"},
			expectID:        1,
			expectAddr:      "",
		},
	}

	for i, c := range cases {
		rr, err := newRouterBuilder().build(make(chan rpcpb.EventNotify))
		assert.NoError(t, err)
		r := rr.(*defaultRouter)
		r.updateShardLocked(protoc.MustMarshal(&c.shard), c.leaderReplicaID, nil, false, false)
		r.updateStoreLocked(protoc.MustMarshal(&c.store))

		shard, addr := r.SelectShard(0, c.key)
		assert.Equal(t, c.expectID, shard.ID, "index %d", i)
		assert.Equal(t, c.expectAddr, addr, "index %d", i)
	}
}

func TestForeachShards(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := NewTestDataBuilder()

	cases := []struct {
		shards []Shard
	}{
		{
			shards: []Shard{b.CreateShard(1, "")},
		},
		{
			shards: []Shard{b.CreateShard(1, ""), b.CreateShard(2, "")},
		},
	}

	for i, c := range cases {
		rr, err := newRouterBuilder().build(make(chan rpcpb.EventNotify))
		assert.NoError(t, err)
		r := rr.(*defaultRouter)

		for _, s := range c.shards {
			r.updateShardLocked(protoc.MustMarshal(&s), 0, nil, false, false)
		}

		n := 0
		r.ForeachShards(0, func(shard Shard) bool {
			n++
			return true
		})
		assert.Equal(t, len(c.shards), n, "index %d", i)

		n = 0
		r.ForeachShards(0, func(shard Shard) bool {
			n++
			return false
		})
		assert.Equal(t, 1, n, "index %d", i)
	}
}

func TestGetShard(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := NewTestDataBuilder()

	cases := []struct {
		shard Shard
		id    uint64
	}{
		{
			shard: b.CreateShard(1, ""),
			id:    1,
		},
	}

	for i, c := range cases {
		rr, err := newRouterBuilder().build(make(chan rpcpb.EventNotify))
		assert.NoError(t, err)
		r := rr.(*defaultRouter)

		r.updateShardLocked(protoc.MustMarshal(&c.shard), 0, nil, false, false)
		assert.Equal(t, c.shard, r.GetShard(c.id), "index %d", i)
	}
}

func TestSelectShardByPolicy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	b := NewTestDataBuilder()
	cases := []struct {
		key             []byte
		leaderReplicaID uint64
		lease           *metapb.EpochLease
		shard           Shard
		stores          []metapb.Store
		policy          rpcpb.ReplicaSelectPolicy
		expectStoreID   []uint64
		expectShardID   uint64
	}{
		{
			key:             b.CreateShard(1, "100/101,200/201,300/301").Start,
			leaderReplicaID: 100,
			lease:           &metapb.EpochLease{Epoch: 1, ReplicaID: 200},
			shard:           b.CreateShard(1, "100/101,200/201,300/301"),
			stores:          []metapb.Store{{ID: 101}, {ID: 201}, {ID: 301}},
			policy:          rpcpb.SelectLeader,
			expectShardID:   1,
			expectStoreID:   []uint64{101, 101},
		},
		{
			key:             b.CreateShard(1, "100/101,200/201,300/301").Start,
			leaderReplicaID: 100,
			lease:           &metapb.EpochLease{Epoch: 1, ReplicaID: 200},
			shard:           b.CreateShard(1, "100/101,200/201,300/301"),
			stores:          []metapb.Store{{ID: 101}, {ID: 201}, {ID: 301}},
			policy:          rpcpb.SelectRandom,
			expectShardID:   1,
			expectStoreID:   []uint64{101, 301},
		},
		{
			key:             b.CreateShard(2, "").Start,
			leaderReplicaID: 100,
			lease:           &metapb.EpochLease{Epoch: 1, ReplicaID: 200},
			shard:           b.CreateShard(1, "100/101,200/201,300/301"),
			stores:          []metapb.Store{{ID: 101}, {ID: 201}, {ID: 301}},
			policy:          rpcpb.SelectLeaseHolder,
			expectShardID:   0,
			expectStoreID:   []uint64{0, 0},
		},
		{
			key:             b.CreateShard(1, "100/101,200/201,300/301").Start,
			leaderReplicaID: 100,
			lease:           &metapb.EpochLease{Epoch: 1, ReplicaID: 200},
			shard:           b.CreateShard(1, "100/101,200/201,300/301"),
			stores:          []metapb.Store{{ID: 101}, {ID: 201}, {ID: 301}},
			policy:          rpcpb.SelectLeaseHolder,
			expectShardID:   1,
			expectStoreID:   []uint64{201, 201},
		},
	}

	for i, c := range cases {
		rr, err := newRouterBuilder().build(make(chan rpcpb.EventNotify))
		assert.NoError(t, err)
		r := rr.(*defaultRouter)
		r.updateShardLocked(protoc.MustMarshal(&c.shard), c.leaderReplicaID, c.lease, false, false)
		for _, s := range c.stores {
			r.updateStoreLocked(protoc.MustMarshal(&s))
		}

		shard, store, lease := r.SelectShardWithPolicy(0, c.key, c.policy)
		assert.Equal(t, c.expectShardID, shard.ID, "index %d", i)
		assert.True(t, c.expectStoreID[0] <= store.ID && store.ID <= c.expectStoreID[1], "index %d", i)
		if c.expectShardID > 0 && c.policy == rpcpb.SelectLeaseHolder {
			assert.Equal(t, c.lease, lease, "index %d", i)
		} else {
			assert.Nil(t, lease, "index %d", i)
		}
	}
}

func TestAscendRange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := make(chan rpcpb.EventNotify)
	defer close(c)
	rr, err := newRouterBuilder().build(c)
	assert.NoError(t, err)
	r := rr.(*defaultRouter)

	b := NewTestDataBuilder()
	s1 := b.CreateShard(1, "10/11,20/21,30/31")
	s2 := b.CreateShard(2, "100/101,200/201,300/301")
	s3 := b.CreateShard(3, "1000/1001,2000/2001,3000/3001")
	r.updateShardLocked(protoc.MustMarshal(&s1), 10, nil, false, false)
	r.updateShardLocked(protoc.MustMarshal(&s2), 100, nil, false, false)
	r.updateShardLocked(protoc.MustMarshal(&s3), 1000, nil, false, false)
	r.updateStoreLocked(protoc.MustMarshal(&metapb.Store{ID: 11}))
	r.updateStoreLocked(protoc.MustMarshal(&metapb.Store{ID: 21}))
	r.updateStoreLocked(protoc.MustMarshal(&metapb.Store{ID: 31}))
	r.updateStoreLocked(protoc.MustMarshal(&metapb.Store{ID: 101}))
	r.updateStoreLocked(protoc.MustMarshal(&metapb.Store{ID: 201}))
	r.updateStoreLocked(protoc.MustMarshal(&metapb.Store{ID: 301}))
	r.updateStoreLocked(protoc.MustMarshal(&metapb.Store{ID: 1001}))
	r.updateStoreLocked(protoc.MustMarshal(&metapb.Store{ID: 2001}))
	r.updateStoreLocked(protoc.MustMarshal(&metapb.Store{ID: 3001}))

	cases := []struct {
		keyRange     []uint64
		policy       rpcpb.ReplicaSelectPolicy
		expectShards []uint64
		expectStores []uint64
	}{
		{
			keyRange:     []uint64{1, 4},
			policy:       rpcpb.SelectLeader,
			expectShards: []uint64{1, 2, 3},
			expectStores: []uint64{11, 101, 1001},
		},
		{
			keyRange:     []uint64{1, 3},
			policy:       rpcpb.SelectLeader,
			expectShards: []uint64{1, 2},
			expectStores: []uint64{11, 101},
		},
		{
			keyRange:     []uint64{1, 2},
			policy:       rpcpb.SelectLeader,
			expectShards: []uint64{1},
			expectStores: []uint64{11},
		},
		{
			keyRange:     []uint64{0, 1},
			policy:       rpcpb.SelectLeader,
			expectShards: nil,
			expectStores: nil,
		},
	}

	for i, c := range cases {
		var shards []uint64
		var stores []uint64
		r.AscendRange(0, format.Uint64ToBytes(c.keyRange[0]), format.Uint64ToBytes(c.keyRange[1]), c.policy, func(shard Shard, replciaStore metapb.Store, _ *metapb.EpochLease) bool {
			shards = append(shards, shard.ID)
			stores = append(stores, replciaStore.ID)
			return true
		})
		assert.Equal(t, c.expectShards, shards, "index %d", i)
		assert.Equal(t, c.expectStores, stores, "index %d", i)
	}
}
