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

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/prophet/event"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/stretchr/testify/assert"
)

// FIXME: add leaktest checks

func TestHandleInitEvent(t *testing.T) {
	b := NewTestDataBuilder()
	rr, err := newRouterBuilder().build(make(chan rpcpb.EventNotify))
	assert.NoError(t, err)
	r := rr.(*defaultRouter)

	shard := b.CreateShard(1, "100/101,200/201,300/301")
	store := meta.Store{ID: 101}
	e := rpcpb.EventNotify{}
	e.Type = event.EventInit
	e.InitEvent = &rpcpb.InitEventData{
		Resources:  [][]byte{protoc.MustMarshal(&shard)},
		Containers: [][]byte{protoc.MustMarshal(&store)},
		Leaders:    []uint64{100},
	}
	r.handleEvent(e)

	assert.Equal(t, shard, r.mu.shards[shard.ID])
	assert.Equal(t, shard, r.mu.keyRanges[0].Search(shard.Start))
	assert.Equal(t, store, r.mu.stores[store.ID])
	assert.Equal(t, store, r.mu.leaders[shard.ID])
	_, ok := r.mu.missingLeaderStoreShards[shard.ID]
	assert.False(t, ok)
}

func TestHandleResourceEvent(t *testing.T) {
	b := NewTestDataBuilder()
	rr, err := newRouterBuilder().build(make(chan rpcpb.EventNotify))
	assert.NoError(t, err)
	r := rr.(*defaultRouter)

	shard := b.CreateShard(1, "100/101,200/201,300/301")

	e := rpcpb.EventNotify{}
	e.Type = event.EventResource
	e.ResourceEvent = &rpcpb.ResourceEventData{
		Data: protoc.MustMarshal(&shard),
	}
	r.handleEvent(e)

	assert.Equal(t, shard, r.mu.shards[shard.ID])
	assert.Equal(t, shard, r.mu.keyRanges[0].Search(shard.Start))

	_, ok := r.mu.leaders[shard.ID]
	assert.False(t, ok)
}

func TestHandleResourceEventWithLeader(t *testing.T) {
	b := NewTestDataBuilder()
	rr, err := newRouterBuilder().build(make(chan rpcpb.EventNotify))
	assert.NoError(t, err)
	r := rr.(*defaultRouter)

	shard := b.CreateShard(1, "100/101,200/201,300/301")
	store := meta.Store{ID: 101}

	e := rpcpb.EventNotify{}
	e.Type = event.EventContainer
	e.ContainerEvent = &rpcpb.ContainerEventData{
		Data: protoc.MustMarshal(&store),
	}
	r.handleEvent(e)

	e.Type = event.EventResource
	e.ResourceEvent = &rpcpb.ResourceEventData{
		Data:   protoc.MustMarshal(&shard),
		Leader: 100,
	}
	r.handleEvent(e)

	assert.Equal(t, shard, r.mu.shards[shard.ID])
	assert.Equal(t, shard, r.mu.keyRanges[0].Search(shard.Start))
	assert.Equal(t, store, r.mu.stores[store.ID])
	assert.Equal(t, store, r.mu.leaders[shard.ID])
	_, ok := r.mu.missingLeaderStoreShards[shard.ID]
	assert.False(t, ok)
}

func TestHandleResourceEventWithMissingLeaderStore(t *testing.T) {
	b := NewTestDataBuilder()
	rr, err := newRouterBuilder().build(make(chan rpcpb.EventNotify))
	assert.NoError(t, err)
	r := rr.(*defaultRouter)

	shard := b.CreateShard(1, "100/101,200/201,300/301")
	store := meta.Store{ID: 101}

	e := rpcpb.EventNotify{}
	e.Type = event.EventResource
	e.ResourceEvent = &rpcpb.ResourceEventData{
		Data:   protoc.MustMarshal(&shard),
		Leader: 100,
	}
	r.handleEvent(e)

	assert.Equal(t, shard, r.mu.shards[shard.ID])
	assert.Equal(t, shard, r.mu.keyRanges[0].Search(shard.Start))
	assert.Equal(t, meta.Store{}, r.mu.leaders[shard.ID])
	assert.Equal(t, Replica{ID: 100, ContainerID: 101}, r.mu.missingLeaderStoreShards[shard.ID])

	e.Type = event.EventContainer
	e.ContainerEvent = &rpcpb.ContainerEventData{
		Data: protoc.MustMarshal(&store),
	}
	r.handleEvent(e)

	assert.Equal(t, shard, r.mu.shards[shard.ID])
	assert.Equal(t, shard, r.mu.keyRanges[0].Search(shard.Start))
	assert.Equal(t, store, r.mu.leaders[shard.ID])
	assert.Equal(t, Replica{}, r.mu.missingLeaderStoreShards[shard.ID])
}

func TestHandleStoreEvent(t *testing.T) {
	rr, err := newRouterBuilder().build(make(chan rpcpb.EventNotify))
	assert.NoError(t, err)
	r := rr.(*defaultRouter)
	store := meta.Store{ID: 101}

	e := rpcpb.EventNotify{}
	e.Type = event.EventContainer
	e.ContainerEvent = &rpcpb.ContainerEventData{
		Data: protoc.MustMarshal(&store),
	}
	r.handleEvent(e)

	assert.Equal(t, store, r.mu.stores[store.ID])
}

func TestSelectShard(t *testing.T) {
	b := NewTestDataBuilder()

	cases := []struct {
		key             []byte
		leaderReplicaID uint64
		shard           Shard
		store           meta.Store
		expectID        uint64
		expectAddr      string
	}{
		{
			key:             b.CreateShard(1, "100/101,200/201,300/301").Start,
			leaderReplicaID: 100,
			shard:           b.CreateShard(1, "100/101,200/201,300/301"),
			store:           meta.Store{ID: 101, ClientAddr: "101"},
			expectID:        1,
			expectAddr:      "101",
		},
		{
			key:             b.CreateShard(2, "").Start,
			leaderReplicaID: 100,
			shard:           b.CreateShard(1, "100/101,200/201,300/301"),
			store:           meta.Store{ID: 101, ClientAddr: "101"},
			expectID:        0,
			expectAddr:      "",
		},
		{
			key:             b.CreateShard(1, "").Start,
			leaderReplicaID: 100,
			shard:           b.CreateShard(1, "100/101,200/201,300/301"),
			store:           meta.Store{ID: 102, ClientAddr: "102"},
			expectID:        1,
			expectAddr:      "",
		},
	}

	for i, c := range cases {
		rr, err := newRouterBuilder().build(make(chan rpcpb.EventNotify))
		assert.NoError(t, err)
		r := rr.(*defaultRouter)
		r.updateShardLocked(protoc.MustMarshal(&c.shard), c.leaderReplicaID, false, false)
		r.updateStoreLocked(protoc.MustMarshal(&c.store))

		shard, addr := r.SelectShard(0, c.key)
		assert.Equal(t, c.expectID, shard.ID, "index %d", i)
		assert.Equal(t, c.expectAddr, addr, "index %d", i)
	}
}

func TestForeachShards(t *testing.T) {
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
			r.updateShardLocked(protoc.MustMarshal(&s), 0, false, false)
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

		r.updateShardLocked(protoc.MustMarshal(&c.shard), 0, false, false)
		assert.Equal(t, c.shard, r.GetShard(c.id), "index %d", i)
	}
}
