// Copyright 2020 PingCAP, Inc.
// Modifications copyright (C) 2021 MatrixOrigin.
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

package cluster

import (
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/event"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockhbstream"
	_ "github.com/matrixorigin/matrixcube/components/prophet/schedulers"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/stretchr/testify/assert"
)

func TestCreateShards(t *testing.T) {
	cluster, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()

	cluster.coordinator = co

	var changedRes *metapb.Shard
	var changedResFrom metapb.ShardState
	var changedResTo metapb.ShardState
	cluster.shardStateChangedHandler = func(res *metapb.Shard, from metapb.ShardState, to metapb.ShardState) {
		changedRes = res
		changedResFrom = from
		changedResTo = to
	}

	res := newTestShardMeta(1)
	res.SetUnique("res1")
	data, err := res.Marshal()
	assert.NoError(t, err)
	req := &rpcpb.ProphetRequest{}
	req.CreateShards.Shards = append(req.CreateShards.Shards, data)

	_, err = cluster.HandleCreateShards(req)
	assert.Error(t, err)

	cluster.addShardStore(1, 1)
	_, err = cluster.HandleCreateShards(req)
	assert.Error(t, err)

	cluster.addShardStore(2, 1)
	_, err = cluster.HandleCreateShards(req)
	assert.Error(t, err)

	cluster.addShardStore(3, 1)
	_, err = cluster.HandleCreateShards(req)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(cluster.core.WaitingCreateShards))
	assert.Equal(t, 0, cluster.GetShardCount())

	// recreate
	_, err = cluster.HandleCreateShards(req)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(cluster.core.WaitingCreateShards))
	assert.Equal(t, 0, cluster.GetShardCount())

	cluster.doNotifyCreateShards()
	e := <-cluster.ChangedEventNotifier()
	assert.Equal(t, event.ShardEvent, e.Type)
	assert.True(t, e.ShardEvent.Create)

	for _, res := range cluster.core.WaitingCreateShards {
		v, err := cluster.storage.GetShard(res.GetID())
		assert.NoError(t, err)
		assert.Equal(t, metapb.ShardState_Creating, v.GetState())

		assert.NoError(t, cluster.HandleShardHeartbeat(core.NewCachedShard(res, &res.GetReplicas()[0])))
		assert.Equal(t, 1, cluster.GetShardCount())
		assert.Equal(t, 0, len(cluster.core.WaitingCreateShards))
		assert.NotNil(t, changedRes)
		assert.Equal(t, changedResFrom, metapb.ShardState_Creating)
		assert.Equal(t, changedResTo, metapb.ShardState_Running)

		v, err = cluster.storage.GetShard(res.GetID())
		assert.NoError(t, err)
		assert.Equal(t, metapb.ShardState_Running, v.GetState())
	}
}

func TestCreateShardsRestart(t *testing.T) {
	cluster, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()

	cluster.coordinator = co

	res := newTestShardMeta(1)
	data, err := res.Marshal()
	assert.NoError(t, err)
	req := &rpcpb.ProphetRequest{}
	req.CreateShards.Shards = append(req.CreateShards.Shards, data)

	cluster.addShardStore(1, 1)
	cluster.addShardStore(2, 1)
	cluster.addShardStore(3, 1)
	_, err = cluster.HandleCreateShards(req)
	assert.NoError(t, err)

	// restart
	tc := newTestRaftCluster(cluster.GetOpts(), cluster.storage, core.NewBasicCluster(nil))
	tc.LoadClusterInfo()
	assert.Equal(t, 1, len(tc.core.WaitingCreateShards))
	tc.doNotifyCreateShards()
	e := <-tc.ChangedEventNotifier()
	assert.Equal(t, event.ShardEvent, e.Type)
	assert.True(t, e.ShardEvent.Create)
}

func TestRemoveShards(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(nil))
	cache := cluster.core.Shards
	storage := cluster.storage
	nc := cluster.ChangedEventNotifier()

	n, np := uint64(7), uint64(3)
	shards := newTestShards(n, np)
	// add 1,2,3,4,5,6
	for i := uint64(1); i < n; i++ {
		cluster.processShardHeartbeat(shards[i])
		checkNotifyCount(t, nc, event.ShardEvent, event.ShardStatsEvent)
	}

	removed := []uint64{1, 2, 3, 4, 5}
	_, err = cluster.HandleRemoveShards(&rpcpb.ProphetRequest{
		RemoveShards: rpcpb.RemoveShardsReq{IDs: removed[:1]},
	})
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), cluster.core.DestroyedShards.GetCardinality())
	assert.True(t, cluster.core.DestroyedShards.Contains(removed[0]))
	assert.Error(t, cluster.processShardHeartbeat(shards[1]))
	checkNotifyCount(t, nc, event.ShardEvent)

	_, err = cluster.HandleRemoveShards(&rpcpb.ProphetRequest{
		RemoveShards: rpcpb.RemoveShardsReq{IDs: removed},
	})
	assert.Error(t, err)

	_, err = cluster.HandleRemoveShards(&rpcpb.ProphetRequest{
		RemoveShards: rpcpb.RemoveShardsReq{IDs: removed[1:]},
	})
	assert.NoError(t, err)
	checkNotifyCount(t, nc, event.ShardEvent, event.ShardEvent, event.ShardEvent, event.ShardEvent)
	assert.Equal(t, uint64(5), cluster.core.DestroyedShards.GetCardinality())
	for _, id := range removed {
		assert.True(t, cluster.core.DestroyedShards.Contains(id))
	}
	assert.Equal(t, 1, cache.GetShardCount())
	assert.Equal(t, metapb.ShardState_Running, cache.GetShard(n-1).Meta.GetState())

	cnt := uint64(0)
	storage.LoadShards(10, func(r metapb.Shard) {
		if r.GetID() < n-1 {
			assert.Equal(t, metapb.ShardState_Destroyed, r.GetState())
		} else {
			assert.Equal(t, metapb.ShardState_Running, r.GetState())
		}
		cnt++
	})
	assert.Equal(t, n-1, cnt)

	// restart
	cluster = newTestRaftCluster(opt, storage, core.NewBasicCluster(nil))
	cluster.LoadClusterInfo()
	cache = cluster.core.Shards
	assert.Equal(t, uint64(len(removed)), cluster.core.DestroyedShards.GetCardinality())
	for _, id := range removed {
		assert.True(t, cluster.core.DestroyedShards.Contains(id))
	}
	assert.NotNil(t, cache.GetShard(n-1))
	assert.Equal(t, 1, cache.GetShardCount())
}

func TestShardHeartbeatAtRemovedState(t *testing.T) {
	cluster, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()

	cluster.coordinator = co

	assert.Nil(t, cluster.addLeaderStore(1, 1))

	n, np := uint64(2), uint64(3)
	shards := newTestShards(n, np)
	// add 1
	for i := uint64(1); i < n; i++ {
		cluster.processShardHeartbeat(shards[i])
	}
	cluster.HandleRemoveShards(&rpcpb.ProphetRequest{
		RemoveShards: rpcpb.RemoveShardsReq{IDs: []uint64{1}},
	})

	stream := mockhbstream.NewHeartbeatStream()
	co.hbStreams.BindStream(shards[1].Meta.GetReplicas()[0].StoreID, stream)
	assert.NoError(t, cluster.HandleShardHeartbeat(shards[1]))
	rsp := stream.Recv()
	assert.NotNil(t, rsp)
	assert.True(t, rsp.DestroyDirectly)
}

func TestHandleCheckShardState(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(nil))

	n, np := uint64(7), uint64(3)
	shards := newTestShards(n, np)
	// add 1,2,3,4,5,6
	for i := uint64(1); i < n; i++ {
		cluster.processShardHeartbeat(shards[i])
	}

	ids := []uint64{1, 2, 3, 4, 5, 6}
	bm := util.MustMarshalBM64(roaring64.BitmapOf(ids...))
	rsp, err := cluster.HandleCheckShardState(&rpcpb.ProphetRequest{
		CheckShardState: rpcpb.CheckShardStateReq{
			IDs: bm,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), util.MustUnmarshalBM64(rsp.Destroyed).GetCardinality())

	cluster.HandleRemoveShards(&rpcpb.ProphetRequest{
		RemoveShards: rpcpb.RemoveShardsReq{IDs: ids[:3]},
	})
	rsp, err = cluster.HandleCheckShardState(&rpcpb.ProphetRequest{
		CheckShardState: rpcpb.CheckShardStateReq{
			IDs: bm,
		},
	})
	assert.NoError(t, err)
	destroyed := util.MustUnmarshalBM64(rsp.Destroyed).ToArray()
	assert.Equal(t, ids[:3], destroyed)
	assert.Equal(t, 3, len(destroyed))

	// restart
	cluster = newTestRaftCluster(opt, cluster.storage, core.NewBasicCluster(nil))
	cluster.LoadClusterInfo()
	rsp, err = cluster.HandleCheckShardState(&rpcpb.ProphetRequest{
		CheckShardState: rpcpb.CheckShardStateReq{
			IDs: bm,
		},
	})
	assert.NoError(t, err)
	destroyed = util.MustUnmarshalBM64(rsp.Destroyed).ToArray()
	assert.Equal(t, ids[:3], destroyed)
	assert.Equal(t, 3, len(destroyed))
}

func checkNotifyCount(t *testing.T, nc <-chan rpcpb.EventNotify, expectNotifyTypes ...uint32) {
	for _, nt := range expectNotifyTypes {
		select {
		case n := <-nc:
			assert.Equal(t, nt, n.Type)
		default:
			assert.FailNow(t, "missing notify")
		}
	}
}
