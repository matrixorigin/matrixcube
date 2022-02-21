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
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
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

	var changedRes metadata.Shard
	var changedResFrom metapb.ShardState
	var changedResTo metapb.ShardState
	cluster.resourceStateChangedHandler = func(res metadata.Shard, from metapb.ShardState, to metapb.ShardState) {
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
	assert.Equal(t, 1, len(cluster.core.WaittingCreateShards))
	assert.Equal(t, 0, cluster.GetShardCount())

	// recreate
	_, err = cluster.HandleCreateShards(req)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(cluster.core.WaittingCreateShards))
	assert.Equal(t, 0, cluster.GetShardCount())

	cluster.doNotifyCreateShards()
	e := <-cluster.ChangedEventNotifier()
	assert.Equal(t, event.EventShard, e.Type)
	assert.True(t, e.ShardEvent.Create)

	for _, res := range cluster.core.WaittingCreateShards {
		v, err := cluster.storage.GetShard(res.ID())
		assert.NoError(t, err)
		assert.Equal(t, metapb.ShardState_Creating, v.State())

		assert.NoError(t, cluster.HandleShardHeartbeat(core.NewCachedShard(res, &res.Peers()[0])))
		assert.Equal(t, 1, cluster.GetShardCount())
		assert.Equal(t, 0, len(cluster.core.WaittingCreateShards))
		assert.NotNil(t, changedRes)
		assert.Equal(t, changedResFrom, metapb.ShardState_Creating)
		assert.Equal(t, changedResTo, metapb.ShardState_Running)

		v, err = cluster.storage.GetShard(res.ID())
		assert.NoError(t, err)
		assert.Equal(t, metapb.ShardState_Running, v.State())
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
	tc := newTestRaftCluster(cluster.GetOpts(), cluster.storage, core.NewBasicCluster(metadata.TestShardFactory, nil))
	tc.LoadClusterInfo()
	assert.Equal(t, 1, len(tc.core.WaittingCreateShards))
	tc.doNotifyCreateShards()
	e := <-tc.ChangedEventNotifier()
	assert.Equal(t, event.EventShard, e.Type)
	assert.True(t, e.ShardEvent.Create)
}

func TestRemoveShards(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestShardFactory, nil))
	cache := cluster.core.Shards
	storage := cluster.storage
	nc := cluster.ChangedEventNotifier()

	n, np := uint64(7), uint64(3)
	resources := newTestShards(n, np)
	// add 1,2,3,4,5,6
	for i := uint64(1); i < n; i++ {
		cluster.processShardHeartbeat(resources[i])
		checkNotifyCount(t, nc, event.EventShard, event.EventShardStats)
	}

	removed := []uint64{1, 2, 3, 4, 5}
	_, err = cluster.HandleRemoveShards(&rpcpb.ProphetRequest{
		RemoveShards: rpcpb.RemoveShardsReq{IDs: removed[:1]},
	})
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), cluster.core.DestroyedShards.GetCardinality())
	assert.True(t, cluster.core.DestroyedShards.Contains(removed[0]))
	assert.Error(t, cluster.processShardHeartbeat(resources[1]))
	checkNotifyCount(t, nc, event.EventShard)

	_, err = cluster.HandleRemoveShards(&rpcpb.ProphetRequest{
		RemoveShards: rpcpb.RemoveShardsReq{IDs: removed},
	})
	assert.Error(t, err)

	_, err = cluster.HandleRemoveShards(&rpcpb.ProphetRequest{
		RemoveShards: rpcpb.RemoveShardsReq{IDs: removed[1:]},
	})
	assert.NoError(t, err)
	checkNotifyCount(t, nc, event.EventShard, event.EventShard, event.EventShard, event.EventShard)
	assert.Equal(t, uint64(5), cluster.core.DestroyedShards.GetCardinality())
	for _, id := range removed {
		assert.True(t, cluster.core.DestroyedShards.Contains(id))
	}
	assert.Equal(t, 1, cache.GetShardCount())
	assert.Equal(t, metapb.ShardState_Running, cache.GetShard(n-1).Meta.State())

	cnt := uint64(0)
	storage.LoadShards(10, func(r metadata.Shard) {
		if r.ID() < n-1 {
			assert.Equal(t, metapb.ShardState_Destroyed, r.State())
		} else {
			assert.Equal(t, metapb.ShardState_Running, r.State())
		}
		cnt++
	})
	assert.Equal(t, n-1, cnt)

	// restart
	cluster = newTestRaftCluster(opt, storage, core.NewBasicCluster(metadata.TestShardFactory, nil))
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
	resources := newTestShards(n, np)
	// add 1
	for i := uint64(1); i < n; i++ {
		cluster.processShardHeartbeat(resources[i])
	}
	cluster.HandleRemoveShards(&rpcpb.ProphetRequest{
		RemoveShards: rpcpb.RemoveShardsReq{IDs: []uint64{1}},
	})

	stream := mockhbstream.NewHeartbeatStream()
	co.hbStreams.BindStream(resources[1].Meta.Peers()[0].StoreID, stream)
	assert.NoError(t, cluster.HandleShardHeartbeat(resources[1]))
	rsp := stream.Recv()
	assert.NotNil(t, rsp)
	assert.True(t, rsp.DestroyDirectly)
}

func TestHandleCheckShardState(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestShardFactory, nil))

	n, np := uint64(7), uint64(3)
	resources := newTestShards(n, np)
	// add 1,2,3,4,5,6
	for i := uint64(1); i < n; i++ {
		cluster.processShardHeartbeat(resources[i])
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
	cluster = newTestRaftCluster(opt, cluster.storage, core.NewBasicCluster(metadata.TestShardFactory, nil))
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
