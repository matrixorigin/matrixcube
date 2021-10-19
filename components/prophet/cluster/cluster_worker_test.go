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
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	_ "github.com/matrixorigin/matrixcube/components/prophet/schedulers"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/stretchr/testify/assert"
)

func TestReportSplit(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory, nil))

	left := &metadata.TestResource{ResID: 1, Start: []byte("a"), End: []byte("b")}
	right := &metadata.TestResource{ResID: 2, Start: []byte("b"), End: []byte("c")}
	request := &rpcpb.Request{}
	request.ReportSplit.Left, _ = left.Marshal()
	request.ReportSplit.Right, _ = right.Marshal()
	_, err = cluster.HandleReportSplit(request)
	assert.NoError(t, err)

	request.ReportSplit.Left, _ = right.Marshal()
	request.ReportSplit.Right, _ = left.Marshal()
	_, err = cluster.HandleReportSplit(request)
	assert.Error(t, err)
}

func TestReportBatchSplit(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory, nil))

	resources := []*metadata.TestResource{
		{ResID: 1, Start: []byte(""), End: []byte("a")},
		{ResID: 2, Start: []byte("a"), End: []byte("b")},
		{ResID: 3, Start: []byte("b"), End: []byte("c")},
		{ResID: 3, Start: []byte("c"), End: []byte("")},
	}

	request := &rpcpb.Request{}
	for _, res := range resources {
		v, _ := res.Marshal()
		request.BatchReportSplit.Resources = append(request.BatchReportSplit.Resources, v)
	}

	_, err = cluster.HandleBatchReportSplit(request)
	assert.NoError(t, err)
}

func TestCreateResources(t *testing.T) {
	cluster, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()

	cluster.coordinator = co

	var changedRes metadata.Resource
	var changedResFrom metapb.ResourceState
	var changedResTo metapb.ResourceState
	cluster.resourceStateChangedHandler = func(res metadata.Resource, from metapb.ResourceState, to metapb.ResourceState) {
		changedRes = res
		changedResFrom = from
		changedResTo = to
	}

	res := newTestResourceMeta(1)
	res.SetUnique("res1")
	data, err := res.Marshal()
	assert.NoError(t, err)
	req := &rpcpb.Request{}
	req.CreateResources.Resources = append(req.CreateResources.Resources, data)

	_, err = cluster.HandleCreateResources(req)
	assert.Error(t, err)

	cluster.addResourceContainer(1, 1)
	_, err = cluster.HandleCreateResources(req)
	assert.Error(t, err)

	cluster.addResourceContainer(2, 1)
	_, err = cluster.HandleCreateResources(req)
	assert.Error(t, err)

	cluster.addResourceContainer(3, 1)
	_, err = cluster.HandleCreateResources(req)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(cluster.core.WaittingCreateResources))
	assert.Equal(t, 0, cluster.GetResourceCount())

	// recreate
	_, err = cluster.HandleCreateResources(req)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(cluster.core.WaittingCreateResources))
	assert.Equal(t, 0, cluster.GetResourceCount())

	cluster.doNotifyCreateResources()
	e := <-cluster.ChangedEventNotifier()
	assert.Equal(t, event.EventResource, e.Type)
	assert.True(t, e.ResourceEvent.Create)

	for _, res := range cluster.core.WaittingCreateResources {
		v, err := cluster.storage.GetResource(res.ID())
		assert.NoError(t, err)
		assert.Equal(t, metapb.ResourceState_WaittingCreate, v.State())

		assert.NoError(t, cluster.HandleResourceHeartbeat(core.NewCachedResource(res, &res.Peers()[0])))
		assert.Equal(t, 1, cluster.GetResourceCount())
		assert.Equal(t, 0, len(cluster.core.WaittingCreateResources))
		assert.NotNil(t, changedRes)
		assert.Equal(t, changedResFrom, metapb.ResourceState_WaittingCreate)
		assert.Equal(t, changedResTo, metapb.ResourceState_Running)

		v, err = cluster.storage.GetResource(res.ID())
		assert.NoError(t, err)
		assert.Equal(t, metapb.ResourceState_Running, v.State())
	}
}

func TestCreateResourcesRestart(t *testing.T) {
	cluster, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()

	cluster.coordinator = co

	res := newTestResourceMeta(1)
	data, err := res.Marshal()
	assert.NoError(t, err)
	req := &rpcpb.Request{}
	req.CreateResources.Resources = append(req.CreateResources.Resources, data)

	cluster.addResourceContainer(1, 1)
	cluster.addResourceContainer(2, 1)
	cluster.addResourceContainer(3, 1)
	_, err = cluster.HandleCreateResources(req)
	assert.NoError(t, err)

	// restart
	tc := newTestRaftCluster(cluster.GetOpts(), cluster.storage, core.NewBasicCluster(metadata.TestResourceFactory, nil))
	tc.LoadClusterInfo()
	assert.Equal(t, 1, len(tc.core.WaittingCreateResources))
	tc.doNotifyCreateResources()
	e := <-tc.ChangedEventNotifier()
	assert.Equal(t, event.EventResource, e.Type)
	assert.True(t, e.ResourceEvent.Create)
}

func TestRemoveResources(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory, nil))
	cache := cluster.core.Resources
	storage := cluster.storage
	nc := cluster.ChangedEventNotifier()

	n, np := uint64(7), uint64(3)
	resources := newTestResources(n, np)
	// add 1,2,3,4,5,6
	for i := uint64(1); i < n; i++ {
		cluster.processResourceHeartbeat(resources[i])
		checkNotifyCount(t, nc, event.EventResource, event.EventResourceStats)
	}

	removed := []uint64{1, 2, 3, 4, 5}
	_, err = cluster.HandleRemoveResources(&rpcpb.Request{
		RemoveResources: rpcpb.RemoveResourcesReq{IDs: removed[:1]},
	})
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), cluster.core.RemovedResources.GetCardinality())
	assert.True(t, cluster.core.RemovedResources.Contains(removed[0]))
	assert.Error(t, cluster.processResourceHeartbeat(resources[1]))
	checkNotifyCount(t, nc, event.EventResource)

	_, err = cluster.HandleRemoveResources(&rpcpb.Request{
		RemoveResources: rpcpb.RemoveResourcesReq{IDs: removed},
	})
	assert.Error(t, err)

	_, err = cluster.HandleRemoveResources(&rpcpb.Request{
		RemoveResources: rpcpb.RemoveResourcesReq{IDs: removed[1:]},
	})
	assert.NoError(t, err)
	checkNotifyCount(t, nc, event.EventResource, event.EventResource, event.EventResource, event.EventResource)
	assert.Equal(t, uint64(5), cluster.core.RemovedResources.GetCardinality())
	for _, id := range removed {
		assert.True(t, cluster.core.RemovedResources.Contains(id))
	}

	for i := uint64(1); i < n-1; i++ {
		assert.Equal(t, metapb.ResourceState_Removed, cache.GetResource(i).Meta.State())
	}
	assert.Equal(t, metapb.ResourceState_Running, cache.GetResource(n-1).Meta.State())

	cnt := uint64(0)
	storage.LoadResources(10, func(r metadata.Resource) {
		if r.ID() < n-1 {
			assert.Equal(t, metapb.ResourceState_Removed, r.State())
		} else {
			assert.Equal(t, metapb.ResourceState_Running, r.State())
		}
		cnt++
	})
	assert.Equal(t, n-1, cnt)

	// restart
	cluster = newTestRaftCluster(opt, storage, core.NewBasicCluster(metadata.TestResourceFactory, nil))
	cluster.LoadClusterInfo()
	cache = cluster.core.Resources
	assert.Equal(t, uint64(len(removed)), cluster.core.RemovedResources.GetCardinality())
	for _, id := range removed {
		assert.True(t, cluster.core.RemovedResources.Contains(id))
	}
	assert.NotNil(t, cache.GetResource(n-1))
	assert.Equal(t, 1, cache.GetResourceCount())
}

func TestResourceHeartbeatAtRemovedState(t *testing.T) {
	cluster, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()

	cluster.coordinator = co

	assert.Nil(t, cluster.addLeaderContainer(1, 1))

	n, np := uint64(2), uint64(3)
	resources := newTestResources(n, np)
	// add 1
	for i := uint64(1); i < n; i++ {
		cluster.processResourceHeartbeat(resources[i])
	}
	cluster.HandleRemoveResources(&rpcpb.Request{
		RemoveResources: rpcpb.RemoveResourcesReq{IDs: []uint64{1}},
	})

	stream := mockhbstream.NewHeartbeatStream()
	co.hbStreams.BindStream(resources[1].Meta.Peers()[0].ContainerID, stream)
	assert.NoError(t, cluster.HandleResourceHeartbeat(resources[1]))
	rsp := stream.Recv()
	assert.NotNil(t, rsp)
	assert.True(t, rsp.DestoryDirectly)
}

func TestHandleCheckResourceState(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory, nil))

	n, np := uint64(7), uint64(3)
	resources := newTestResources(n, np)
	// add 1,2,3,4,5,6
	for i := uint64(1); i < n; i++ {
		cluster.processResourceHeartbeat(resources[i])
	}

	ids := []uint64{1, 2, 3, 4, 5, 6}
	bm := util.MustMarshalBM64(roaring64.BitmapOf(ids...))
	rsp, err := cluster.HandleCheckResourceState(&rpcpb.Request{
		CheckResourceState: rpcpb.CheckResourceStateReq{
			IDs: bm,
		},
	})
	assert.NoError(t, err)
	assert.Empty(t, rsp.Removed)

	cluster.HandleRemoveResources(&rpcpb.Request{
		RemoveResources: rpcpb.RemoveResourcesReq{IDs: ids[:3]},
	})
	rsp, err = cluster.HandleCheckResourceState(&rpcpb.Request{
		CheckResourceState: rpcpb.CheckResourceStateReq{
			IDs: bm,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, ids[:3], rsp.Removed)
	assert.Equal(t, 3, len(rsp.Removed))

	// restart
	cluster = newTestRaftCluster(opt, cluster.storage, core.NewBasicCluster(metadata.TestResourceFactory, nil))
	cluster.LoadClusterInfo()
	rsp, err = cluster.HandleCheckResourceState(&rpcpb.Request{
		CheckResourceState: rpcpb.CheckResourceStateReq{
			IDs: bm,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, ids[:3], rsp.Removed)
	assert.Equal(t, 3, len(rsp.Removed))
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
