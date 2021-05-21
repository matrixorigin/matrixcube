package cluster

import (
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/event"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockhbstream"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	_ "github.com/matrixorigin/matrixcube/components/prophet/schedulers"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/pilosa/pilosa/roaring"
	"github.com/stretchr/testify/assert"
)

func TestReportSplit(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory))

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
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory))

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

func TestRemoveResources(t *testing.T) {
	_, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory))
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
	assert.Equal(t, uint64(1), cluster.core.RemovedResources.Count())
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
	assert.Equal(t, uint64(5), cluster.core.RemovedResources.Count())
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
	cluster = newTestRaftCluster(opt, storage, core.NewBasicCluster(metadata.TestResourceFactory))
	cluster.LoadClusterInfo()
	cache = cluster.core.Resources
	assert.Equal(t, uint64(len(removed)), cluster.core.RemovedResources.Count())
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
	cluster := newTestRaftCluster(opt, storage.NewTestStorage(), core.NewBasicCluster(metadata.TestResourceFactory))

	n, np := uint64(7), uint64(3)
	resources := newTestResources(n, np)
	// add 1,2,3,4,5,6
	for i := uint64(1); i < n; i++ {
		cluster.processResourceHeartbeat(resources[i])
	}

	ids := []uint64{1, 2, 3, 4, 5, 6}
	bm := util.MustMarshalBM64(roaring.NewBitmap(ids...))
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
	cluster = newTestRaftCluster(opt, cluster.storage, core.NewBasicCluster(metadata.TestResourceFactory))
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
