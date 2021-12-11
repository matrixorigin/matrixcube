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
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockhbstream"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/hbstream"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/schedulers"
	"github.com/matrixorigin/matrixcube/components/prophet/statistics"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/testutil"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func newTestOperator(resourceID uint64, resourceEpoch metapb.ResourceEpoch, kind operator.OpKind, steps ...operator.OpStep) *operator.Operator {
	return operator.NewOperator("test", "test", resourceID, resourceEpoch, kind, steps...)
}

func (c *testCluster) AllocPeer(containerID uint64) (metapb.Replica, error) {
	id, err := c.AllocID()
	if err != nil {
		return metapb.Replica{}, err
	}
	return metapb.Replica{ID: id, ContainerID: containerID}, nil
}

func (c *testCluster) addResourceContainer(containerID uint64, resourceCount int, resourceSizes ...uint64) error {
	var resourceSize uint64
	if len(resourceSizes) == 0 {
		resourceSize = uint64(resourceCount) * 10
	} else {
		resourceSize = resourceSizes[0]
	}

	stats := &metapb.ContainerStats{}
	stats.Capacity = 100 * (1 << 30)
	stats.UsedSize = resourceSize * (1 << 20)
	stats.Available = stats.Capacity - stats.UsedSize
	newContainer := core.NewCachedContainer(&metadata.TestContainer{CID: containerID},
		core.SetContainerStats(stats),
		core.SetResourceCount("", resourceCount),
		core.SetResourceSize("", int64(resourceSize)),
		core.SetLastHeartbeatTS(time.Now()),
	)

	c.SetContainerLimit(containerID, limit.AddPeer, 60)
	c.SetContainerLimit(containerID, limit.RemovePeer, 60)
	c.Lock()
	defer c.Unlock()
	return c.putContainerLocked(newContainer)
}

func (c *testCluster) addLeaderResource(resourceID uint64, leaderContainerID uint64, followerContainerIDs ...uint64) error {
	resource := newTestResourceMeta(resourceID)
	leader, _ := c.AllocPeer(leaderContainerID)
	resource.SetPeers([]metapb.Replica{leader})
	for _, followerContainerID := range followerContainerIDs {
		peer, _ := c.AllocPeer(followerContainerID)
		resource.SetPeers(append(resource.Peers(), peer))
	}
	resourceInfo := core.NewCachedResource(resource, &leader, core.SetApproximateSize(10), core.SetApproximateKeys(10))
	c.core.PutResource(resourceInfo)
	return nil
}

func (c *testCluster) updateLeaderCount(containerID uint64, leaderCount int) error {
	container := c.GetContainer(containerID)
	newContainer := container.Clone(
		core.SetLeaderCount("", leaderCount),
		core.SetLeaderSize("", int64(leaderCount)*10),
	)
	c.Lock()
	defer c.Unlock()
	return c.putContainerLocked(newContainer)
}

func (c *testCluster) addLeaderContainer(containerID uint64, leaderCount int) error {
	stats := &metapb.ContainerStats{}
	newContainer := core.NewCachedContainer(&metadata.TestContainer{CID: containerID},
		core.SetContainerStats(stats),
		core.SetLeaderCount("", leaderCount),
		core.SetLeaderSize("", int64(leaderCount)*10),
		core.SetLastHeartbeatTS(time.Now()),
	)

	c.SetContainerLimit(containerID, limit.AddPeer, 60)
	c.SetContainerLimit(containerID, limit.RemovePeer, 60)
	c.Lock()
	defer c.Unlock()
	return c.putContainerLocked(newContainer)
}

func (c *testCluster) setContainerDown(containerID uint64) error {
	container := c.GetContainer(containerID)
	newContainer := container.Clone(
		core.UpContainer(),
		core.SetLastHeartbeatTS(time.Time{}),
	)
	c.Lock()
	defer c.Unlock()
	return c.putContainerLocked(newContainer)
}

func (c *testCluster) setContainerOffline(containerID uint64) error {
	container := c.GetContainer(containerID)
	newContainer := container.Clone(core.OfflineContainer(false))
	c.Lock()
	defer c.Unlock()
	return c.putContainerLocked(newContainer)
}

func (c *testCluster) LoadResource(resourceID uint64, followerContainerIDs ...uint64) error {
	//  resources load from etcd will have no leader
	resource := newTestResourceMeta(resourceID)
	resource.SetPeers([]metapb.Replica{})
	for _, id := range followerContainerIDs {
		peer, _ := c.AllocPeer(id)
		resource.SetPeers(append(resource.Peers(), peer))
	}
	c.core.PutResource(core.NewCachedResource(resource, nil))
	return nil
}

func TestBasic(t *testing.T) {
	tc, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()
	oc := co.opController

	assert.Nil(t, tc.addLeaderResource(1, 1))

	op1 := newTestOperator(1, tc.GetResource(1).Meta.Epoch(), operator.OpLeader)
	oc.AddWaitingOperator(op1)
	assert.Equal(t, uint64(1), oc.OperatorCount(op1.Kind()))
	assert.Equal(t, op1.ResourceID(), oc.GetOperator(1).ResourceID())

	// resource 1 already has an operator, cannot add another one.
	op2 := newTestOperator(1, tc.GetResource(1).Meta.Epoch(), operator.OpResource)
	oc.AddWaitingOperator(op2)
	assert.Equal(t, uint64(0), oc.OperatorCount(op2.Kind()))

	// Remove the operator manually, then we can add a new operator.
	assert.True(t, oc.RemoveOperator(op1, ""))
	op3 := newTestOperator(1, tc.GetResource(1).Meta.Epoch(), operator.OpResource)
	oc.AddWaitingOperator(op3)
	assert.Equal(t, uint64(1), oc.OperatorCount(op3.Kind()))
	assert.Equal(t, op3.ResourceID(), oc.GetOperator(1).ResourceID())
}

func TestDispatch(t *testing.T) {
	tc, co, cleanup := prepare(t, nil, func(tc *testCluster) { tc.prepareChecker.isPrepared = true }, nil)
	defer cleanup()
	tc.DisableJointConsensus()
	// Transfer peer from container 4 to container 1.
	assert.Nil(t, tc.addResourceContainer(4, 40))
	assert.Nil(t, tc.addResourceContainer(3, 30))
	assert.Nil(t, tc.addResourceContainer(2, 20))
	assert.Nil(t, tc.addResourceContainer(1, 10))
	assert.Nil(t, tc.addLeaderResource(1, 2, 3, 4))

	// Transfer leader from container 4 to container 2.
	assert.Nil(t, tc.updateLeaderCount(4, 50))
	assert.Nil(t, tc.updateLeaderCount(3, 50))
	assert.Nil(t, tc.updateLeaderCount(2, 20))
	assert.Nil(t, tc.updateLeaderCount(1, 10))
	assert.Nil(t, tc.addLeaderResource(2, 4, 3, 2))

	co.run()

	// Wait for schedule and turn off balance.
	waitOperator(t, co, 1)
	testutil.CheckTransferPeer(t, co.opController.GetOperator(1), operator.OpKind(0), 4, 1)
	assert.Nil(t, co.removeScheduler(schedulers.BalanceResourceName))
	waitOperator(t, co, 2)
	testutil.CheckTransferLeader(t, co.opController.GetOperator(2), operator.OpKind(0), 4, 2)
	assert.Nil(t, co.removeScheduler(schedulers.BalanceLeaderName))

	stream := mockhbstream.NewHeartbeatStream()

	// Transfer peer.
	resource := tc.GetResource(1).Clone()
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	resource = waitAddLearner(t, stream, resource, 1)
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	resource = waitPromoteLearner(t, stream, resource, 1)
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	resource = waitRemovePeer(t, stream, resource, 4)
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	waitNoResponse(t, stream)

	// Transfer leader.
	resource = tc.GetResource(2).Clone()
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	waitTransferLeader(t, stream, resource, 2)
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	waitNoResponse(t, stream)
}

func TestCollectMetrics(t *testing.T) {
	tc, co, cleanup := prepare(t, nil, func(tc *testCluster) {
		tc.resourceStats = statistics.NewResourceStatistics(tc.GetOpts(), nil)
	}, func(co *coordinator) { co.run() })
	defer cleanup()

	// Make sure there are no problem when concurrent write and read
	var wg sync.WaitGroup
	count := 10
	wg.Add(count + 1)
	for i := 0; i <= count; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				assert.Nil(t, tc.addResourceContainer(uint64(i%5), rand.Intn(200)))
			}
		}(i)
	}
	for i := 0; i < 1000; i++ {
		co.collectHotSpotMetrics()
		co.collectSchedulerMetrics()
		co.cluster.collectClusterMetrics()
	}
	co.resetHotSpotMetrics()
	co.resetSchedulerMetrics()
	co.cluster.resetClusterMetrics()
	wg.Wait()
}

func TestCheckResource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc, co, cleanup := prepare(t, nil, nil, func(co *coordinator) { co.run() })
	hbStreams, opt := co.hbStreams, tc.opt
	defer cleanup()

	assert.Nil(t, tc.addResourceContainer(4, 4))
	assert.Nil(t, tc.addResourceContainer(3, 3))
	assert.Nil(t, tc.addResourceContainer(2, 2))
	assert.Nil(t, tc.addResourceContainer(1, 1))
	assert.Nil(t, tc.addLeaderResource(1, 2, 3))
	checkCOResource(t, tc, co, 1, false, 1)
	waitOperator(t, co, 1)
	testutil.CheckAddPeer(t, co.opController.GetOperator(1), operator.OpReplica, 1)
	checkCOResource(t, tc, co, 1, false, 0)

	r := tc.GetResource(1)
	p := metapb.Replica{ID: 1, ContainerID: 1, Role: metapb.ReplicaRole_Learner}
	r = r.Clone(
		core.WithAddPeer(p),
		core.WithPendingPeers(append(r.GetPendingPeers(), p)),
	)
	tc.core.PutResource(r)
	checkCOResource(t, tc, co, 1, false, 0)
	co.stop()
	co.wg.Wait()

	tc = newTestCluster(opt)
	co = newCoordinator(ctx, tc.RaftCluster, hbStreams)
	co.run()

	assert.Nil(t, tc.addResourceContainer(4, 4))
	assert.Nil(t, tc.addResourceContainer(3, 3))
	assert.Nil(t, tc.addResourceContainer(2, 2))
	assert.Nil(t, tc.addResourceContainer(1, 1))
	tc.core.PutResource(r)
	checkCOResource(t, tc, co, 1, false, 0)
	r = r.Clone(core.WithPendingPeers(nil))
	tc.core.PutResource(r)
	checkCOResource(t, tc, co, 1, false, 1)
	waitOperator(t, co, 1)
	op := co.opController.GetOperator(1)
	assert.Equal(t, 1, op.Len())
	assert.Equal(t, uint64(1), op.Step(0).(operator.PromoteLearner).ToContainer)
	checkCOResource(t, tc, co, 1, false, 0)
}

func TestCheckerIsBusy(t *testing.T) {
	tc, co, cleanup := prepare(t, func(cfg *config.ScheduleConfig) {
		cfg.ReplicaScheduleLimit = 0 // ensure replica checker is busy
		cfg.MergeScheduleLimit = 10
	}, nil, func(co *coordinator) { co.run() })
	defer cleanup()

	assert.Nil(t, tc.addResourceContainer(1, 0))
	num := 1 + typeutil.MaxUint64(tc.opt.GetReplicaScheduleLimit(), tc.opt.GetMergeScheduleLimit())
	var operatorKinds = []operator.OpKind{
		operator.OpReplica, operator.OpResource | operator.OpMerge,
	}
	for i, operatorKind := range operatorKinds {
		for j := uint64(0); j < num; j++ {
			resourceID := j + uint64(i+1)*num
			assert.Nil(t, tc.addLeaderResource(resourceID, 1))
			switch operatorKind {
			case operator.OpReplica:
				op := newTestOperator(resourceID, tc.GetResource(resourceID).Meta.Epoch(), operatorKind)
				assert.Equal(t, 1, co.opController.AddWaitingOperator(op))
			case operator.OpResource | operator.OpMerge:
				if resourceID%2 == 1 {
					ops, err := operator.CreateMergeResourceOperator("merge-resource", co.cluster, tc.GetResource(resourceID), tc.GetResource(resourceID-1), operator.OpMerge)
					assert.NoError(t, err)
					assert.Equal(t, len(ops), co.opController.AddWaitingOperator(ops...))
				}
			}

		}
	}
	checkCOResource(t, tc, co, num, true, 0)
}

func TestReplica(t *testing.T) {
	tc, co, cleanup := prepare(t, func(cfg *config.ScheduleConfig) {
		// Turn off balance.
		cfg.LeaderScheduleLimit = 0
		cfg.ResourceScheduleLimit = 0
	}, nil, func(co *coordinator) { co.run() })
	defer cleanup()

	assert.Nil(t, tc.addResourceContainer(1, 1))
	assert.Nil(t, tc.addResourceContainer(2, 2))
	assert.Nil(t, tc.addResourceContainer(3, 3))
	assert.Nil(t, tc.addResourceContainer(4, 4))

	stream := mockhbstream.NewHeartbeatStream()

	// Add peer to container 1.
	assert.Nil(t, tc.addLeaderResource(1, 2, 3))
	resource := tc.GetResource(1)
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	resource = waitAddLearner(t, stream, resource, 1)
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	resource = waitPromoteLearner(t, stream, resource, 1)
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	waitNoResponse(t, stream)

	// Peer in container 3 is down, remove peer in container 3 and add peer to container 4.
	assert.Nil(t, tc.setContainerDown(3))
	p, _ := resource.GetContainerPeer(3)
	downPeer := metapb.ReplicaStats{
		Replica:     p,
		DownSeconds: 24 * 60 * 60,
	}
	resource = resource.Clone(
		core.WithDownPeers(append(resource.GetDownPeers(), downPeer)),
	)
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	resource = waitAddLearner(t, stream, resource, 4)
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	resource = waitPromoteLearner(t, stream, resource, 4)
	resource = resource.Clone(core.WithDownPeers(nil))
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	waitNoResponse(t, stream)

	// Remove peer from container 4.
	assert.Nil(t, tc.addLeaderResource(2, 1, 2, 3, 4))
	resource = tc.GetResource(2)
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	resource = waitRemovePeer(t, stream, resource, 4)
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	waitNoResponse(t, stream)

	// Remove offline peer directly when it's pending.
	assert.Nil(t, tc.addLeaderResource(3, 1, 2, 3))
	assert.Nil(t, tc.setContainerOffline(3))
	resource = tc.GetResource(3)
	p, _ = resource.GetContainerPeer(3)
	resource = resource.Clone(core.WithPendingPeers([]metapb.Replica{p}))
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	waitNoResponse(t, stream)
}

func TestPeerState(t *testing.T) {
	tc, co, cleanup := prepare(t, nil, nil, func(co *coordinator) { co.run() })
	defer cleanup()

	// Transfer peer from container 4 to container 1.
	assert.Nil(t, tc.addResourceContainer(1, 10))
	assert.Nil(t, tc.addResourceContainer(2, 10))
	assert.Nil(t, tc.addResourceContainer(3, 10))
	assert.Nil(t, tc.addResourceContainer(4, 40))
	assert.Nil(t, tc.addLeaderResource(1, 2, 3, 4))

	stream := mockhbstream.NewHeartbeatStream()

	// Wait for schedule.
	waitOperator(t, co, 1)
	testutil.CheckTransferPeer(t, co.opController.GetOperator(1), operator.OpKind(0), 4, 1)

	resource := tc.GetResource(1).Clone()

	// Add new peer.
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	resource = waitAddLearner(t, stream, resource, 1)
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	resource = waitPromoteLearner(t, stream, resource, 1)

	// If the new peer is pending, the operator will not finish.
	p, _ := resource.GetContainerPeer(1)
	resource = resource.Clone(core.WithPendingPeers(append(resource.GetPendingPeers(), p)))
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	waitNoResponse(t, stream)
	assert.NotNil(t, co.opController.GetOperator(resource.Meta.ID()))

	// The new peer is not pending now, the operator will finish.
	// And we will proceed to remove peer in container 4.
	resource = resource.Clone(core.WithPendingPeers(nil))
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	waitRemovePeer(t, stream, resource, 4)
	assert.Nil(t, tc.addLeaderResource(1, 1, 2, 3))
	resource = tc.GetResource(1).Clone()
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	waitNoResponse(t, stream)
}

func TestShouldRun(t *testing.T) {
	tc, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()

	assert.Nil(t, tc.addLeaderContainer(1, 5))
	assert.Nil(t, tc.addLeaderContainer(2, 2))
	assert.Nil(t, tc.addLeaderContainer(3, 0))
	assert.Nil(t, tc.addLeaderContainer(4, 0))
	assert.Nil(t, tc.LoadResource(1, 1, 2, 3))
	assert.Nil(t, tc.LoadResource(2, 1, 2, 3))
	assert.Nil(t, tc.LoadResource(3, 1, 2, 3))
	assert.Nil(t, tc.LoadResource(4, 1, 2, 3))
	assert.Nil(t, tc.LoadResource(5, 1, 2, 3))
	assert.Nil(t, tc.LoadResource(6, 2, 1, 4))
	assert.Nil(t, tc.LoadResource(7, 2, 1, 4))
	assert.False(t, co.shouldRun())
	assert.Equal(t, 2, tc.core.Resources.GetContainerResourceCount("", 4))

	tbl := []struct {
		resourceID uint64
		shouldRun  bool
	}{
		{1, false},
		{2, false},
		{3, false},
		{4, false},
		{5, false},
		// container4 needs collect two resource
		{6, false},
		{7, true},
	}

	for _, tb := range tbl {
		r := tc.GetResource(tb.resourceID)
		nr := r.Clone(core.WithLeader(&r.Meta.Peers()[0]))
		assert.Nil(t, tc.processResourceHeartbeat(nr))
		assert.Equal(t, tb.shouldRun, co.shouldRun())
	}
	nr := &metadata.TestResource{ResID: 6, ResPeers: []metapb.Replica{}}
	newResource := core.NewCachedResource(nr, nil)
	assert.NotNil(t, tc.processResourceHeartbeat(newResource))
	assert.Equal(t, 7, co.cluster.prepareChecker.sum)
}

func TestShouldRunWithNonLeaderResources(t *testing.T) {
	tc, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()

	assert.Nil(t, tc.addLeaderContainer(1, 10))
	assert.Nil(t, tc.addLeaderContainer(2, 0))
	assert.Nil(t, tc.addLeaderContainer(3, 0))
	for i := 0; i < 10; i++ {
		assert.Nil(t, tc.LoadResource(uint64(i+1), 1, 2, 3))
	}
	assert.False(t, co.shouldRun())
	assert.Equal(t, 10, tc.core.Resources.GetContainerResourceCount("", 1))

	tbl := []struct {
		resourceID uint64
		shouldRun  bool
	}{
		{1, false},
		{2, false},
		{3, false},
		{4, false},
		{5, false},
		{6, false},
		{7, false},
		{8, true},
	}

	for _, tb := range tbl {
		r := tc.GetResource(tb.resourceID)
		nr := r.Clone(core.WithLeader(&r.Meta.Peers()[0]))
		assert.Nil(t, tc.processResourceHeartbeat(nr))
		assert.Equal(t, tb.shouldRun, co.shouldRun())
	}
	nr := &metadata.TestResource{ResID: 8, ResPeers: []metapb.Replica{}}
	newResource := core.NewCachedResource(nr, nil)
	assert.NotNil(t, tc.processResourceHeartbeat(newResource))
	assert.Equal(t, 8, co.cluster.prepareChecker.sum)

	// Now, after server is prepared, there exist some resources with no leader.
	assert.Equal(t, uint64(0), tc.GetResource(9).GetLeader().GetContainerID())
	assert.Equal(t, uint64(0), tc.GetResource(10).GetLeader().GetContainerID())
}

func TestAddScheduler(t *testing.T) {
	tc, co, cleanup := prepare(t, nil, nil, func(co *coordinator) { co.run() })
	defer cleanup()

	assert.Equal(t, 4, len(co.schedulers))
	assert.Nil(t, co.removeScheduler(schedulers.BalanceLeaderName))
	assert.Nil(t, co.removeScheduler(schedulers.BalanceResourceName))
	assert.Nil(t, co.removeScheduler(schedulers.HotResourceName))
	assert.Nil(t, co.removeScheduler(schedulers.LabelName))
	assert.Empty(t, co.schedulers)

	stream := mockhbstream.NewHeartbeatStream()

	// Add containers 1,2,3
	assert.Nil(t, tc.addLeaderContainer(1, 1))
	assert.Nil(t, tc.addLeaderContainer(2, 1))
	assert.Nil(t, tc.addLeaderContainer(3, 1))
	// Add resources 1 with leader in container 1 and followers in containers 2,3
	assert.Nil(t, tc.addLeaderResource(1, 1, 2, 3))
	// Add resources 2 with leader in container 2 and followers in containers 1,3
	assert.Nil(t, tc.addLeaderResource(2, 2, 1, 3))
	// Add resources 3 with leader in container 3 and followers in containers 1,2
	assert.Nil(t, tc.addLeaderResource(3, 3, 1, 2))

	oc := co.opController
	gls, err := schedule.CreateScheduler(schedulers.GrantLeaderType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(schedulers.GrantLeaderType, []string{"0"}))
	assert.Nil(t, err)
	assert.NotNil(t, co.addScheduler(gls))
	assert.NotNil(t, co.removeScheduler(gls.GetName()))

	gls, err = schedule.CreateScheduler(schedulers.GrantLeaderType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(schedulers.GrantLeaderType, []string{"1"}))
	assert.Nil(t, err)
	assert.Nil(t, co.addScheduler(gls))

	// Transfer all leaders to container 1.
	waitOperator(t, co, 2)
	resource2 := tc.GetResource(2)
	assert.Nil(t, dispatchHeartbeat(co, resource2, stream))
	resource2 = waitTransferLeader(t, stream, resource2, 1)
	assert.Nil(t, dispatchHeartbeat(co, resource2, stream))
	waitNoResponse(t, stream)

	waitOperator(t, co, 3)
	resource3 := tc.GetResource(3)
	assert.Nil(t, dispatchHeartbeat(co, resource3, stream))
	resource3 = waitTransferLeader(t, stream, resource3, 1)
	assert.Nil(t, dispatchHeartbeat(co, resource3, stream))
	waitNoResponse(t, stream)
}

func TestRemoveScheduler(t *testing.T) {
	tc, co, cleanup := prepare(t, func(cfg *config.ScheduleConfig) {
		cfg.ReplicaScheduleLimit = 0
	}, nil, func(co *coordinator) { co.run() })
	defer cleanup()

	// Add containers 1,2
	assert.Nil(t, tc.addLeaderContainer(1, 1))
	assert.Nil(t, tc.addLeaderContainer(2, 1))

	assert.Equal(t, 4, len(co.schedulers))
	oc := co.opController
	storage := tc.RaftCluster.storage

	gls1, err := schedule.CreateScheduler(schedulers.GrantLeaderType, oc, storage, schedule.ConfigSliceDecoder(schedulers.GrantLeaderType, []string{"1"}))
	assert.Nil(t, err)
	assert.Nil(t, co.addScheduler(gls1, "1"))
	assert.Equal(t, 5, len(co.schedulers))
	sches, _, err := storage.LoadAllScheduleConfig()
	assert.Nil(t, err)
	assert.Equal(t, 5, len(sches))

	// remove all schedulers
	assert.Nil(t, co.removeScheduler(schedulers.BalanceLeaderName))
	assert.Nil(t, co.removeScheduler(schedulers.BalanceResourceName))
	assert.Nil(t, co.removeScheduler(schedulers.HotResourceName))
	assert.Nil(t, co.removeScheduler(schedulers.LabelName))
	assert.Nil(t, co.removeScheduler(schedulers.GrantLeaderName))
	// all removed
	sches, _, err = storage.LoadAllScheduleConfig()
	assert.Nil(t, err)
	assert.Empty(t, sches)
	assert.Empty(t, co.schedulers)
	assert.Nil(t, co.cluster.opt.Persist(co.cluster.storage))
	co.stop()
	co.wg.Wait()
}

func TestRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc, co, cleanup := prepare(t, func(cfg *config.ScheduleConfig) {
		// Turn off balance, we test add replica only.
		cfg.LeaderScheduleLimit = 0
		cfg.ResourceScheduleLimit = 0
	}, nil, func(co *coordinator) { co.run() })
	hbStreams := co.hbStreams
	defer cleanup()

	// Add 3 containers (1, 2, 3) and a resource with 1 replica on container 1.
	assert.Nil(t, tc.addResourceContainer(1, 1))
	assert.Nil(t, tc.addResourceContainer(2, 2))
	assert.Nil(t, tc.addResourceContainer(3, 3))
	assert.Nil(t, tc.addLeaderResource(1, 1))
	resource := tc.GetResource(1)
	tc.prepareChecker.collect(resource)

	// Add 1 replica on container 2.
	co = newCoordinator(ctx, tc.RaftCluster, hbStreams)
	co.run()
	stream := mockhbstream.NewHeartbeatStream()
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	resource = waitAddLearner(t, stream, resource, 2)
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	resource = waitPromoteLearner(t, stream, resource, 2)
	co.stop()
	co.wg.Wait()

	// Recreate coordinator then add another replica on container 3.
	co = newCoordinator(ctx, tc.RaftCluster, hbStreams)
	co.run()
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	resource = waitAddLearner(t, stream, resource, 3)
	assert.Nil(t, dispatchHeartbeat(co, resource, stream))
	waitPromoteLearner(t, stream, resource, 3)
}

func dispatchHeartbeat(co *coordinator, resource *core.CachedResource, stream opt.HeartbeatStream) error {
	co.hbStreams.BindStream(resource.GetLeader().GetContainerID(), stream)
	co.cluster.core.PutResource(resource.Clone())
	co.opController.Dispatch(resource, schedule.DispatchFromHeartBeat)
	return nil
}

func prepare(t *testing.T, setCfg func(*config.ScheduleConfig), setTc func(*testCluster), run func(*coordinator)) (*testCluster, *coordinator, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	if setCfg != nil {
		setCfg(cfg)
	}
	tc := newTestCluster(opt)
	hbStreams := hbstream.NewTestHeartbeatStreams(ctx, 0, tc, true /* need to run */, nil)
	if setTc != nil {
		setTc(tc)
	}
	co := newCoordinator(ctx, tc.RaftCluster, hbStreams)
	if run != nil {
		run(co)
	}
	return tc, co, func() {
		co.stop()
		co.wg.Wait()
		hbStreams.Close()
		cancel()
	}
}

func checkCOResource(t *testing.T, tc *testCluster, co *coordinator, resourceID uint64, expectCheckerIsBusy bool, expectAddOperator int) {
	ops := co.checkers.CheckResource(tc.GetResource(resourceID))
	if ops == nil {
		assert.Equal(t, 0, expectAddOperator)
	} else {
		assert.Equal(t, expectAddOperator, co.opController.AddWaitingOperator(ops...))
	}
}

func waitOperator(t *testing.T, co *coordinator, resourceID uint64) {
	testutil.WaitUntil(t, func(t *testing.T) bool {
		return co.opController.GetOperator(resourceID) != nil
	})
}

type testOperatorController struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testOperatorController) setup(t *testing.T) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testOperatorController) tearDown() {
	s.cancel()
}

func TestOperatorCount(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	tc, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()
	oc := co.opController
	assert.Equal(t, uint64(0), oc.OperatorCount(operator.OpLeader))
	assert.Equal(t, uint64(0), oc.OperatorCount(operator.OpResource))

	assert.Nil(t, tc.addLeaderResource(1, 1))
	assert.Nil(t, tc.addLeaderResource(2, 2))
	{
		op1 := newTestOperator(1, tc.GetResource(1).Meta.Epoch(), operator.OpLeader)
		oc.AddWaitingOperator(op1)
		assert.Equal(t, oc.OperatorCount(operator.OpLeader), uint64(1)) // 1:leader
		op2 := newTestOperator(2, tc.GetResource(2).Meta.Epoch(), operator.OpLeader)
		oc.AddWaitingOperator(op2)
		assert.Equal(t, oc.OperatorCount(operator.OpLeader), uint64(2)) // 1:leader, 2:leader
		assert.True(t, oc.RemoveOperator(op1, ""))
		assert.Equal(t, oc.OperatorCount(operator.OpLeader), uint64(1)) // 2:leader
	}

	{
		op1 := newTestOperator(1, tc.GetResource(1).Meta.Epoch(), operator.OpResource)
		oc.AddWaitingOperator(op1)
		assert.Equal(t, oc.OperatorCount(operator.OpResource), uint64(1)) // 1:resource 2:leader
		assert.Equal(t, oc.OperatorCount(operator.OpLeader), uint64(1))
		op2 := newTestOperator(2, tc.GetResource(2).Meta.Epoch(), operator.OpResource)
		op2.SetPriorityLevel(core.HighPriority)
		oc.AddWaitingOperator(op2)
		assert.Equal(t, oc.OperatorCount(operator.OpResource), uint64(2)) // 1:resource 2:resource
		assert.Equal(t, oc.OperatorCount(operator.OpLeader), uint64(0))
	}
}

func TestContainerOverloaded(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	tc, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()
	oc := co.opController
	lb, err := schedule.CreateScheduler(schedulers.BalanceResourceType, oc, tc.storage, schedule.ConfigSliceDecoder(schedulers.BalanceResourceType, []string{"0", "", ""}))
	assert.NoError(t, err)
	opt := tc.GetOpts()
	assert.Nil(t, tc.addResourceContainer(4, 100))
	assert.Nil(t, tc.addResourceContainer(3, 100))
	assert.Nil(t, tc.addResourceContainer(2, 100))
	assert.Nil(t, tc.addResourceContainer(1, 10))
	assert.Nil(t, tc.addLeaderResource(1, 2, 3, 4))
	resource := tc.GetResource(1).Clone(core.SetApproximateSize(60))
	tc.core.PutResource(resource)
	start := time.Now()
	{
		op1 := lb.Schedule(tc)[0]
		assert.NotNil(t, op1)
		assert.True(t, oc.AddOperator(op1))
		assert.True(t, oc.RemoveOperator(op1, ""))
	}
	for {
		time.Sleep(time.Millisecond * 10)
		ops := lb.Schedule(tc)
		if time.Since(start) > time.Second {
			break
		}
		assert.Nil(t, ops)
	}

	// reset all containers' limit
	// scheduling one time needs 1/10 seconds
	opt.SetAllContainersLimit(limit.AddPeer, 600)
	opt.SetAllContainersLimit(limit.RemovePeer, 600)
	for i := 0; i < 10; i++ {
		op1 := lb.Schedule(tc)[0]
		assert.NotNil(t, op1)
		assert.True(t, oc.AddOperator(op1))
		assert.True(t, oc.RemoveOperator(op1, ""))
	}
	// sleep 1 seconds to make sure that the token is filled up
	time.Sleep(1 * time.Second)
	for i := 0; i < 100; i++ {
		assert.NotNil(t, lb.Schedule(tc))
	}
}

func TestContainerOverloadedWithReplace(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	tc, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()
	oc := co.opController
	lb, err := schedule.CreateScheduler(schedulers.BalanceResourceType, oc, tc.storage, schedule.ConfigSliceDecoder(schedulers.BalanceResourceType, []string{"0", "", ""}))
	assert.NoError(t, err)

	assert.Nil(t, tc.addResourceContainer(4, 100))
	assert.Nil(t, tc.addResourceContainer(3, 100))
	assert.Nil(t, tc.addResourceContainer(2, 100))
	assert.Nil(t, tc.addResourceContainer(1, 10))
	assert.Nil(t, tc.addLeaderResource(1, 2, 3, 4))
	assert.Nil(t, tc.addLeaderResource(2, 1, 3, 4))
	resource := tc.GetResource(1).Clone(core.SetApproximateSize(60))
	tc.core.PutResource(resource)
	resource = tc.GetResource(2).Clone(core.SetApproximateSize(60))
	tc.core.PutResource(resource)
	op1 := newTestOperator(1, tc.GetResource(1).Meta.Epoch(), operator.OpResource, operator.AddPeer{ToContainer: 1, PeerID: 1})
	assert.True(t, oc.AddOperator(op1))
	op2 := newTestOperator(1, tc.GetResource(1).Meta.Epoch(), operator.OpResource, operator.AddPeer{ToContainer: 2, PeerID: 2})
	op2.SetPriorityLevel(core.HighPriority)
	assert.True(t, oc.AddOperator(op2))
	op3 := newTestOperator(1, tc.GetResource(2).Meta.Epoch(), operator.OpResource, operator.AddPeer{ToContainer: 1, PeerID: 3})
	assert.False(t, oc.AddOperator(op3))
	assert.Nil(t, lb.Schedule(tc))
	// sleep 2 seconds to make sure that token is filled up
	time.Sleep(2 * time.Second)
	assert.NotNil(t, lb.Schedule(tc))
}

// FIXME: remove after move into schedulers package
type mockLimitScheduler struct {
	schedule.Scheduler
	limit   uint64
	counter *schedule.OperatorController
	kind    operator.OpKind
}

func (s *mockLimitScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.counter.OperatorCount(s.kind) < s.limit
}

func TestController(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	tc, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()
	oc := co.opController

	assert.Nil(t, tc.addLeaderResource(1, 1))
	assert.Nil(t, tc.addLeaderResource(2, 2))
	scheduler, err := schedule.CreateScheduler(schedulers.BalanceLeaderType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(schedulers.BalanceLeaderType, []string{"0", "", ""}))
	assert.NoError(t, err)
	lb := &mockLimitScheduler{
		Scheduler: scheduler,
		counter:   oc,
		kind:      operator.OpLeader,
	}

	sc := newScheduleController(co, lb)

	for i := schedulers.MinScheduleInterval; sc.GetInterval() != schedulers.MaxScheduleInterval; i = sc.GetNextInterval(i) {
		assert.Equal(t, i, sc.GetInterval())
		assert.Nil(t, sc.Schedule())
	}
	// limit = 2
	lb.limit = 2
	// count = 0
	{
		assert.True(t, sc.AllowSchedule())
		op1 := newTestOperator(1, tc.GetResource(1).Meta.Epoch(), operator.OpLeader)
		assert.Equal(t, 1, oc.AddWaitingOperator(op1))
		// count = 1
		assert.True(t, sc.AllowSchedule())
		op2 := newTestOperator(2, tc.GetResource(2).Meta.Epoch(), operator.OpLeader)
		assert.Equal(t, 1, oc.AddWaitingOperator(op2))
		// count = 2
		assert.False(t, sc.AllowSchedule())
		assert.True(t, oc.RemoveOperator(op1, ""))
		// count = 1
		assert.True(t, sc.AllowSchedule())
	}

	op11 := newTestOperator(1, tc.GetResource(1).Meta.Epoch(), operator.OpLeader)
	// add a PriorityKind operator will remove old operator
	{
		op3 := newTestOperator(2, tc.GetResource(2).Meta.Epoch(), operator.OpHotResource)
		op3.SetPriorityLevel(core.HighPriority)
		assert.Equal(t, 1, oc.AddWaitingOperator(op11))
		assert.False(t, sc.AllowSchedule())
		assert.Equal(t, 1, oc.AddWaitingOperator(op3))
		assert.True(t, sc.AllowSchedule())
		assert.True(t, oc.RemoveOperator(op3, ""))
	}

	// add a admin operator will remove old operator
	{
		op2 := newTestOperator(2, tc.GetResource(2).Meta.Epoch(), operator.OpLeader)
		assert.Equal(t, 1, oc.AddWaitingOperator(op2))
		assert.False(t, sc.AllowSchedule())
		op4 := newTestOperator(2, tc.GetResource(2).Meta.Epoch(), operator.OpAdmin)
		op4.SetPriorityLevel(core.HighPriority)
		assert.Equal(t, 1, oc.AddWaitingOperator(op4))
		assert.True(t, sc.AllowSchedule())
		assert.True(t, oc.RemoveOperator(op4, ""))
	}

	// test wrong resource id.
	{
		op5 := newTestOperator(3, metapb.ResourceEpoch{}, operator.OpHotResource)
		assert.Equal(t, 0, oc.AddWaitingOperator(op5))
	}

	// test wrong resource epoch.
	assert.True(t, oc.RemoveOperator(op11, ""))
	epoch := metapb.ResourceEpoch{
		Version: tc.GetResource(1).Meta.Epoch().Version + 1,
		ConfVer: tc.GetResource(1).Meta.Epoch().ConfVer,
	}
	{
		op6 := newTestOperator(1, epoch, operator.OpLeader)
		assert.Equal(t, 0, oc.AddWaitingOperator(op6))
	}
	epoch.Version--
	{
		op6 := newTestOperator(1, epoch, operator.OpLeader)
		assert.Equal(t, 1, oc.AddWaitingOperator(op6))
		assert.True(t, oc.RemoveOperator(op6, ""))
	}
}

func TestInterval(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	_, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()

	lb, err := schedule.CreateScheduler(schedulers.BalanceLeaderType, co.opController, storage.NewTestStorage(), schedule.ConfigSliceDecoder(schedulers.BalanceLeaderType, []string{"0", "", ""}))
	assert.NoError(t, err)
	sc := newScheduleController(co, lb)

	// If no operator for x seconds, the next check should be in x/2 seconds.
	idleSeconds := []int{5, 10, 20, 30, 60}
	for _, n := range idleSeconds {
		sc.nextInterval = schedulers.MinScheduleInterval
		for totalSleep := time.Duration(0); totalSleep <= time.Second*time.Duration(n); totalSleep += sc.GetInterval() {
			assert.Nil(t, sc.Schedule())
		}
		assert.True(t, sc.GetInterval() < time.Second*time.Duration(n/2))
	}
}

func waitAddLearner(t *testing.T, stream mockhbstream.HeartbeatStream, resource *core.CachedResource, containerID uint64) *core.CachedResource {
	var res *rpcpb.ResourceHeartbeatRsp
	testutil.WaitUntil(t, func(t *testing.T) bool {
		if res = stream.Recv(); res != nil {
			return res.GetResourceID() == resource.Meta.ID() &&
				res.GetConfigChange().GetChangeType() == metapb.ConfigChangeType_AddLearnerNode &&
				res.GetConfigChange().GetReplica().ContainerID == containerID
		}
		return false
	})
	return resource.Clone(
		core.WithAddPeer(res.GetConfigChange().GetReplica()),
		core.WithIncConfVer(),
	)
}

func waitPromoteLearner(t *testing.T, stream mockhbstream.HeartbeatStream, resource *core.CachedResource, containerID uint64) *core.CachedResource {
	var res *rpcpb.ResourceHeartbeatRsp
	testutil.WaitUntil(t, func(t *testing.T) bool {
		if res = stream.Recv(); res != nil {
			return res.GetResourceID() == resource.Meta.ID() &&
				res.GetConfigChange().GetChangeType() == metapb.ConfigChangeType_AddNode &&
				res.GetConfigChange().GetReplica().ContainerID == containerID
		}
		return false
	})
	// Remove learner than add voter.
	return resource.Clone(
		core.WithRemoveContainerPeer(containerID),
		core.WithAddPeer(res.GetConfigChange().GetReplica()),
	)
}

func waitRemovePeer(t *testing.T, stream mockhbstream.HeartbeatStream, resource *core.CachedResource, containerID uint64) *core.CachedResource {
	var res *rpcpb.ResourceHeartbeatRsp
	testutil.WaitUntil(t, func(t *testing.T) bool {
		if res = stream.Recv(); res != nil {
			return res.GetResourceID() == resource.Meta.ID() &&
				res.GetConfigChange().GetChangeType() == metapb.ConfigChangeType_RemoveNode &&
				res.GetConfigChange().GetReplica().ContainerID == containerID
		}
		return false
	})
	return resource.Clone(
		core.WithRemoveContainerPeer(containerID),
		core.WithIncConfVer(),
	)
}

func waitTransferLeader(t *testing.T, stream mockhbstream.HeartbeatStream, resource *core.CachedResource, containerID uint64) *core.CachedResource {
	var res *rpcpb.ResourceHeartbeatRsp
	testutil.WaitUntil(t, func(t *testing.T) bool {
		if res = stream.Recv(); res != nil {
			return res.GetResourceID() == resource.Meta.ID() && res.GetTransferLeader().GetReplica().ContainerID == containerID
		}
		return false
	})

	p := res.GetTransferLeader().GetReplica()
	return resource.Clone(
		core.WithLeader(&p),
	)
}

func waitNoResponse(t *testing.T, stream mockhbstream.HeartbeatStream) {
	testutil.WaitUntil(t, func(t *testing.T) bool {
		res := stream.Recv()
		return res == nil
	})
}
