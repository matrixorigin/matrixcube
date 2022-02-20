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

package schedulers

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/statistics"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/testutil"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func init() {
	schedulePeerPr = 1.0
}

func TestGCPendingOpInfos(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "host"})
	for id := uint64(1); id <= 10; id++ {
		tc.PutContainerWithLabels(id)
	}

	sche, err := schedule.CreateScheduler(HotResourceType, schedule.NewOperatorController(ctx, tc, nil), storage.NewTestStorage(), schedule.ConfigJSONDecoder([]byte("null")))
	assert.NoError(t, err)
	hb := sche.(*hotScheduler)

	nilOp := func(resource *core.CachedResource, ty opType) *operator.Operator {
		return nil
	}
	notDoneOp := func(resource *core.CachedResource, ty opType) *operator.Operator {
		var op *operator.Operator
		var err error
		switch ty {
		case movePeer:
			op, err = operator.CreateMovePeerOperator("move-peer-test", tc, resource, operator.OpAdmin, 2, metapb.Replica{ID: resource.Meta.ID()*10000 + 1, ContainerID: 4})
		case transferLeader:
			op, err = operator.CreateTransferLeaderOperator("transfer-leader-test", tc, resource, 1, 2, operator.OpAdmin)
		}
		assert.NoError(t, err)
		assert.NotNil(t, op)
		return op
	}
	doneOp := func(resource *core.CachedResource, ty opType) *operator.Operator {
		op := notDoneOp(resource, ty)
		op.Cancel()
		return op
	}
	shouldRemoveOp := func(resource *core.CachedResource, ty opType) *operator.Operator {
		op := doneOp(resource, ty)
		operator.SetOperatorStatusReachTime(op, operator.CREATED, time.Now().Add(-3*statistics.ContainerHeartBeatReportInterval*time.Second))
		return op
	}
	opCreaters := [4]func(resource *core.CachedResource, ty opType) *operator.Operator{nilOp, shouldRemoveOp, notDoneOp, doneOp}

	for i := 0; i < len(opCreaters); i++ {
		for j := 0; j < len(opCreaters); j++ {
			ResourceID := uint64(i*len(opCreaters) + j + 1)
			resource := newTestresource(ResourceID)
			hb.resourcePendings[ResourceID] = [2]*operator.Operator{
				movePeer:       opCreaters[i](resource, movePeer),
				transferLeader: opCreaters[j](resource, transferLeader),
			}
		}
	}

	hb.gcResourcePendings()

	for i := 0; i < len(opCreaters); i++ {
		for j := 0; j < len(opCreaters); j++ {
			ResourceID := uint64(i*len(opCreaters) + j + 1)
			if i < 2 && j < 2 {
				_, ok := hb.resourcePendings[ResourceID]
				assert.False(t, ok)
			} else if i < 2 {
				_, ok := hb.resourcePendings[ResourceID]
				assert.True(t, ok)
				assert.Nil(t, hb.resourcePendings[ResourceID][movePeer])
				assert.NotNil(t, hb.resourcePendings[ResourceID][transferLeader])
			} else if j < 2 {
				_, ok := hb.resourcePendings[ResourceID]
				assert.True(t, ok)
				assert.NotNil(t, hb.resourcePendings[ResourceID][movePeer])
				assert.Nil(t, hb.resourcePendings[ResourceID][transferLeader])
			} else {
				_, ok := hb.resourcePendings[ResourceID]
				assert.True(t, ok)
				assert.NotNil(t, hb.resourcePendings[ResourceID][movePeer])
				assert.NotNil(t, hb.resourcePendings[ResourceID][transferLeader])
			}
		}
	}
}

func checkByteRateOnly(t *testing.T, tc *mockcluster.Cluster, opt *config.PersistOptions, hb schedule.Scheduler) {
	// Add containers 1, 2, 3, 4, 5, 6  with resource counts 3, 2, 2, 2, 0, 0.

	tc.AddLabelsContainer(1, 3, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsContainer(2, 2, map[string]string{"zone": "z2", "host": "h2"})
	tc.AddLabelsContainer(3, 2, map[string]string{"zone": "z3", "host": "h3"})
	tc.AddLabelsContainer(4, 2, map[string]string{"zone": "z4", "host": "h4"})
	tc.AddLabelsContainer(5, 0, map[string]string{"zone": "z2", "host": "h5"})
	tc.AddLabelsContainer(6, 0, map[string]string{"zone": "z5", "host": "h6"})
	tc.AddLabelsContainer(7, 0, map[string]string{"zone": "z5", "host": "h7"})
	tc.SetContainerDown(7)

	//| container_id | write_bytes_rate |
	//|----------|------------------|
	//|    1     |       7.5MB      |
	//|    2     |       4.5MB      |
	//|    3     |       4.5MB      |
	//|    4     |        6MB       |
	//|    5     |        0MB       |
	//|    6     |        0MB       |
	tc.UpdateStorageWrittenBytes(1, 7.5*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 4.5*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 4.5*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(4, 6*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(5, 0)
	tc.UpdateStorageWrittenBytes(6, 0)

	//| resource_id | leader_container | follower_container | follower_container | written_bytes |
	//|-----------|--------------|----------------|----------------|---------------|
	//|     1     |       1      |        2       |       3        |      512KB    |
	//|     2     |       1      |        3       |       4        |      512KB    |
	//|     3     |       1      |        2       |       4        |      512KB    |
	// resource 1, 2 and 3 are hot resources.
	addCachedResource(tc, write, []testCachedResource{
		{1, []uint64{1, 2, 3}, 512 * KB, 0},
		{2, []uint64{1, 3, 4}, 512 * KB, 0},
		{3, []uint64{1, 2, 4}, 512 * KB, 0},
	})
	assert.NotEmpty(t, hb.Schedule(tc))

	// Will transfer a hot resource from container 1, because the total count of peers
	// which is hot for container 1 is more larger than other containers.
	op := hb.Schedule(tc)[0]
	hb.(*hotScheduler).clearPendingInfluence()
	switch op.Len() {
	case 1:
		// balance by leader selected
		testutil.CheckTransferLeaderFrom(t, op, operator.OpHotResource, 1)
	case 4:
		// balance by peer selected
		if op.ResourceID() == 2 {
			// peer in container 1 of the resource 2 can transfer to container 5 or container 6 because of the label
			testutil.CheckTransferPeerWithLeaderTransferFrom(t, op, operator.OpHotResource, 1)
		} else {
			// peer in container 1 of the resource 1,3 can only transfer to container 6
			testutil.CheckTransferPeerWithLeaderTransfer(t, op, operator.OpHotResource, 1, 6)
		}
	default:
		t.Fatalf("wrong op: %v", op)
	}

	// hot resource scheduler is restricted by `hot-resource-schedule-limit`.
	tc.SetHotResourceScheduleLimit(0)
	assert.Empty(t, hb.Schedule(tc))
	hb.(*hotScheduler).clearPendingInfluence()
	tc.SetHotResourceScheduleLimit(int(config.NewTestOptions().GetScheduleConfig().HotResourceScheduleLimit))

	// hot resource scheduler is restricted by schedule limit.
	tc.SetLeaderScheduleLimit(0)
	for i := 0; i < 20; i++ {
		op := hb.Schedule(tc)[0]
		hb.(*hotScheduler).clearPendingInfluence()
		assert.Equal(t, 4, op.Len())
		if op.ResourceID() == 2 {
			// peer in container 1 of the resource 2 can transfer to container 5 or container 6 because of the label
			testutil.CheckTransferPeerWithLeaderTransferFrom(t, op, operator.OpHotResource, 1)
		} else {
			// peer in container 1 of the resource 1,3 can only transfer to container 6
			testutil.CheckTransferPeerWithLeaderTransfer(t, op, operator.OpHotResource, 1, 6)
		}
	}
	tc.SetLeaderScheduleLimit(int(config.NewTestOptions().GetScheduleConfig().LeaderScheduleLimit))

	// hot resource scheduler is not affect by `balance-resource-schedule-limit`.
	tc.SetResourceScheduleLimit(0)
	assert.Equal(t, 1, len(hb.Schedule(tc)))
	hb.(*hotScheduler).clearPendingInfluence()
	// Always produce operator
	assert.Equal(t, 1, len(hb.Schedule(tc)))
	hb.(*hotScheduler).clearPendingInfluence()
	assert.Equal(t, 1, len(hb.Schedule(tc)))
	hb.(*hotScheduler).clearPendingInfluence()

	//| container_id | write_bytes_rate |
	//|----------|------------------|
	//|    1     |        6MB       |
	//|    2     |        5MB       |
	//|    3     |        6MB       |
	//|    4     |        3.1MB     |
	//|    5     |        0MB       |
	//|    6     |        3MB       |
	tc.UpdateStorageWrittenBytes(1, 6*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 5*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 6*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(4, 3.1*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(5, 0)
	tc.UpdateStorageWrittenBytes(6, 3*MB*statistics.ContainerHeartBeatReportInterval)

	//| resource_id | leader_container | follower_container | follower_container | written_bytes |
	//|-----------|--------------|----------------|----------------|---------------|
	//|     1     |       1      |        2       |       3        |      512KB    |
	//|     2     |       1      |        2       |       3        |      512KB    |
	//|     3     |       6      |        1       |       4        |      512KB    |
	//|     4     |       5      |        6       |       4        |      512KB    |
	//|     5     |       3      |        4       |       5        |      512KB    |
	addCachedResource(tc, write, []testCachedResource{
		{1, []uint64{1, 2, 3}, 512 * KB, 0},
		{2, []uint64{1, 2, 3}, 512 * KB, 0},
		{3, []uint64{6, 1, 4}, 512 * KB, 0},
		{4, []uint64{5, 6, 4}, 512 * KB, 0},
		{5, []uint64{3, 4, 5}, 512 * KB, 0},
	})

	// 6 possible operator.
	// Assuming different operators have the same possibility,
	// if code has bug, at most 6/7 possibility to success,
	// test 30 times, possibility of success < 0.1%.
	// Cannot transfer leader because container 2 and container 3 are hot.
	// Source container is 1 or 3.
	//   resource 1 and 2 are the same, cannot move peer to container 5 due to the label.
	//   resource 3 can only move peer to container 5.
	//   resource 5 can only move peer to container 6.
	tc.SetLeaderScheduleLimit(0)
	for i := 0; i < 30; i++ {
		op := hb.Schedule(tc)[0]
		hb.(*hotScheduler).clearPendingInfluence()
		switch op.ResourceID() {
		case 1, 2:
			if op.Len() == 3 {
				testutil.CheckTransferPeer(t, op, operator.OpHotResource, 3, 6)
			} else if op.Len() == 4 {
				testutil.CheckTransferPeerWithLeaderTransfer(t, op, operator.OpHotResource, 1, 6)
			} else {
				t.Fatalf("wrong operator: %v", op)
			}
		case 3:
			testutil.CheckTransferPeer(t, op, operator.OpHotResource, 1, 5)
		case 5:
			testutil.CheckTransferPeerWithLeaderTransfer(t, op, operator.OpHotResource, 3, 6)
		default:
			t.Fatalf("wrong operator: %v", op)
		}
	}

	// Should not panic if resource not found.
	for i := uint64(1); i <= 3; i++ {
		tc.Resources.RemoveResource(tc.GetResource(i))
	}
	hb.Schedule(tc)
	hb.(*hotScheduler).clearPendingInfluence()
}

func TestByteRateOnly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	// TODO: enable palcement rules
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "host"})
	tc.DisableJointConsensus()
	hb, err := schedule.CreateScheduler(HotWriteResourceType, schedule.NewOperatorController(ctx, nil, nil), storage.NewTestStorage(), nil)
	assert.NoError(t, err)
	tc.SetHotResourceCacheHitsThreshold(0)

	checkByteRateOnly(t, tc, opt, hb)
	tc.SetEnablePlacementRules(true)
	checkByteRateOnly(t, tc, opt, hb)
}

func TestWithKeyRate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(HotWriteResourceType, schedule.NewOperatorController(ctx, nil, nil), storage.NewTestStorage(), nil)
	assert.NoError(t, err)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)

	tc := mockcluster.NewCluster(opt)
	tc.SetHotResourceCacheHitsThreshold(0)
	tc.DisableJointConsensus()
	tc.AddResourceContainer(1, 20)
	tc.AddResourceContainer(2, 20)
	tc.AddResourceContainer(3, 20)
	tc.AddResourceContainer(4, 20)
	tc.AddResourceContainer(5, 20)

	tc.UpdateStorageWrittenStats(1, 10.5*MB*statistics.ContainerHeartBeatReportInterval, 10.5*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 9.5*MB*statistics.ContainerHeartBeatReportInterval, 9.5*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 9.5*MB*statistics.ContainerHeartBeatReportInterval, 9.8*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 9*MB*statistics.ContainerHeartBeatReportInterval, 9*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(5, 8.9*MB*statistics.ContainerHeartBeatReportInterval, 9.2*MB*statistics.ContainerHeartBeatReportInterval)

	addCachedResource(tc, write, []testCachedResource{
		{1, []uint64{2, 1, 3}, 0.5 * MB, 0.5 * MB},
		{2, []uint64{2, 1, 3}, 0.5 * MB, 0.5 * MB},
		{3, []uint64{2, 4, 3}, 0.05 * MB, 0.1 * MB},
	})

	for i := 0; i < 100; i++ {
		hb.(*hotScheduler).clearPendingInfluence()
		op := hb.Schedule(tc)[0]
		// byteDecRatio <= 0.95 && keyDecRatio <= 0.95
		testutil.CheckTransferPeer(t, op, operator.OpHotResource, 1, 4)
		// container byte rate (min, max): (10, 10.5) | 9.5 | 9.5 | (9, 9.5) | 8.9
		// container key rate (min, max):  (10, 10.5) | 9.5 | 9.8 | (9, 9.5) | 9.2

		op = hb.Schedule(tc)[0]
		// byteDecRatio <= 0.99 && keyDecRatio <= 0.95
		testutil.CheckTransferPeer(t, op, operator.OpHotResource, 3, 5)
		// container byte rate (min, max): (10, 10.5) | 9.5 | (9.45, 9.5) | (9, 9.5) | (8.9, 8.95)
		// container key rate (min, max):  (10, 10.5) | 9.5 | (9.7, 9.8) | (9, 9.5) | (9.2, 9.3)

		// byteDecRatio <= 0.95
		// op = hb.Schedule(tc)[0]
		// FIXME: cover this case
		// testutil.CheckTransferPeerWithLeaderTransfer(t, op, operator.OpHotResource, 1, 5)
		// container byte rate (min, max): (9.5, 10.5) | 9.5 | (9.45, 9.5) | (9, 9.5) | (8.9, 9.45)
		// container key rate (min, max):  (9.2, 10.2) | 9.5 | (9.7, 9.8) | (9, 9.5) | (9.2, 9.8)
	}
}

func TestUnhealthyContainer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(HotWriteResourceType, schedule.NewOperatorController(ctx, nil, nil), storage.NewTestStorage(), nil)
	assert.NoError(t, err)
	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)
	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)

	tc := mockcluster.NewCluster(opt)
	tc.SetHotResourceCacheHitsThreshold(0)
	tc.DisableJointConsensus()
	tc.AddResourceContainer(1, 20)
	tc.AddResourceContainer(2, 20)
	tc.AddResourceContainer(3, 20)
	tc.AddResourceContainer(4, 20)

	tc.UpdateStorageWrittenStats(1, 10.5*MB*statistics.ContainerHeartBeatReportInterval, 10.5*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(2, 10*MB*statistics.ContainerHeartBeatReportInterval, 10*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(3, 9.5*MB*statistics.ContainerHeartBeatReportInterval, 9.5*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenStats(4, 0*MB*statistics.ContainerHeartBeatReportInterval, 0*MB*statistics.ContainerHeartBeatReportInterval)
	addCachedResource(tc, write, []testCachedResource{
		{1, []uint64{1, 2, 3}, 0.5 * MB, 0.5 * MB},
		{2, []uint64{2, 1, 3}, 0.5 * MB, 0.5 * MB},
		{3, []uint64{3, 2, 1}, 0.5 * MB, 0.5 * MB},
	})

	intervals := []time.Duration{
		9 * time.Second,
		10 * time.Second,
		19 * time.Second,
		20 * time.Second,
		9 * time.Minute,
		10 * time.Minute,
		29 * time.Minute,
		30 * time.Minute,
	}
	// test dst
	for _, interval := range intervals {
		tc.SetContainerLastHeartbeatInterval(4, interval)
		hb.(*hotScheduler).clearPendingInfluence()
		hb.Schedule(tc)
		// no panic
	}
}

func TestLeader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(HotWriteResourceType, schedule.NewOperatorController(ctx, nil, nil), storage.NewTestStorage(), nil)
	assert.NoError(t, err)

	tc := mockcluster.NewCluster(opt)
	tc.SetHotResourceCacheHitsThreshold(0)
	tc.AddResourceContainer(1, 20)
	tc.AddResourceContainer(2, 20)
	tc.AddResourceContainer(3, 20)

	tc.UpdateStorageWrittenBytes(1, 10*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 10*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 10*MB*statistics.ContainerHeartBeatReportInterval)

	tc.UpdateStorageWrittenKeys(1, 10*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(2, 10*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(3, 10*MB*statistics.ContainerHeartBeatReportInterval)

	// store1 has 2 peer as leader
	// store2 has 3 peer as leader
	// store3 has 2 peer as leader
	// If transfer leader from store2 to store1 or store3, it will keep on looping, which introduces a lot of unnecessary scheduling
	addCachedResource(tc, write, []testCachedResource{
		{1, []uint64{1, 2, 3}, 0.5 * MB, 1 * MB},
		{2, []uint64{1, 2, 3}, 0.5 * MB, 1 * MB},
		{3, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB},
		{4, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB},
		{5, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB},
		{6, []uint64{3, 1, 2}, 0.5 * MB, 1 * MB},
		{7, []uint64{3, 1, 2}, 0.5 * MB, 1 * MB},
	})

	for i := 0; i < 100; i++ {
		hb.(*hotScheduler).clearPendingInfluence()
		assert.Empty(t, hb.Schedule(tc))
	}

	addCachedResource(tc, write, []testCachedResource{
		{8, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB},
	})

	// store1 has 2 peer as leader
	// store2 has 4 peer as leader
	// store3 has 2 peer as leader
	// We expect to transfer leader from store2 to store1 or store3
	for i := 0; i < 100; i++ {
		hb.(*hotScheduler).clearPendingInfluence()
		op := hb.Schedule(tc)[0]
		testutil.CheckTransferLeaderFrom(t, op, operator.OpHotResource, 2)
		assert.Empty(t, hb.Schedule(tc))
	}
}

func TestWithPendingInfluence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	hb, err := schedule.CreateScheduler(HotWriteResourceType, schedule.NewOperatorController(ctx, nil, nil), storage.NewTestStorage(), nil)
	assert.NoError(t, err)
	for i := 0; i < 2; i++ {
		// 0: byte rate
		// 1: key rate
		tc := mockcluster.NewCluster(opt)
		tc.SetHotResourceCacheHitsThreshold(0)
		tc.SetLeaderScheduleLimit(0)
		tc.DisableJointConsensus()
		tc.AddResourceContainer(1, 20)
		tc.AddResourceContainer(2, 20)
		tc.AddResourceContainer(3, 20)
		tc.AddResourceContainer(4, 20)

		updateContainer := tc.UpdateStorageWrittenBytes // byte rate
		if i == 1 {                                     // key rate
			updateContainer = tc.UpdateStorageWrittenKeys
		}
		updateContainer(1, 8*MB*statistics.ContainerHeartBeatReportInterval)
		updateContainer(2, 6*MB*statistics.ContainerHeartBeatReportInterval)
		updateContainer(3, 6*MB*statistics.ContainerHeartBeatReportInterval)
		updateContainer(4, 4*MB*statistics.ContainerHeartBeatReportInterval)

		if i == 0 { // byte rate
			addCachedResource(tc, write, []testCachedResource{
				{1, []uint64{1, 2, 3}, 512 * KB, 0},
				{2, []uint64{1, 2, 3}, 512 * KB, 0},
				{3, []uint64{1, 2, 3}, 512 * KB, 0},
				{4, []uint64{1, 2, 3}, 512 * KB, 0},
				{5, []uint64{1, 2, 3}, 512 * KB, 0},
				{6, []uint64{1, 2, 3}, 512 * KB, 0},
			})
		} else if i == 1 { // key rate
			addCachedResource(tc, write, []testCachedResource{
				{1, []uint64{1, 2, 3}, 0, 512 * KB},
				{2, []uint64{1, 2, 3}, 0, 512 * KB},
				{3, []uint64{1, 2, 3}, 0, 512 * KB},
				{4, []uint64{1, 2, 3}, 0, 512 * KB},
				{5, []uint64{1, 2, 3}, 0, 512 * KB},
				{6, []uint64{1, 2, 3}, 0, 512 * KB},
			})
		}

		for i := 0; i < 20; i++ {
			hb.(*hotScheduler).clearPendingInfluence()
			cnt := 0
		testLoop:
			for j := 0; j < 1000; j++ {
				assert.True(t, cnt <= 5)
				emptyCnt := 0
				ops := hb.Schedule(tc)
				for len(ops) == 0 {
					emptyCnt++
					if emptyCnt >= 10 {
						break testLoop
					}
					ops = hb.Schedule(tc)
				}
				op := ops[0]
				switch op.Len() {
				case 1:
					// balance by leader selected
					testutil.CheckTransferLeaderFrom(t, op, operator.OpHotResource, 1)
				case 4:
					// balance by peer selected
					testutil.CheckTransferPeerWithLeaderTransfer(t, op, operator.OpHotResource, 1, 4)
					cnt++
					if cnt == 3 {
						assert.True(t, op.Cancel())
					}
				default:
					t.Fatalf("wrong op: %v", op)
				}
			}
			assert.Equal(t, 4, cnt)
		}
	}
}

func TestWithRuleEnabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	statistics.Denoising = false
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetEnablePlacementRules(true)
	hb, err := schedule.CreateScheduler(HotWriteResourceType, schedule.NewOperatorController(ctx, nil, nil), storage.NewTestStorage(), nil)
	assert.NoError(t, err)
	tc.SetHotResourceCacheHitsThreshold(0)
	key, err := hex.DecodeString("")
	assert.NoError(t, err)

	tc.AddResourceContainer(1, 20)
	tc.AddResourceContainer(2, 20)
	tc.AddResourceContainer(3, 20)

	err = tc.SetRule(&placement.Rule{
		GroupID:  "prophet",
		ID:       "leader",
		Index:    1,
		Override: true,
		Role:     placement.Leader,
		Count:    1,
		LabelConstraints: []placement.LabelConstraint{
			{
				Key:    "ID",
				Op:     placement.In,
				Values: []string{"2", "1"},
			},
		},
		StartKey: key,
		EndKey:   key,
	})
	assert.NoError(t, err)
	err = tc.SetRule(&placement.Rule{
		GroupID:  "prophet",
		ID:       "voter",
		Index:    2,
		Override: false,
		Role:     placement.Voter,
		Count:    2,
		StartKey: key,
		EndKey:   key,
	})
	assert.NoError(t, err)

	tc.UpdateStorageWrittenBytes(1, 10*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 10*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 10*MB*statistics.ContainerHeartBeatReportInterval)

	tc.UpdateStorageWrittenKeys(1, 10*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(2, 10*MB*statistics.ContainerHeartBeatReportInterval)
	tc.UpdateStorageWrittenKeys(3, 10*MB*statistics.ContainerHeartBeatReportInterval)

	addCachedResource(tc, write, []testCachedResource{
		{1, []uint64{1, 2, 3}, 0.5 * MB, 1 * MB},
		{2, []uint64{1, 2, 3}, 0.5 * MB, 1 * MB},
		{3, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB},
		{4, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB},
		{5, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB},
		{6, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB},
		{7, []uint64{2, 1, 3}, 0.5 * MB, 1 * MB},
	})

	for i := 0; i < 100; i++ {
		hb.(*hotScheduler).clearPendingInfluence()
		op := hb.Schedule(tc)[0]
		// The targetID should always be 1 as leader is only allowed to be placed in container1 or container2 by placement rule
		testutil.CheckTransferLeader(t, op, operator.OpHotResource, 2, 1)
		assert.Empty(t, hb.Schedule(tc))
	}
}

// TODO: fix testcase
// func TestHotReadByteRateOnly(t *testing.T) {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()
// 	opt := config.NewTestOptions()
// 	tc := mockcluster.NewCluster(opt)
// 	tc.DisableJointConsensus()
// 	hb, err := schedule.CreateScheduler(HotReadResourceType, schedule.NewOperatorController(ctx, tc, nil), storage.NewTestStorage(), nil)
// 	assert.NoError(t, err)
// 	tc.SetHotResourceCacheHitsThreshold(0)

// 	// Add containers 1, 2, 3, 4, 5 with resource counts 3, 2, 2, 2, 0.
// 	tc.AddResourceContainer(1, 3)
// 	tc.AddResourceContainer(2, 2)
// 	tc.AddResourceContainer(3, 2)
// 	tc.AddResourceContainer(4, 2)
// 	tc.AddResourceContainer(5, 0)

// 	//| container_id | read_bytes_rate |
// 	//|--------------|-----------------|
// 	//|       1      |     7.5MB       |
// 	//|       2      |     4.9MB       |
// 	//|       3      |     3.7MB       |
// 	//|       4      |       6MB       |
// 	//|       5      |       0MB       |
// 	tc.UpdateStorageReadBytes(1, 7.5*MB*statistics.ContainerHeartBeatReportInterval)
// 	tc.UpdateStorageReadBytes(2, 4.9*MB*statistics.ContainerHeartBeatReportInterval)
// 	tc.UpdateStorageReadBytes(3, 3.7*MB*statistics.ContainerHeartBeatReportInterval)
// 	tc.UpdateStorageReadBytes(4, 6*MB*statistics.ContainerHeartBeatReportInterval)
// 	tc.UpdateStorageReadBytes(5, 0)

// 	//| resource_id | leader_container | follower_container | follower_container |   read_bytes_rate  |
// 	//|-------------|------------------|--------------------|--------------------|--------------------|
// 	//|     1       |       1          |        2           |       3            |        512KB       |
// 	//|     2       |       2          |        1           |       3            |        512KB       |
// 	//|     3       |       1          |        2           |       3            |        512KB       |
// 	//|     11      |       1          |        2           |       3            |          7KB       |
// 	// resource 1, 2 and 3 are hot resources.
// 	addCachedResource(tc, read, []testCachedResource{
// 		{1, []uint64{1, 2, 3}, 512 * KB, 512 * KB / 100},
// 		{2, []uint64{2, 1, 3}, 512 * KB, 512 * KB / 100},
// 		{3, []uint64{1, 2, 3}, 512 * KB, 512 * KB / 100},
// 		{11, []uint64{1, 2, 3}, 7 * KB, 7 * KB / 100},
// 	})

// 	assert.True(t, tc.IsResourceHot(tc.GetResource(1)))
// 	assert.False(t, tc.IsResourceHot(tc.GetResource(11)))
// 	// check randomly pick hot resource
// 	r := tc.RandHotResourceFromContainer(2, statistics.ReadFlow)
// 	assert.NotNil(t, r)
// 	assert.Equal(t, uint64(2), r.Meta.ID())
// 	// check hot items
// 	stats := tc.HotCache.ResourceStats(statistics.ReadFlow, 0)
// 	assert.Equal(t, 2, len(stats))
// 	for _, ss := range stats {
// 		for _, s := range ss {
// 			assert.Equal(t, 512.0*KB, s.GetByteRate())
// 		}
// 	}

// 	testutil.CheckTransferLeader(t, hb.Schedule(tc)[0], operator.OpHotResource, 1, 3)
// 	hb.(*hotScheduler).clearPendingInfluence()
// 	// assume handle the operator
// 	tc.AddLeaderResourceWithReadInfo(3, 3, 512*KB*statistics.ResourceHeartBeatReportInterval, 0, statistics.ResourceHeartBeatReportInterval, []uint64{1, 2})
// 	// After transfer a hot resource leader from container 1 to container 3
// 	// the three resource leader will be evenly distributed in three containers

// 	//| container_id | read_bytes_rate |
// 	//|----------|-----------------|
// 	//|    1     |       6MB       |
// 	//|    2     |       5.5MB     |
// 	//|    3     |       5.5MB     |
// 	//|    4     |       3.4MB     |
// 	//|    5     |       3MB       |
// 	tc.UpdateStorageReadBytes(1, 6*MB*statistics.ContainerHeartBeatReportInterval)
// 	tc.UpdateStorageReadBytes(2, 5.5*MB*statistics.ContainerHeartBeatReportInterval)
// 	tc.UpdateStorageReadBytes(3, 5.5*MB*statistics.ContainerHeartBeatReportInterval)
// 	tc.UpdateStorageReadBytes(4, 3.4*MB*statistics.ContainerHeartBeatReportInterval)
// 	tc.UpdateStorageReadBytes(5, 3*MB*statistics.ContainerHeartBeatReportInterval)

// 	//| resource_id | leader_container | follower_container | follower_container |   read_bytes_rate  |
// 	//|-----------|--------------|----------------|----------------|--------------------|
// 	//|     1     |       1      |        2       |       3        |        512KB       |
// 	//|     2     |       2      |        1       |       3        |        512KB       |
// 	//|     3     |       3      |        2       |       1        |        512KB       |
// 	//|     4     |       1      |        2       |       3        |        512KB       |
// 	//|     5     |       4      |        2       |       5        |        512KB       |
// 	//|     11    |       1      |        2       |       3        |         24KB       |
// 	addCachedResource(tc, read, []testCachedResource{
// 		{4, []uint64{1, 2, 3}, 512 * KB, 0},
// 		{5, []uint64{4, 2, 5}, 512 * KB, 0},
// 	})

// 	// We will move leader peer of resource 1 from 1 to 5
// 	testutil.CheckTransferPeerWithLeaderTransfer(t, hb.Schedule(tc)[0], operator.OpHotResource, 1, 5)
// 	hb.(*hotScheduler).clearPendingInfluence()

// 	// Should not panic if resource not found.
// 	for i := uint64(1); i <= 3; i++ {
// 		tc.Resources.RemoveResource(tc.GetResource(i))
// 	}
// 	hb.Schedule(tc)
// 	hb.(*hotScheduler).clearPendingInfluence()
// }

// TODO: fix testcase
// func TestHotReadWithKeyRate(t *testing.T) {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()
// 	statistics.Denoising = false
// 	opt := config.NewTestOptions()

// 	tc := mockcluster.NewCluster(opt)
// 	tc.SetHotResourceCacheHitsThreshold(0)
// 	tc.AddResourceContainer(1, 20)
// 	tc.AddResourceContainer(2, 20)
// 	tc.AddResourceContainer(3, 20)
// 	tc.AddResourceContainer(4, 20)
// 	tc.AddResourceContainer(5, 20)

// 	tc.UpdateStorageReadStats(1, 10.5*MB*statistics.ContainerHeartBeatReportInterval, 10.5*MB*statistics.ContainerHeartBeatReportInterval)
// 	tc.UpdateStorageReadStats(2, 9.5*MB*statistics.ContainerHeartBeatReportInterval, 9.5*MB*statistics.ContainerHeartBeatReportInterval)
// 	tc.UpdateStorageReadStats(3, 9.5*MB*statistics.ContainerHeartBeatReportInterval, 9.8*MB*statistics.ContainerHeartBeatReportInterval)
// 	tc.UpdateStorageReadStats(4, 9*MB*statistics.ContainerHeartBeatReportInterval, 9*MB*statistics.ContainerHeartBeatReportInterval)
// 	tc.UpdateStorageReadStats(5, 8.9*MB*statistics.ContainerHeartBeatReportInterval, 9.2*MB*statistics.ContainerHeartBeatReportInterval)

// 	hb, err := schedule.CreateScheduler(HotReadResourceType, schedule.NewOperatorController(ctx, tc, nil), storage.NewTestStorage(), nil)
// 	assert.NoError(t, err)
// 	hb.(*hotScheduler).conf.SetSrcToleranceRatio(1)
// 	hb.(*hotScheduler).conf.SetDstToleranceRatio(1)

// 	addCachedResource(tc, read, []testCachedResource{
// 		{1, []uint64{1, 2, 4}, 0.5 * MB, 0.5 * MB},
// 		{2, []uint64{1, 2, 4}, 0.5 * MB, 0.5 * MB},
// 		{3, []uint64{3, 4, 5}, 0.05 * MB, 0.1 * MB},
// 	})

// 	for i := 0; i < 100; i++ {
// 		hb.(*hotScheduler).clearPendingInfluence()
// 		op := hb.Schedule(tc)[0]
// 		// byteDecRatio <= 0.95 && keyDecRatio <= 0.95
// 		testutil.CheckTransferLeader(t, op, operator.OpHotResource, 1, 4)
// 		// container byte rate (min, max): (10, 10.5) | 9.5 | 9.5 | (9, 9.5) | 8.9
// 		// container key rate (min, max):  (10, 10.5) | 9.5 | 9.8 | (9, 9.5) | 9.2

// 		op = hb.Schedule(tc)[0]
// 		// byteDecRatio <= 0.99 && keyDecRatio <= 0.95
// 		testutil.CheckTransferLeader(t, op, operator.OpHotResource, 3, 5)
// 		// container byte rate (min, max): (10, 10.5) | 9.5 | (9.45, 9.5) | (9, 9.5) | (8.9, 8.95)
// 		// container key rate (min, max):  (10, 10.5) | 9.5 | (9.7, 9.8) | (9, 9.5) | (9.2, 9.3)

// 		// byteDecRatio <= 0.95
// 		// FIXME: cover this case
// 		// op = hb.Schedule(tc)[0]
// 		// testutil.CheckTransferPeerWithLeaderTransfer(t, op, operator.OpHotResource, 1, 5)
// 		// container byte rate (min, max): (9.5, 10.5) | 9.5 | (9.45, 9.5) | (9, 9.5) | (8.9, 9.45)
// 		// container key rate (min, max):  (9.2, 10.2) | 9.5 | (9.7, 9.8) | (9, 9.5) | (9.2, 9.8)
// 	}
// }

// TODO: fix testcase
// func TestHotReadWithPendingInfluence(t *testing.T) {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()
// 	opt := config.NewTestOptions()
// 	hb, err := schedule.CreateScheduler(HotReadResourceType, schedule.NewOperatorController(ctx, mockcluster.NewCluster(opt), nil), storage.NewTestStorage(), nil)
// 	assert.NoError(t, err)
// 	// For test
// 	hb.(*hotScheduler).conf.GreatDecRatio = 0.99
// 	hb.(*hotScheduler).conf.MinorDecRatio = 1
// 	hb.(*hotScheduler).conf.DstToleranceRatio = 1

// 	for i := 0; i < 2; i++ {
// 		// 0: byte rate
// 		// 1: key rate
// 		tc := mockcluster.NewCluster(opt)
// 		tc.SetHotResourceCacheHitsThreshold(0)
// 		tc.DisableJointConsensus()
// 		tc.AddResourceContainer(1, 20)
// 		tc.AddResourceContainer(2, 20)
// 		tc.AddResourceContainer(3, 20)
// 		tc.AddResourceContainer(4, 20)

// 		updateContainer := tc.UpdateStorageReadBytes // byte rate
// 		if i == 1 {                                  // key rate
// 			updateContainer = tc.UpdateStorageReadKeys
// 		}
// 		updateContainer(1, 7.1*MB*statistics.ContainerHeartBeatReportInterval)
// 		updateContainer(2, 6.1*MB*statistics.ContainerHeartBeatReportInterval)
// 		updateContainer(3, 6*MB*statistics.ContainerHeartBeatReportInterval)
// 		updateContainer(4, 5*MB*statistics.ContainerHeartBeatReportInterval)

// 		if i == 0 { // byte rate
// 			addCachedResource(tc, read, []testCachedResource{
// 				{1, []uint64{1, 2, 3}, 512 * KB, 0},
// 				{2, []uint64{1, 2, 3}, 512 * KB, 0},
// 				{3, []uint64{1, 2, 3}, 512 * KB, 0},
// 				{4, []uint64{1, 2, 3}, 512 * KB, 0},
// 				{5, []uint64{2, 1, 3}, 512 * KB, 0},
// 				{6, []uint64{2, 1, 3}, 512 * KB, 0},
// 				{7, []uint64{3, 2, 1}, 512 * KB, 0},
// 				{8, []uint64{3, 2, 1}, 512 * KB, 0},
// 			})
// 		} else if i == 1 { // key rate
// 			addCachedResource(tc, read, []testCachedResource{
// 				{1, []uint64{1, 2, 3}, 0, 512 * KB},
// 				{2, []uint64{1, 2, 3}, 0, 512 * KB},
// 				{3, []uint64{1, 2, 3}, 0, 512 * KB},
// 				{4, []uint64{1, 2, 3}, 0, 512 * KB},
// 				{5, []uint64{2, 1, 3}, 0, 512 * KB},
// 				{6, []uint64{2, 1, 3}, 0, 512 * KB},
// 				{7, []uint64{3, 2, 1}, 0, 512 * KB},
// 				{8, []uint64{3, 2, 1}, 0, 512 * KB},
// 			})
// 		}

// 		for i := 0; i < 20; i++ {
// 			hb.(*hotScheduler).clearPendingInfluence()

// 			op1 := hb.Schedule(tc)[0]
// 			testutil.CheckTransferLeader(t, op1, operator.OpLeader, 1, 3)
// 			// container byte/key rate (min, max): (6.6, 7.1) | 6.1 | (6, 6.5) | 5

// 			op2 := hb.Schedule(tc)[0]
// 			testutil.CheckTransferPeerWithLeaderTransfer(t, op2, operator.OpHotResource, 1, 4)
// 			// container byte/key rate (min, max): (6.1, 7.1) | 6.1 | (6, 6.5) | (5, 5.5)

// 			ops := hb.Schedule(tc)
// 			t.Logf("%v", ops)
// 			assert.Empty(t, ops)
// 		}
// 		for i := 0; i < 20; i++ {
// 			hb.(*hotScheduler).clearPendingInfluence()

// 			op1 := hb.Schedule(tc)[0]
// 			testutil.CheckTransferLeader(t, op1, operator.OpLeader, 1, 3)
// 			// container byte/key rate (min, max): (6.6, 7.1) | 6.1 | (6, 6.5) | 5

// 			op2 := hb.Schedule(tc)[0]
// 			testutil.CheckTransferPeerWithLeaderTransfer(t, op2, operator.OpHotResource, 1, 4)
// 			// container bytekey rate (min, max): (6.1, 7.1) | 6.1 | (6, 6.5) | (5, 5.5)
// 			assert.True(t, op2.Cancel())
// 			// container byte/key rate (min, max): (6.6, 7.1) | 6.1 | (6, 6.5) | 5

// 			op2 = hb.Schedule(tc)[0]
// 			testutil.CheckTransferPeerWithLeaderTransfer(t, op2, operator.OpHotResource, 1, 4)
// 			// container byte/key rate (min, max): (6.1, 7.1) | 6.1 | (6, 6.5) | (5, 5.5)

// 			assert.True(t, op1.Cancel())
// 			// container byte/key rate (min, max): (6.6, 7.1) | 6.1 | 6 | (5, 5.5)

// 			op3 := hb.Schedule(tc)[0]
// 			testutil.CheckTransferPeerWithLeaderTransfer(t, op3, operator.OpHotResource, 1, 4)
// 			// container byte/key rate (min, max): (6.1, 7.1) | 6.1 | 6 | (5, 6)

// 			ops := hb.Schedule(tc)
// 			assert.Empty(t, ops)
// 		}
// 	}
// }

func TestUpdateCache(t *testing.T) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetHotResourceCacheHitsThreshold(0)

	/// For read flow
	addCachedResource(tc, read, []testCachedResource{
		{1, []uint64{1, 2, 3}, 512 * KB, 0},
		{2, []uint64{2, 1, 3}, 512 * KB, 0},
		{3, []uint64{1, 2, 3}, 20 * KB, 0},
		// lower than hot read flow rate, but higher than write flow rate
		{11, []uint64{1, 2, 3}, 7 * KB, 0},
	})
	stats := tc.ResourceStats(statistics.ReadFlow, 0)
	assert.Equal(t, 2, len(stats[1]))
	assert.Equal(t, 1, len(stats[2]))
	assert.Equal(t, 0, len(stats[3]))

	addCachedResource(tc, read, []testCachedResource{
		{3, []uint64{2, 1, 3}, 20 * KB, 0},
		{11, []uint64{1, 2, 3}, 7 * KB, 0},
	})
	stats = tc.ResourceStats(statistics.ReadFlow, 0)
	assert.Equal(t, 1, len(stats[1]))
	assert.Equal(t, 2, len(stats[2]))
	assert.Equal(t, 0, len(stats[3]))

	addCachedResource(tc, write, []testCachedResource{
		{4, []uint64{1, 2, 3}, 512 * KB, 0},
		{5, []uint64{1, 2, 3}, 20 * KB, 0},
		{6, []uint64{1, 2, 3}, 0.8 * KB, 0},
	})
	stats = tc.ResourceStats(statistics.WriteFlow, 0)
	assert.Equal(t, 2, len(stats[1]))
	assert.Equal(t, 2, len(stats[2]))
	assert.Equal(t, 2, len(stats[3]))

	addCachedResource(tc, write, []testCachedResource{
		{5, []uint64{1, 2, 5}, 20 * KB, 0},
	})
	stats = tc.ResourceStats(statistics.WriteFlow, 0)

	assert.Equal(t, 2, len(stats[1]))
	assert.Equal(t, 2, len(stats[2]))
	assert.Equal(t, 1, len(stats[3]))
	assert.Equal(t, 1, len(stats[5]))
}

func TestKeyThresholds(t *testing.T) {
	opt := config.NewTestOptions()
	{ // only a few resources
		tc := mockcluster.NewCluster(opt)
		tc.SetHotResourceCacheHitsThreshold(0)
		addCachedResource(tc, read, []testCachedResource{
			{1, []uint64{1, 2, 3}, 0, 1},
			{2, []uint64{1, 2, 3}, 0, 1 * KB},
		})
		stats := tc.ResourceStats(statistics.ReadFlow, 0)
		assert.Equal(t, 1, len(stats[1]))
		addCachedResource(tc, write, []testCachedResource{
			{3, []uint64{4, 5, 6}, 0, 1},
			{4, []uint64{4, 5, 6}, 0, 1 * KB},
		})
		stats = tc.ResourceStats(statistics.WriteFlow, 0)
		assert.Equal(t, 1, len(stats[4]))
		assert.Equal(t, 1, len(stats[5]))
		assert.Equal(t, 1, len(stats[6]))
	}
	{ // many resources
		tc := mockcluster.NewCluster(opt)
		resources := []testCachedResource{}
		for i := 1; i <= 1000; i += 2 {
			resources = append(resources,
				testCachedResource{
					id:      uint64(i),
					peers:   []uint64{1, 2, 3},
					keyRate: 100 * KB,
				},
				testCachedResource{
					id:      uint64(i + 1),
					peers:   []uint64{1, 2, 3},
					keyRate: 10 * KB,
				},
			)
		}

		{ // read
			addCachedResource(tc, read, resources)
			stats := tc.ResourceStats(statistics.ReadFlow, 0)
			assert.True(t, len(stats[1]) > 500)

			// for AntiCount
			addCachedResource(tc, read, resources)
			addCachedResource(tc, read, resources)
			addCachedResource(tc, read, resources)
			addCachedResource(tc, read, resources)
			stats = tc.ResourceStats(statistics.ReadFlow, 0)
			assert.Equal(t, 500, len(stats[1]))
		}
		{ // write
			addCachedResource(tc, write, resources)
			stats := tc.ResourceStats(statistics.WriteFlow, 0)
			assert.True(t, len(stats[1]) > 500)
			assert.True(t, len(stats[2]) > 500)
			assert.True(t, len(stats[3]) > 500)

			// for AntiCount
			addCachedResource(tc, write, resources)
			addCachedResource(tc, write, resources)
			addCachedResource(tc, write, resources)
			addCachedResource(tc, write, resources)
			stats = tc.ResourceStats(statistics.WriteFlow, 0)
			assert.Equal(t, 500, len(stats[1]))
			assert.Equal(t, 500, len(stats[2]))
			assert.Equal(t, 500, len(stats[3]))
		}
	}
}

func TestByteAndKey(t *testing.T) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetHotResourceCacheHitsThreshold(0)
	resources := []testCachedResource{}
	for i := 1; i <= 500; i++ {
		resources = append(resources, testCachedResource{
			id:       uint64(i),
			peers:    []uint64{1, 2, 3},
			byteRate: 100 * KB,
			keyRate:  100 * KB,
		})
	}
	{ // read
		addCachedResource(tc, read, resources)
		stats := tc.ResourceStats(statistics.ReadFlow, 0)
		assert.Equal(t, 500, len(stats[1]))

		addCachedResource(tc, read, []testCachedResource{
			{10001, []uint64{1, 2, 3}, 10 * KB, 10 * KB},
			{10002, []uint64{1, 2, 3}, 500 * KB, 10 * KB},
			{10003, []uint64{1, 2, 3}, 10 * KB, 500 * KB},
			{10004, []uint64{1, 2, 3}, 500 * KB, 500 * KB},
		})
		stats = tc.ResourceStats(statistics.ReadFlow, 0)
		assert.Equal(t, 503, len(stats[1]))
	}
	{ // write
		addCachedResource(tc, write, resources)
		stats := tc.ResourceStats(statistics.WriteFlow, 0)
		assert.Equal(t, 500, len(stats[1]))
		assert.Equal(t, 500, len(stats[2]))
		assert.Equal(t, 500, len(stats[3]))
		addCachedResource(tc, write, []testCachedResource{
			{10001, []uint64{1, 2, 3}, 10 * KB, 10 * KB},
			{10002, []uint64{1, 2, 3}, 500 * KB, 10 * KB},
			{10003, []uint64{1, 2, 3}, 10 * KB, 500 * KB},
			{10004, []uint64{1, 2, 3}, 500 * KB, 500 * KB},
		})
		stats = tc.ResourceStats(statistics.WriteFlow, 0)
		assert.Equal(t, 503, len(stats[1]))
		assert.Equal(t, 503, len(stats[2]))
		assert.Equal(t, 503, len(stats[3]))
	}
}

type testCachedResource struct {
	id uint64
	// the containerID list for the peers, the leader is containerd in the first container
	peers    []uint64
	byteRate float64
	keyRate  float64
}

func addCachedResource(tc *mockcluster.Cluster, rwTy rwType, resources []testCachedResource) {
	addFunc := tc.AddLeaderResourceWithReadInfo
	if rwTy == write {
		addFunc = tc.AddLeaderResourceWithWriteInfo
	}
	for _, r := range resources {
		addFunc(
			r.id, r.peers[0],
			uint64(r.byteRate*statistics.ResourceHeartBeatReportInterval),
			uint64(r.keyRate*statistics.ResourceHeartBeatReportInterval),
			statistics.ResourceHeartBeatReportInterval,
			r.peers[1:],
		)
	}
}

func newTestresource(id uint64) *core.CachedResource {
	peers := []metapb.Replica{{ID: id*100 + 1, ContainerID: 1}, {ID: id*100 + 2, ContainerID: 2}, {ID: id*100 + 3, ContainerID: 3}}
	return core.NewCachedResource(&metadata.TestResource{ResID: id, ResPeers: peers}, &peers[0])
}

func TestCheckResourceFlow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "host"})
	tc.DisableJointConsensus()
	sche, err := schedule.CreateScheduler(HotResourceType, schedule.NewOperatorController(ctx, tc, nil), storage.NewTestStorage(), schedule.ConfigJSONDecoder([]byte("null")))
	assert.NoError(t, err)
	hb := sche.(*hotScheduler)
	checkResourceFlowTest(t, tc, hb, write, tc.AddLeaderResourceWithWriteInfo)
	checkResourceFlowTest(t, tc, hb, read, tc.AddLeaderResourceWithReadInfo)
}

func checkResourceFlowTest(t *testing.T, tc *mockcluster.Cluster, hb *hotScheduler, kind rwType, heartbeat func(
	regionID uint64, leaderID uint64,
	readBytes, readKeys uint64,
	reportInterval uint64,
	followerIds []uint64, filledNums ...int) []*statistics.HotPeerStat) {

	tc.AddResourceContainer(2, 20)
	tc.UpdateStorageReadStats(2, 9.5*MB*statistics.ContainerHeartBeatReportInterval, 9.5*MB*statistics.ContainerHeartBeatReportInterval)
	// hot degree increase
	heartbeat(1, 1, 512*KB*statistics.ResourceHeartBeatReportInterval, 0, statistics.ResourceHeartBeatReportInterval, []uint64{2, 3}, 1)
	heartbeat(1, 1, 512*KB*statistics.ResourceHeartBeatReportInterval, 0, statistics.ResourceHeartBeatReportInterval, []uint64{2, 3}, 1)
	items := heartbeat(1, 1, 512*KB*statistics.ResourceHeartBeatReportInterval, 0, statistics.ResourceHeartBeatReportInterval, []uint64{2, 3}, 1)
	assert.True(t, len(items) > 0)
	for _, item := range items {
		assert.Equal(t, 3, item.HotDegree)
	}

	// transfer leader, skip the first heartbeat and schedule.
	items = heartbeat(1, 2, 512*KB*statistics.ResourceHeartBeatReportInterval, 0, statistics.ResourceHeartBeatReportInterval, []uint64{1, 3}, 1)
	for _, item := range items {
		if !item.IsNeedDelete() {
			assert.Equal(t, 3, item.HotDegree)
		}
	}

	// try schedule
	hb.prepareForBalance(tc)
	leaderSolver := newBalanceSolver(hb, tc, kind, transferLeader)
	leaderSolver.cur = &solution{srcContainerID: 2}
	assert.Empty(t, leaderSolver.filterHotPeers()) // skip schedule
	threshold := tc.GetHotResourceCacheHitsThreshold()
	tc.SetHotResourceCacheHitsThreshold(0)
	assert.Equal(t, 1, len(leaderSolver.filterHotPeers()))
	tc.SetHotResourceCacheHitsThreshold(threshold)

	// move peer: add peer and remove peer
	items = heartbeat(1, 2, 512*KB*statistics.ResourceHeartBeatReportInterval, 0, statistics.ResourceHeartBeatReportInterval, []uint64{1, 3, 4}, 1)
	assert.True(t, len(items) > 0)
	for _, item := range items {
		assert.Equal(t, 4, item.HotDegree)
	}
	items = heartbeat(1, 2, 512*KB*statistics.ResourceHeartBeatReportInterval, 0, statistics.ResourceHeartBeatReportInterval, []uint64{1, 4}, 1)
	assert.True(t, len(items) > 0)
	for _, item := range items {
		if item.ContainerID == 3 {
			assert.True(t, item.IsNeedDelete())
			continue
		}
		assert.Equal(t, 5, item.HotDegree)
	}
}

func TestCheckResourceFlowWithDifferentThreshold(t *testing.T) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "host"})
	tc.DisableJointConsensus()
	// some peers are hot, and some are cold #3198
	rate := uint64(512 * KB)
	for i := 0; i < statistics.TopNN; i++ {
		for j := 0; j < statistics.DefaultAotSize; j++ {
			tc.AddLeaderResourceWithWriteInfo(uint64(i+100), 1, rate*statistics.ResourceHeartBeatReportInterval, 0, statistics.ResourceHeartBeatReportInterval, []uint64{2, 3}, 1)
		}
	}
	items := tc.AddLeaderResourceWithWriteInfo(201, 1, rate*statistics.ResourceHeartBeatReportInterval, 0, statistics.ResourceHeartBeatReportInterval, []uint64{2, 3}, 1)
	assert.Equal(t, float64(rate)*statistics.HotThresholdRatio, items[0].GetThresholds()[0])
	// Threshold of store 1,2,3 is 409.6 KB and others are 1 KB
	// Make the hot threshold of some store is high and the others are low
	rate = 10 * KB
	tc.AddLeaderResourceWithWriteInfo(201, 1, rate*statistics.ResourceHeartBeatReportInterval, 0, statistics.ResourceHeartBeatReportInterval, []uint64{2, 3, 4}, 1)
	items = tc.AddLeaderResourceWithWriteInfo(201, 1, rate*statistics.ResourceHeartBeatReportInterval, 0, statistics.ResourceHeartBeatReportInterval, []uint64{3, 4}, 1)
	for _, item := range items {
		if item.ContainerID < 4 {
			assert.True(t, item.IsNeedDelete())
		} else {
			assert.False(t, item.IsNeedDelete())
		}
	}
}
