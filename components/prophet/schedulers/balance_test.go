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
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/hbstream"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/testutil"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

type testBalanceSpeedCase struct {
	sourceCount    uint64
	targetCount    uint64
	resourceSize   int64
	expectedResult bool
	kind           core.SchedulePolicy
}

func TestShouldBalance(t *testing.T) {
	// container size = 100GiB
	// resource size = 96MiB
	const R = 96
	tests := []testBalanceSpeedCase{
		// target size is zero
		{2, 0, R / 10, true, core.BySize},
		{2, 0, R, false, core.BySize},
		// all in high space stage
		{10, 5, R / 10, true, core.BySize},
		{10, 5, 2 * R, false, core.BySize},
		{10, 10, R / 10, false, core.BySize},
		{10, 10, 2 * R, false, core.BySize},
		// all in transition stage
		{700, 680, R / 10, true, core.BySize},
		{700, 680, 5 * R, false, core.BySize},
		{700, 700, R / 10, false, core.BySize},
		// all in low space stage
		{900, 890, R / 10, true, core.BySize},
		{900, 890, 5 * R, false, core.BySize},
		{900, 900, R / 10, false, core.BySize},
		// one in high space stage, other in transition stage
		{650, 550, R, true, core.BySize},
		{650, 500, 50 * R, false, core.BySize},
		// one in transition space stage, other in low space stage
		{800, 700, R, true, core.BySize},
		{800, 700, 50 * R, false, core.BySize},

		// default leader tolerant ratio is 5, when schedule by count
		// target size is zero
		{2, 0, R / 10, false, core.ByCount},
		{2, 0, R, false, core.ByCount},
		// all in high space stage
		{10, 5, R / 10, true, core.ByCount},
		{10, 5, 2 * R, true, core.ByCount},
		{10, 6, 2 * R, false, core.ByCount},
		{10, 10, R / 10, false, core.ByCount},
		{10, 10, 2 * R, false, core.ByCount},
		// all in transition stage
		{70, 50, R / 10, true, core.ByCount},
		{70, 50, 5 * R, true, core.ByCount},
		{70, 70, R / 10, false, core.ByCount},
		// all in low space stage
		{90, 80, R / 10, true, core.ByCount},
		{90, 80, 5 * R, true, core.ByCount},
		{90, 90, R / 10, false, core.ByCount},
		// one in high space stage, other in transition stage
		{65, 55, R / 2, true, core.ByCount},
		{65, 50, 5 * R, true, core.ByCount},
		// one in transition space stage, other in low space stage
		{80, 70, R / 2, true, core.ByCount},
		{80, 70, 5 * R, true, core.ByCount},
	}

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetTolerantSizeRatio(2.5)
	tc.SetResourceScoreFormulaVersion("v1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oc := schedule.NewOperatorController(ctx, nil, nil)
	// create a resource to control average resource size.
	tc.AddLeaderResource(1, 1, 2)

	for _, c := range tests {
		tc.AddLeaderContainer(1, int(c.sourceCount))
		tc.AddLeaderContainer(2, int(c.targetCount))
		source := tc.GetContainer(1)
		target := tc.GetContainer(2)
		resource := tc.GetResource(1).Clone(core.SetApproximateSize(c.resourceSize))
		tc.PutResource(resource)
		tc.SetLeaderSchedulePolicy(c.kind.String())
		kind := core.NewScheduleKind(metapb.ResourceKind_LeaderKind, c.kind)
		shouldBalance, _, _ := shouldBalance(tc, source, target, resource, kind, oc.GetOpInfluence(tc), "")
		assert.Equal(t, c.expectedResult, shouldBalance)
	}

	for _, c := range tests {
		if c.kind.String() == core.BySize.String() {
			tc.AddResourceContainer(1, int(c.sourceCount))
			tc.AddResourceContainer(2, int(c.targetCount))
			source := tc.GetContainer(1)
			target := tc.GetContainer(2)
			resource := tc.GetResource(1).Clone(core.SetApproximateSize(c.resourceSize))
			tc.PutResource(resource)
			kind := core.NewScheduleKind(metapb.ResourceKind_ReplicaKind, c.kind)
			shouldBalance, _, _ := shouldBalance(tc, source, target, resource, kind, oc.GetOpInfluence(tc), "")
			assert.Equal(t, c.expectedResult, shouldBalance)
		}
	}
}

func TestBalanceLimit(t *testing.T) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.AddLeaderContainer(1, 10)
	tc.AddLeaderContainer(2, 20)
	tc.AddLeaderContainer(3, 30)

	// StandDeviation is sqrt((10^2+0+10^2)/3).
	assert.Equal(t, uint64(math.Sqrt(200.0/3.0)), adjustBalanceLimit("", tc, metapb.ResourceKind_LeaderKind))

	tc.SetContainerOffline(1)
	// StandDeviation is sqrt((5^2+5^2)/2).
	assert.Equal(t, uint64(math.Sqrt(50.0/2.0)), adjustBalanceLimit("", tc, metapb.ResourceKind_LeaderKind))
}

func TestTolerantRatio(t *testing.T) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	// create a resource to control average resource size.
	assert.NotNil(t, tc.AddLeaderResource(1, 1, 2))
	resourceSize := int64(96 * KB)
	resource := tc.GetResource(1).Clone(core.SetApproximateSize(resourceSize))

	tc.SetTolerantSizeRatio(0)
	assert.Equal(t, getTolerantResource(tc, resource, core.ScheduleKind{ResourceKind: metapb.ResourceKind_LeaderKind, Policy: core.ByCount}), int64(leaderTolerantSizeRatio))
	assert.Equal(t, getTolerantResource(tc, resource, core.ScheduleKind{ResourceKind: metapb.ResourceKind_LeaderKind, Policy: core.BySize}), int64(adjustTolerantRatio("", tc)*float64(resourceSize)))
	assert.Equal(t, getTolerantResource(tc, resource, core.ScheduleKind{ResourceKind: metapb.ResourceKind_ReplicaKind, Policy: core.ByCount}), int64(adjustTolerantRatio("", tc)*float64(resourceSize)))
	assert.Equal(t, getTolerantResource(tc, resource, core.ScheduleKind{ResourceKind: metapb.ResourceKind_ReplicaKind, Policy: core.BySize}), int64(adjustTolerantRatio("", tc)*float64(resourceSize)))

	tc.SetTolerantSizeRatio(10)
	assert.Equal(t, getTolerantResource(tc, resource, core.ScheduleKind{ResourceKind: metapb.ResourceKind_LeaderKind, Policy: core.ByCount}), int64(tc.GetScheduleConfig().TolerantSizeRatio))
	assert.Equal(t, getTolerantResource(tc, resource, core.ScheduleKind{ResourceKind: metapb.ResourceKind_LeaderKind, Policy: core.BySize}), int64(adjustTolerantRatio("", tc)*float64(resourceSize)))
	assert.Equal(t, getTolerantResource(tc, resource, core.ScheduleKind{ResourceKind: metapb.ResourceKind_ReplicaKind, Policy: core.ByCount}), int64(adjustTolerantRatio("", tc)*float64(resourceSize)))
	assert.Equal(t, getTolerantResource(tc, resource, core.ScheduleKind{ResourceKind: metapb.ResourceKind_ReplicaKind, Policy: core.BySize}), int64(adjustTolerantRatio("", tc)*float64(resourceSize)))
}

type testBalanceLeaderScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	lb     schedule.Scheduler
	oc     *schedule.OperatorController
	opt    *config.PersistOptions
}

func (s *testBalanceLeaderScheduler) setup(t *testing.T) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.opt = config.NewTestOptions()
	s.tc = mockcluster.NewCluster(s.opt)
	s.oc = schedule.NewOperatorController(s.ctx, s.tc, nil)
	lb, err := schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"0", "", ""}))
	assert.NoError(t, err)
	s.lb = lb
}

func (s *testBalanceLeaderScheduler) tearDown() {
	s.cancel()
}

func (s *testBalanceLeaderScheduler) schedule() []*operator.Operator {
	return s.lb.Schedule(s.tc)
}

func TestLeaderBalanceLimit(t *testing.T) {
	s := &testBalanceLeaderScheduler{}
	s.setup(t)
	defer s.tearDown()

	s.tc.SetTolerantSizeRatio(2.5)
	// containers:     1    2    3    4
	// Leaders:        1    0    0    0
	// resources:      L    F    F    F
	s.tc.AddLeaderContainer(1, 1)
	s.tc.AddLeaderContainer(2, 0)
	s.tc.AddLeaderContainer(3, 0)
	s.tc.AddLeaderContainer(4, 0)
	s.tc.AddLeaderResource(1, 1, 2, 3, 4)
	assert.Nil(t, s.schedule())

	// containers:     1    2    3    4
	// Leaders:        16   0    0    0
	// resources:      L    F    F    F
	s.tc.UpdateLeaderCount(1, 16)
	assert.NotNil(t, s.schedule())

	// containers:     1    2    3    4
	// Leaders:        7    8    9   10
	// resources:      F    F    F    L
	s.tc.UpdateLeaderCount(1, 7)
	s.tc.UpdateLeaderCount(2, 8)
	s.tc.UpdateLeaderCount(3, 9)
	s.tc.UpdateLeaderCount(4, 10)
	s.tc.AddLeaderResource(1, 4, 1, 2, 3)
	assert.Nil(t, s.schedule())

	// containers:     1    2    3    4
	// Leaders:        7    8    9   16
	// resources:      F    F    F    L
	s.tc.UpdateLeaderCount(4, 16)
	assert.NotNil(t, s.schedule())
}

func TestBalanceLeaderSchedulePolicy(t *testing.T) {
	s := &testBalanceLeaderScheduler{}
	s.setup(t)
	defer s.tearDown()

	// containers:          1       2       3       4
	// Leader Count:    10      10      10      10
	// Leader Size :    10000   100    	100    	100
	// resource1:         L       F       F       F
	s.tc.AddLeaderContainer(1, 10, 10000*MB)
	s.tc.AddLeaderContainer(2, 10, 100*MB)
	s.tc.AddLeaderContainer(3, 10, 100*MB)
	s.tc.AddLeaderContainer(4, 10, 100*MB)
	s.tc.AddLeaderResource(1, 1, 2, 3, 4)
	assert.Equal(t, core.ByCount.String(), s.tc.GetScheduleConfig().LeaderSchedulePolicy)
	assert.Nil(t, s.schedule())
	s.tc.SetLeaderSchedulePolicy(core.BySize.String())
	assert.NotNil(t, s.schedule())
}

func TestBalanceLeaderTolerantRatio(t *testing.T) {
	s := &testBalanceLeaderScheduler{}
	s.setup(t)
	defer s.tearDown()

	s.tc.SetTolerantSizeRatio(2.5)
	// test schedule leader by count, with tolerantSizeRatio=2.5
	// containers:          1       2       3       4
	// Leader Count:    14->15  10      10      10
	// Leader Size :    100     100     100     100
	// resource1:         L       F       F       F
	s.tc.AddLeaderContainer(1, 14, 100)
	s.tc.AddLeaderContainer(2, 10, 100)
	s.tc.AddLeaderContainer(3, 10, 100)
	s.tc.AddLeaderContainer(4, 10, 100)
	s.tc.AddLeaderResource(1, 1, 2, 3, 4)

	assert.Equal(t, core.ByCount.String(), s.tc.GetScheduleConfig().LeaderSchedulePolicy) // default by count
	assert.Nil(t, s.schedule())
	assert.Equal(t, 14, s.tc.GetContainer(1).GetLeaderCount(""))
	s.tc.AddLeaderContainer(1, 15, 100)
	assert.Equal(t, 15, s.tc.GetContainer(1).GetLeaderCount(""))
	assert.NotNil(t, s.schedule())
	s.tc.SetTolerantSizeRatio(6) // (15-10)<6
	assert.Nil(t, s.schedule())
}

func TestScheduleWithOpInfluence(t *testing.T) {
	s := &testBalanceLeaderScheduler{}
	s.setup(t)
	defer s.tearDown()

	s.tc.SetTolerantSizeRatio(2.5)
	// containers:     1    2    3    4
	// Leaders:    7    8    9   14
	// resource1:    F    F    F    L
	s.tc.AddLeaderContainer(1, 7)
	s.tc.AddLeaderContainer(2, 8)
	s.tc.AddLeaderContainer(3, 9)
	s.tc.AddLeaderContainer(4, 14)
	s.tc.AddLeaderResource(1, 4, 1, 2, 3)
	op := s.schedule()[0]
	assert.NotNil(t, op)
	s.oc.SetOperator(op)
	// After considering the scheduled operator, leaders of container1 and container4 are 8
	// and 13 respectively. As the `TolerantSizeRatio` is 2.5, `shouldBalance`
	// returns false when leader difference is not greater than 5.
	assert.Equal(t, core.ByCount.String(), s.tc.GetScheduleConfig().LeaderSchedulePolicy) // default by count
	assert.NotNil(t, s.schedule())
	s.tc.SetLeaderSchedulePolicy(core.BySize.String())
	assert.Nil(t, s.schedule())

	// containers:     1    2    3    4
	// Leaders:    8    8    9   13
	// resource1:    F    F    F    L
	s.tc.UpdateLeaderCount(1, 8)
	s.tc.UpdateLeaderCount(2, 8)
	s.tc.UpdateLeaderCount(3, 9)
	s.tc.UpdateLeaderCount(4, 13)
	s.tc.AddLeaderResource(1, 4, 1, 2, 3)
	assert.Nil(t, s.schedule())
}

func TestTransferLeaderOut(t *testing.T) {
	s := &testBalanceLeaderScheduler{}
	s.setup(t)
	defer s.tearDown()

	// containers:     1    2    3    4
	// Leaders:    7    8    9   12
	s.tc.AddLeaderContainer(1, 7)
	s.tc.AddLeaderContainer(2, 8)
	s.tc.AddLeaderContainer(3, 9)
	s.tc.AddLeaderContainer(4, 12)
	s.tc.SetTolerantSizeRatio(0.1)
	for i := uint64(1); i <= 7; i++ {
		s.tc.AddLeaderResource(i, 4, 1, 2, 3)
	}

	// balance leader: 4->1, 4->1, 4->2
	resources := make(map[uint64]struct{})
	targets := map[uint64]uint64{
		1: 2,
		2: 1,
	}
	for i := 0; i < 20; i++ {
		if len(s.schedule()) == 0 {
			continue
		}
		if op := s.schedule()[0]; op != nil {
			if _, ok := resources[op.ResourceID()]; !ok {
				s.oc.SetOperator(op)
				resources[op.ResourceID()] = struct{}{}
				tr := op.Step(0).(operator.TransferLeader)
				assert.Equal(t, uint64(4), tr.FromContainer)
				targets[tr.ToContainer]--
			}
		}
	}
	assert.Equal(t, 3, len(resources))
	for _, count := range targets {
		assert.Equal(t, uint64(0), count)
	}
}

func TestBalanceFilter(t *testing.T) {
	s := &testBalanceLeaderScheduler{}
	s.setup(t)
	defer s.tearDown()

	// containers:     1    2    3    4
	// Leaders:    1    2    3   16
	// resource1:    F    F    F    L
	s.tc.AddLeaderContainer(1, 1)
	s.tc.AddLeaderContainer(2, 2)
	s.tc.AddLeaderContainer(3, 3)
	s.tc.AddLeaderContainer(4, 16)
	s.tc.AddLeaderResource(1, 4, 1, 2, 3)

	testutil.CheckTransferLeader(t, s.schedule()[0], operator.OpKind(0), 4, 1)
	// Test stateFilter.
	// if container 4 is offline, we should consider it
	// because it still provides services
	s.tc.SetContainerOffline(4)
	testutil.CheckTransferLeader(t, s.schedule()[0], operator.OpKind(0), 4, 1)
	// If container 1 is down, it will be filtered,
	// container 2 becomes the container with least leaders.
	s.tc.SetContainerDown(1)
	testutil.CheckTransferLeader(t, s.schedule()[0], operator.OpKind(0), 4, 2)

	// Test healthFilter.
	// If container 2 is busy, it will be filtered,
	// container 3 becomes the container with least leaders.
	s.tc.SetContainerBusy(2, true)
	testutil.CheckTransferLeader(t, s.schedule()[0], operator.OpKind(0), 4, 3)

	// Test disconnectFilter.
	// If container 3 is disconnected, no operator can be created.
	s.tc.SetContainerDisconnect(3)
	assert.Empty(t, s.schedule())
}

func TestLeaderWeight(t *testing.T) {
	s := &testBalanceLeaderScheduler{}
	s.setup(t)
	defer s.tearDown()

	// containers:     1       2       3       4
	// Leaders:    10      10      10      10
	// Weight:     0.5     0.9     1       2
	// resource1:    L       F       F       F
	s.tc.SetTolerantSizeRatio(2.5)
	for i := uint64(1); i <= 4; i++ {
		s.tc.AddLeaderContainer(i, 10)
	}
	s.tc.UpdateContainerLeaderWeight(1, 0.5)
	s.tc.UpdateContainerLeaderWeight(2, 0.9)
	s.tc.UpdateContainerLeaderWeight(3, 1)
	s.tc.UpdateContainerLeaderWeight(4, 2)
	s.tc.AddLeaderResource(1, 1, 2, 3, 4)
	testutil.CheckTransferLeader(t, s.schedule()[0], operator.OpKind(0), 1, 4)
	s.tc.UpdateLeaderCount(4, 30)
	testutil.CheckTransferLeader(t, s.schedule()[0], operator.OpKind(0), 1, 3)
}

func TestBalancePolicy(t *testing.T) {
	s := &testBalanceLeaderScheduler{}
	s.setup(t)
	defer s.tearDown()

	// containers:       1    2     3    4
	// LeaderCount: 20   66     6   20
	// LeaderSize:  66   20    20    6
	s.tc.AddLeaderContainer(1, 20, 600*MB)
	s.tc.AddLeaderContainer(2, 66, 200*MB)
	s.tc.AddLeaderContainer(3, 6, 20*MB)
	s.tc.AddLeaderContainer(4, 20, 1*MB)
	s.tc.AddLeaderResource(1, 2, 1, 3, 4)
	s.tc.AddLeaderResource(2, 1, 2, 3, 4)
	s.tc.SetLeaderSchedulePolicy("count")
	testutil.CheckTransferLeader(t, s.schedule()[0], operator.OpKind(0), 2, 3)
	s.tc.SetLeaderSchedulePolicy("size")
	testutil.CheckTransferLeader(t, s.schedule()[0], operator.OpKind(0), 1, 4)
}

func TestBalanceSelector(t *testing.T) {
	s := &testBalanceLeaderScheduler{}
	s.setup(t)
	defer s.tearDown()

	// containers:     1    2    3    4
	// Leaders:    1    2    3   16
	// resource1:    -    F    F    L
	// resource2:    F    F    L    -
	s.tc.AddLeaderContainer(1, 1)
	s.tc.AddLeaderContainer(2, 2)
	s.tc.AddLeaderContainer(3, 3)
	s.tc.AddLeaderContainer(4, 16)
	s.tc.AddLeaderResource(1, 4, 2, 3)
	s.tc.AddLeaderResource(2, 3, 1, 2)
	// container4 has max leader score, container1 has min leader score.
	// The scheduler try to move a leader out of 16 first.
	testutil.CheckTransferLeader(t, s.schedule()[0], operator.OpKind(0), 4, 2)

	// containers:     1    2    3    4
	// Leaders:    1    14   15   16
	// resource1:    -    F    F    L
	// resource2:    F    F    L    -
	s.tc.UpdateLeaderCount(2, 14)
	s.tc.UpdateLeaderCount(3, 15)
	// Cannot move leader out of container4, move a leader into container1.
	testutil.CheckTransferLeader(t, s.schedule()[0], operator.OpKind(0), 3, 1)

	// containers:     1    2    3    4
	// Leaders:    1    2    15   16
	// resource1:    -    F    L    F
	// resource2:    L    F    F    -
	s.tc.AddLeaderContainer(2, 2)
	s.tc.AddLeaderResource(1, 3, 2, 4)
	s.tc.AddLeaderResource(2, 1, 2, 3)
	// No leader in container16, no follower in container1. Now source and target are container3 and container2.
	testutil.CheckTransferLeader(t, s.schedule()[0], operator.OpKind(0), 3, 2)

	// containers:     1    2    3    4
	// Leaders:    9    10   10   11
	// resource1:    -    F    F    L
	// resource2:    L    F    F    -
	for i := uint64(1); i <= 4; i++ {
		s.tc.AddLeaderContainer(i, 10)
	}
	s.tc.AddLeaderResource(1, 4, 2, 3)
	s.tc.AddLeaderResource(2, 1, 2, 3)
	// The cluster is balanced.
	assert.Empty(t, s.schedule())
	assert.Empty(t, s.schedule())

	// container3's leader drops:
	// containers:     1    2    3    4
	// Leaders:    11   13   0    16
	// resource1:    -    F    F    L
	// resource2:    L    F    F    -
	s.tc.AddLeaderContainer(1, 11)
	s.tc.AddLeaderContainer(2, 13)
	s.tc.AddLeaderContainer(3, 0)
	s.tc.AddLeaderContainer(4, 16)
	testutil.CheckTransferLeader(t, s.schedule()[0], operator.OpKind(0), 4, 3)
}

type testBalanceLeaderRangeScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	oc     *schedule.OperatorController
}

func (s *testBalanceLeaderRangeScheduler) setup() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	opt := config.NewTestOptions()
	s.tc = mockcluster.NewCluster(opt)
	s.oc = schedule.NewOperatorController(s.ctx, s.tc, nil)
}

func (s *testBalanceLeaderRangeScheduler) tearDown() {
	s.cancel()
}

func TestSingleRangeBalance(t *testing.T) {
	s := &testBalanceLeaderRangeScheduler{}
	s.setup()
	defer s.tearDown()

	// containers:     1       2       3       4
	// Leaders:    10      10      10      10
	// Weight:     0.5     0.9     1       2
	// resource1:    L       F       F       F

	for i := uint64(1); i <= 4; i++ {
		s.tc.AddLeaderContainer(i, 10)
	}
	s.tc.UpdateContainerLeaderWeight(1, 0.5)
	s.tc.UpdateContainerLeaderWeight(2, 0.9)
	s.tc.UpdateContainerLeaderWeight(3, 1)
	s.tc.UpdateContainerLeaderWeight(4, 2)
	s.tc.AddLeaderResourceWithRange(1, "a", "g", 1, 2, 3, 4)
	lb, err := schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"0", "", ""}))
	assert.NoError(t, err)
	ops := lb.Schedule(s.tc)
	assert.NotEmpty(t, ops)
	assert.Equal(t, 1, len(ops))
	assert.Equal(t, 3, len(ops[0].Counters))
	assert.Equal(t, 2, len(ops[0].FinishedCounters))
	lb, err = schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"0", "h", "n"}))
	assert.NoError(t, err)
	assert.Empty(t, lb.Schedule(s.tc))
	lb, err = schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"0", "b", "f"}))
	assert.NoError(t, err)
	assert.Empty(t, lb.Schedule(s.tc))
	lb, err = schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"0", "", "a"}))
	assert.NoError(t, err)
	assert.Empty(t, lb.Schedule(s.tc))
	lb, err = schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"0", "g", ""}))
	assert.NoError(t, err)
	assert.Empty(t, lb.Schedule(s.tc))
	lb, err = schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"0", "", "f"}))
	assert.NoError(t, err)
	assert.Empty(t, lb.Schedule(s.tc))
	lb, err = schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"0", "b", ""}))
	assert.NoError(t, err)
	assert.Empty(t, lb.Schedule(s.tc))
}

func TestMultiRangeBalance(t *testing.T) {
	s := &testBalanceLeaderRangeScheduler{}
	s.setup()
	defer s.tearDown()

	// Containers:     1       2       3       4
	// Leaders:    10      10      10      10
	// Weight:     0.5     0.9     1       2
	// resource1:    L       F       F       F

	for i := uint64(1); i <= 4; i++ {
		s.tc.AddLeaderContainer(i, 10)
	}
	s.tc.UpdateContainerLeaderWeight(1, 0.5)
	s.tc.UpdateContainerLeaderWeight(2, 0.9)
	s.tc.UpdateContainerLeaderWeight(3, 1)
	s.tc.UpdateContainerLeaderWeight(4, 2)
	s.tc.AddLeaderResourceWithRange(1, "a", "g", 1, 2, 3, 4)
	lb, err := schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"0", "", "g", "0", "o", "t"}))
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), lb.Schedule(s.tc)[0].ResourceID())
	s.tc.RemoveResource(s.tc.GetResource(1))
	s.tc.AddLeaderResourceWithRange(2, "p", "r", 1, 2, 3, 4)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), lb.Schedule(s.tc)[0].ResourceID())
	s.tc.RemoveResource(s.tc.GetResource(2))
	s.tc.AddLeaderResourceWithRange(3, "u", "w", 1, 2, 3, 4)
	assert.NoError(t, err)
	assert.Empty(t, lb.Schedule(s.tc))
	s.tc.RemoveResource(s.tc.GetResource(3))
	s.tc.AddLeaderResourceWithRange(4, "", "", 1, 2, 3, 4)
	assert.NoError(t, err)
	assert.Empty(t, lb.Schedule(s.tc))
}

type testBalanceresourceScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testBalanceresourceScheduler) setup() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testBalanceresourceScheduler) tearDown() {
	s.cancel()
}

func (s *testBalanceresourceScheduler) checkReplica3(t *testing.T, tc *mockcluster.Cluster, opt *config.PersistOptions, sb schedule.Scheduler) {
	// container 1 has the largest resource score, so the balance scheduler tries to replace peer in container 1.
	tc.AddLabelsContainer(1, 16, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsContainer(2, 15, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.AddLabelsContainer(3, 14, map[string]string{"zone": "z1", "rack": "r2", "host": "h2"})

	tc.AddLeaderResource(1, 1, 2, 3)
	// This schedule try to replace peer in container 1, but we have no other containers.
	assert.Empty(t, sb.Schedule(tc))

	// container 4 has smaller resource score than container 2.
	tc.AddLabelsContainer(4, 2, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 2, 4)

	// container 5 has smaller resource score than container 1.
	tc.AddLabelsContainer(5, 2, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 1, 5)

	// container 6 has smaller resource score than container 5.
	tc.AddLabelsContainer(6, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 1, 6)

	// container 7 has smaller resource score with container 6.
	tc.AddLabelsContainer(7, 0, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 1, 7)

	// If container 7 is not available, will choose container 6.
	tc.SetContainerDown(7)
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 1, 6)

	// container 8 has smaller resource score than container 7, but the distinct score decrease.
	tc.AddLabelsContainer(8, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h3"})
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 1, 6)

	// Take down 4,5,6,7
	tc.SetContainerDown(4)
	tc.SetContainerDown(5)
	tc.SetContainerDown(6)
	tc.SetContainerDown(7)
	tc.SetContainerDown(8)

	// container 9 has different zone with other containers but larger resource score than container 1.
	tc.AddLabelsContainer(9, 20, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	assert.Empty(t, sb.Schedule(tc))
}

func (s *testBalanceresourceScheduler) checkReplica5(t *testing.T, tc *mockcluster.Cluster, opt *config.PersistOptions, sb schedule.Scheduler) {
	tc.AddLabelsContainer(1, 4, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsContainer(2, 5, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	tc.AddLabelsContainer(3, 6, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.AddLabelsContainer(4, 7, map[string]string{"zone": "z4", "rack": "r1", "host": "h1"})
	tc.AddLabelsContainer(5, 28, map[string]string{"zone": "z5", "rack": "r1", "host": "h1"})

	tc.AddLeaderResource(1, 1, 2, 3, 4, 5)

	// container 6 has smaller resource score.
	tc.AddLabelsContainer(6, 1, map[string]string{"zone": "z5", "rack": "r2", "host": "h1"})
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 5, 6)

	// container 7 has larger resource score and same distinct score with container 6.
	tc.AddLabelsContainer(7, 5, map[string]string{"zone": "z6", "rack": "r1", "host": "h1"})
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 5, 6)

	// container 1 has smaller resource score and higher distinct score.
	tc.AddLeaderResource(1, 2, 3, 4, 5, 6)
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 5, 1)

	// container 6 has smaller resource score and higher distinct score.
	tc.AddLabelsContainer(11, 29, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.AddLabelsContainer(12, 8, map[string]string{"zone": "z2", "rack": "r2", "host": "h1"})
	tc.AddLabelsContainer(13, 7, map[string]string{"zone": "z3", "rack": "r2", "host": "h1"})
	tc.AddLeaderResource(1, 2, 3, 11, 12, 13)
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 11, 6)
}

func (s *testBalanceresourceScheduler) checkReplacePendingResource(t *testing.T, tc *mockcluster.Cluster, sb schedule.Scheduler) {
	// container 1 has the largest resource score, so the balance scheduler try to replace peer in container 1.
	tc.AddLabelsContainer(1, 16, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsContainer(2, 7, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.AddLabelsContainer(3, 15, map[string]string{"zone": "z1", "rack": "r2", "host": "h2"})
	// container 4 has smaller resource score than container 1 and more better place than container 2.
	tc.AddLabelsContainer(4, 10, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})

	// set pending peer
	tc.AddLeaderResource(1, 1, 2, 3)
	tc.AddLeaderResource(2, 1, 2, 3)
	tc.AddLeaderResource(3, 2, 1, 3)
	resource := tc.GetResource(3)
	p, _ := resource.GetContainerPeer(1)
	resource = resource.Clone(core.WithPendingPeers([]metapb.Replica{p}))
	tc.PutResource(resource)

	assert.Equal(t, uint64(3), sb.Schedule(tc)[0].ResourceID())
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 1, 4)
}

func TestBalance(t *testing.T) {
	s := &testBalanceresourceScheduler{}
	s.setup()
	defer s.tearDown()

	opt := config.NewTestOptions()
	// TODO: enable placementrules
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(opt)
	tc.DisableJointConsensus()
	oc := schedule.NewOperatorController(s.ctx, tc, nil)

	sb, err := schedule.CreateScheduler(BalanceResourceType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceResourceType, []string{"0", "", ""}))
	assert.NoError(t, err)

	opt.SetMaxReplicas(1)

	// Add containers 1,2,3,4.
	tc.AddResourceContainer(1, 6)
	tc.AddResourceContainer(2, 8)
	tc.AddResourceContainer(3, 8)
	tc.AddResourceContainer(4, 16)
	// Add resource 1 with leader in container 4.
	tc.AddLeaderResource(1, 4)
	testutil.CheckTransferPeerWithLeaderTransfer(t, sb.Schedule(tc)[0], operator.OpKind(0), 4, 1)

	// Test stateFilter.
	tc.SetContainerOffline(1)
	tc.UpdateResourceCount(2, 6)

	// When container 1 is offline, it will be filtered,
	// container 2 becomes the container with least resources.
	testutil.CheckTransferPeerWithLeaderTransfer(t, sb.Schedule(tc)[0], operator.OpKind(0), 4, 2)
	opt.SetMaxReplicas(3)
	assert.Empty(t, sb.Schedule(tc))

	opt.SetMaxReplicas(1)
	assert.NotEmpty(t, sb.Schedule(tc))
}

func TestReplicas3(t *testing.T) {
	s := &testBalanceresourceScheduler{}
	s.setup()
	defer s.tearDown()

	opt := config.NewTestOptions()
	//TODO: enable placementrules
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "rack", "host"})
	tc.DisableJointConsensus()
	oc := schedule.NewOperatorController(s.ctx, tc, nil)

	sb, err := schedule.CreateScheduler(BalanceResourceType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceResourceType, []string{"0", "", ""}))
	assert.NoError(t, err)

	s.checkReplica3(t, tc, opt, sb)
	tc.SetEnablePlacementRules(true)
	s.checkReplica3(t, tc, opt, sb)
}

func TestReplicas5(t *testing.T) {
	s := &testBalanceresourceScheduler{}
	s.setup()
	defer s.tearDown()

	opt := config.NewTestOptions()
	//TODO: enable placementrules
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxReplicas(5)
	tc.SetLocationLabels([]string{"zone", "rack", "host"})

	tc.DisableJointConsensus()
	oc := schedule.NewOperatorController(s.ctx, tc, nil)

	sb, err := schedule.CreateScheduler(BalanceResourceType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceResourceType, []string{"0", "", ""}))
	assert.NoError(t, err)

	s.checkReplica5(t, tc, opt, sb)
	tc.SetEnablePlacementRules(true)
	s.checkReplica5(t, tc, opt, sb)
}

// TestBalance2 for corner case 1:
// 11 resources distributed across 5 containers.
//| resource_id | leader_container | follower_container | follower_container |
//|-------------|------------------|--------------------|--------------------|
//|     1       |       1          |        2           |       3            |
//|     2       |       1          |        2           |       3            |
//|     3       |       1          |        2           |       3            |
//|     4       |       1          |        2           |       3            |
//|     5       |       1          |        2           |       3            |
//|     6       |       1          |        2           |       3            |
//|     7       |       1          |        2           |       4            |
//|     8       |       1          |        2           |       4            |
//|     9       |       1          |        2           |       4            |
//|    10       |       1          |        4           |       5            |
//|    11       |       1          |        4           |       5            |
// and the space of last container 5 if very small, about 5 * resourceSize
// the source resource is more likely distributed in container[1, 2, 3].
func TestBalance1(t *testing.T) {
	s := &testBalanceresourceScheduler{}
	s.setup()
	defer s.tearDown()

	opt := config.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(opt)
	tc.DisableJointConsensus()
	tc.SetTolerantSizeRatio(1)
	tc.SetResourceScheduleLimit(1)
	tc.SetResourceScoreFormulaVersion("v1")
	oc := schedule.NewOperatorController(s.ctx, tc, nil)

	source := core.NewCachedResource(
		&metadata.TestResource{
			ResID: 1,
			Start: []byte(""),
			End:   []byte("a"),
			ResPeers: []metapb.Replica{
				{ID: 101, ContainerID: 1},
				{ID: 102, ContainerID: 2},
			},
		},
		&metapb.Replica{ID: 101, ContainerID: 1},
		core.SetApproximateSize(1),
		core.SetApproximateKeys(1),
	)
	target := core.NewCachedResource(
		&metadata.TestResource{
			ResID: 2,
			Start: []byte("a"),
			End:   []byte("t"),
			ResPeers: []metapb.Replica{
				{ID: 103, ContainerID: 1},
				{ID: 104, ContainerID: 4},
				{ID: 105, ContainerID: 5},
			},
		},
		&metapb.Replica{ID: 104, ContainerID: 4},
		core.SetApproximateSize(200),
		core.SetApproximateKeys(200),
	)

	sb, err := schedule.CreateScheduler(BalanceResourceType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceResourceType, []string{"0", "", ""}))
	assert.NoError(t, err)

	tc.AddResourceContainer(1, 11)
	tc.AddResourceContainer(2, 9)
	tc.AddResourceContainer(3, 6)
	tc.AddResourceContainer(4, 5)
	tc.AddResourceContainer(5, 2)
	tc.AddLeaderResource(1, 1, 2, 3)
	tc.AddLeaderResource(2, 1, 2, 3)

	// add two merge operator to let the count of opresource to 2.
	ops, err := operator.CreateMergeResourceOperator("merge-resource", tc, source, target, operator.OpMerge)
	assert.NoError(t, err)
	oc.SetOperator(ops[0])
	oc.SetOperator(ops[1])

	assert.True(t, sb.IsScheduleAllowed(tc))
	assert.NotNil(t, sb.Schedule(tc)[0])
	// if the space of container 5 is normal, we can balance resource to container 5
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 1, 5)

	// the used size of container 5 reach (highSpace, lowSpace)
	origin := tc.GetContainer(5)
	stats := origin.GetContainerStats()
	stats.Capacity = 50
	stats.Available = 28
	stats.UsedSize = 20
	container5 := origin.Clone(core.SetContainerStats(stats))
	tc.PutContainer(container5)

	// the scheduler first picks container 1 as source container,
	// and container 5 as target container, but cannot pass `shouldBalance`.
	// Then it will try container 4.
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 1, 4)
}

func TestContainerWeight(t *testing.T) {
	s := &testBalanceresourceScheduler{}
	s.setup()
	defer s.tearDown()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	// TODO: enable placementrules
	tc.SetPlacementRuleEnabled(false)
	tc.DisableJointConsensus()
	oc := schedule.NewOperatorController(s.ctx, tc, nil)

	sb, err := schedule.CreateScheduler(BalanceResourceType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceResourceType, []string{"0", "", ""}))
	assert.NoError(t, err)
	opt.SetMaxReplicas(1)

	tc.AddResourceContainer(1, 10)
	tc.AddResourceContainer(2, 10)
	tc.AddResourceContainer(3, 10)
	tc.AddResourceContainer(4, 10)
	tc.UpdateContainerResourceWeight(1, 0.5)
	tc.UpdateContainerResourceWeight(2, 0.9)
	tc.UpdateContainerResourceWeight(3, 1.0)
	tc.UpdateContainerResourceWeight(4, 2.0)

	tc.AddLeaderResource(1, 1)
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 1, 4)

	tc.UpdateResourceCount(4, 30)
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 1, 3)
}

func TestReplacePendingresource(t *testing.T) {
	s := &testBalanceresourceScheduler{}
	s.setup()
	defer s.tearDown()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "rack", "host"})
	tc.DisableJointConsensus()
	oc := schedule.NewOperatorController(s.ctx, tc, nil)

	sb, err := schedule.CreateScheduler(BalanceResourceType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceResourceType, []string{"0", "", ""}))
	assert.NoError(t, err)

	s.checkReplacePendingResource(t, tc, sb)
	tc.SetEnablePlacementRules(true)
	s.checkReplacePendingResource(t, tc, sb)
}

func TestOpInfluence(t *testing.T) {
	s := &testBalanceresourceScheduler{}
	s.setup()
	defer s.tearDown()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	//TODO: enable placementrules
	tc.SetEnablePlacementRules(false)
	tc.DisableJointConsensus()
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, tc.ID, tc, false /* no need to run */, nil)
	oc := schedule.NewOperatorController(s.ctx, tc, stream)
	sb, err := schedule.CreateScheduler(BalanceResourceType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceResourceType, []string{"0", "", ""}))
	assert.NoError(t, err)
	opt.SetMaxReplicas(1)
	// Add containers 1,2,3,4.
	tc.AddResourceContainerWithLeader(1, 2)
	tc.AddResourceContainerWithLeader(2, 8)
	tc.AddResourceContainerWithLeader(3, 8)
	tc.AddResourceContainerWithLeader(4, 16, 8)

	// add 8 leader resources to container 4 and move them to container 3
	// ensure container score without operator influence : container 4 > container 3
	// and container score with operator influence : container 3 > container 4
	for i := 1; i <= 8; i++ {
		id, _ := tc.AllocID()
		origin := tc.AddLeaderResource(id, 4)
		newPeer := metapb.Replica{ContainerID: 3, Role: metapb.ReplicaRole_Voter}
		op, _ := operator.CreateMovePeerOperator("balance-resource", tc, origin, operator.OpKind(0), 4, newPeer)
		assert.NotNil(t, op)
		oc.AddOperator(op)
	}
	testutil.CheckTransferPeerWithLeaderTransfer(t, sb.Schedule(tc)[0], operator.OpKind(0), 3, 1)
}

func TestShouldNotBalance(t *testing.T) {
	s := &testBalanceresourceScheduler{}
	s.setup()
	defer s.tearDown()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableJointConsensus()
	oc := schedule.NewOperatorController(s.ctx, tc, nil)
	sb, err := schedule.CreateScheduler(BalanceResourceType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceResourceType, []string{"0", "", ""}))
	assert.NoError(t, err)
	resource := tc.MockCachedResource(1, 0, []uint64{2, 3, 4}, nil, metapb.ResourceEpoch{})
	tc.PutResource(resource)
	operators := sb.Schedule(tc)
	if operators != nil {
		assert.Empty(t, operators)
	} else {
		assert.Empty(t, operators)
	}
}

func TestEmptyResource(t *testing.T) {
	s := &testBalanceresourceScheduler{}
	s.setup()
	defer s.tearDown()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableJointConsensus()
	oc := schedule.NewOperatorController(s.ctx, tc, nil)
	sb, err := schedule.CreateScheduler(BalanceResourceType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceResourceType, []string{"0", "", ""}))
	assert.NoError(t, err)
	tc.AddResourceContainer(1, 10)
	tc.AddResourceContainer(2, 9)
	tc.AddResourceContainer(3, 10)
	tc.AddResourceContainer(4, 10)
	res := core.NewCachedResource(
		&metadata.TestResource{
			ResID: 5,
			Start: []byte("a"),
			End:   []byte("b"),
			ResPeers: []metapb.Replica{
				{ID: 6, ContainerID: 1},
				{ID: 7, ContainerID: 3},
				{ID: 8, ContainerID: 4},
			},
		},
		&metapb.Replica{ID: 7, ContainerID: 3},
		core.SetApproximateSize(1),
		core.SetApproximateKeys(1),
	)
	tc.PutResource(res)
	operators := sb.Schedule(tc)
	assert.NotNil(t, operators)

	for i := uint64(10); i < 60; i++ {
		tc.PutResourceContainers(i, 1, 3, 4)
	}
	operators = sb.Schedule(tc)
	assert.Nil(t, operators)
}

func TestMerge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	//TODO: enable palcementrules
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(opt)
	tc.SetMergeScheduleLimit(1)
	stream := hbstream.NewTestHeartbeatStreams(ctx, tc.ID, tc, true /* need to run */, nil)
	oc := schedule.NewOperatorController(ctx, tc, stream)

	mb, err := schedule.CreateScheduler(RandomMergeType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(RandomMergeType, []string{"0", "", ""}))
	assert.NoError(t, err)

	tc.AddResourceContainer(1, 4)
	tc.AddLeaderResource(1, 1)
	tc.AddLeaderResource(2, 1)
	tc.AddLeaderResource(3, 1)
	tc.AddLeaderResource(4, 1)

	assert.True(t, mb.IsScheduleAllowed(tc))
	ops := mb.Schedule(tc)
	assert.Empty(t, ops) // resources are not fully replicated

	tc.SetMaxReplicas(1)
	ops = mb.Schedule(tc)
	assert.Equal(t, 2, len(ops))
	assert.NotEqual(t, 0, ops[0].Kind()&operator.OpMerge)
	assert.NotEqual(t, 0, ops[1].Kind()&operator.OpMerge)

	oc.AddWaitingOperator(ops...)
	assert.False(t, mb.IsScheduleAllowed(tc))
}

type testScatterRangeLeader struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testScatterRangeLeader) setup() {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testScatterRangeLeader) tearDown() {
	s.cancel()
}

func TestScatterRangeLeaderBalance(t *testing.T) {
	s := &testScatterRangeLeader{}
	s.setup()
	defer s.tearDown()

	opt := config.NewTestOptions()
	// TODO: enable palcementrules
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(opt)
	tc.DisableJointConsensus()
	tc.SetTolerantSizeRatio(2.5)
	// Add containers 1,2,3,4,5.
	tc.AddResourceContainer(1, 0)
	tc.AddResourceContainer(2, 0)
	tc.AddResourceContainer(3, 0)
	tc.AddResourceContainer(4, 0)
	tc.AddResourceContainer(5, 0)
	var (
		id        uint64
		resources []*metadata.TestResource
	)
	for i := 0; i < 50; i++ {
		peers := []metapb.Replica{
			{ID: id + 1, ContainerID: 1},
			{ID: id + 2, ContainerID: 2},
			{ID: id + 3, ContainerID: 3},
		}
		resources = append(resources, &metadata.TestResource{
			ResID:    id + 4,
			ResPeers: peers,
			Start:    []byte(fmt.Sprintf("s_%02d", i)),
			End:      []byte(fmt.Sprintf("s_%02d", i+1)),
		})
		id += 4
	}
	// empty case
	resources[49].End = []byte("")
	for _, meta := range resources {
		leader := rand.Intn(4) % 3
		resourceInfo := core.NewCachedResource(
			meta,
			&meta.Peers()[leader],
			core.SetApproximateKeys(96),
			core.SetApproximateSize(96),
		)

		tc.Resources.SetResource(resourceInfo)
	}
	for i := 0; i < 100; i++ {
		_, err := tc.AllocPeer(1)
		assert.NoError(t, err)
	}
	for i := 1; i <= 5; i++ {
		tc.UpdateContainerStatus(uint64(i))
	}
	oc := schedule.NewOperatorController(s.ctx, tc, nil)

	hb, err := schedule.CreateScheduler(ScatterRangeType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(ScatterRangeType, []string{"s_00", "s_50", "t"}))
	assert.NoError(t, err)
	limit := 0
	for {
		if limit > 100 {
			break
		}
		ops := hb.Schedule(tc)
		if ops == nil {
			limit++
			continue
		}
		schedule.ApplyOperator(tc, ops[0])
	}
	for i := 1; i <= 5; i++ {
		leaderCount := tc.Resources.GetContainerLeaderCount("", uint64(i))
		assert.True(t, leaderCount <= 12)
		resourceCount := tc.Resources.GetContainerResourceCount("", uint64(i))
		assert.True(t, resourceCount <= 32)
	}
}

func TestConcurrencyUpdateConfig(t *testing.T) {
	s := &testScatterRangeLeader{}
	s.setup()
	defer s.tearDown()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	oc := schedule.NewOperatorController(s.ctx, tc, nil)
	hb, err := schedule.CreateScheduler(ScatterRangeType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(ScatterRangeType, []string{"s_00", "s_50", "t"}))
	sche := hb.(*scatterRangeScheduler)
	assert.NoError(t, err)
	ch := make(chan struct{})
	args := []string{"test", "s_00", "s_99"}
	go func() {
		for {
			select {
			case <-ch:
				return
			default:
			}
			sche.config.BuildWithArgs(args)
			assert.Nil(t, sche.config.Persist())
		}
	}()
	for i := 0; i < 1000; i++ {
		sche.Schedule(tc)
	}
	ch <- struct{}{}
}

func TestBalanceWhenresourceNotHeartbeat(t *testing.T) {
	s := &testScatterRangeLeader{}
	s.setup()
	defer s.tearDown()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	// Add containers 1,2,3.
	tc.AddResourceContainer(1, 0)
	tc.AddResourceContainer(2, 0)
	tc.AddResourceContainer(3, 0)
	var (
		id        uint64
		resources []*metadata.TestResource
	)
	for i := 0; i < 10; i++ {
		peers := []metapb.Replica{
			{ID: id + 1, ContainerID: 1},
			{ID: id + 2, ContainerID: 2},
			{ID: id + 3, ContainerID: 3},
		}
		resources = append(resources, &metadata.TestResource{
			ResID:    id + 4,
			ResPeers: peers,
			Start:    []byte(fmt.Sprintf("s_%02d", i)),
			End:      []byte(fmt.Sprintf("s_%02d", i+1)),
		})
		id += 4
	}
	// empty case
	resources[9].End = []byte("")

	// To simulate server prepared,
	// container 1 contains 8 leader resource peers and leaders of 2 resources are unknown yet.
	for _, meta := range resources {
		var leader *metapb.Replica
		if meta.ID() < 8 {
			leader = &meta.Peers()[0]
		}
		resourceInfo := core.NewCachedResource(
			meta,
			leader,
			core.SetApproximateKeys(96),
			core.SetApproximateSize(96),
		)

		tc.Resources.SetResource(resourceInfo)
	}

	for i := 1; i <= 3; i++ {
		tc.UpdateContainerStatus(uint64(i))
	}

	oc := schedule.NewOperatorController(s.ctx, tc, nil)
	hb, err := schedule.CreateScheduler(ScatterRangeType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(ScatterRangeType, []string{"s_00", "s_09", "t"}))
	assert.NoError(t, err)

	limit := 0
	for {
		if limit > 100 {
			break
		}
		ops := hb.Schedule(tc)
		if ops == nil {
			limit++
			continue
		}
		schedule.ApplyOperator(tc, ops[0])
	}
}
