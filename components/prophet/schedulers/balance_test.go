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
	tc.SetShardScoreFormulaVersion("v1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	oc := schedule.NewOperatorController(ctx, nil, nil)
	// create a resource to control average resource size.
	tc.AddLeaderShard(1, 1, 2)

	for _, c := range tests {
		tc.AddLeaderStore(1, int(c.sourceCount))
		tc.AddLeaderStore(2, int(c.targetCount))
		source := tc.GetStore(1)
		target := tc.GetStore(2)
		resource := tc.GetShard(1).Clone(core.SetApproximateSize(c.resourceSize))
		tc.PutShard(resource)
		tc.SetLeaderSchedulePolicy(c.kind.String())
		kind := core.NewScheduleKind(metapb.ShardKind_LeaderKind, c.kind)
		shouldBalance, _, _ := shouldBalance(tc, source, target, resource, kind, oc.GetOpInfluence(tc), "")
		assert.Equal(t, c.expectedResult, shouldBalance)
	}

	for _, c := range tests {
		if c.kind.String() == core.BySize.String() {
			tc.AddShardStore(1, int(c.sourceCount))
			tc.AddShardStore(2, int(c.targetCount))
			source := tc.GetStore(1)
			target := tc.GetStore(2)
			resource := tc.GetShard(1).Clone(core.SetApproximateSize(c.resourceSize))
			tc.PutShard(resource)
			kind := core.NewScheduleKind(metapb.ShardKind_ReplicaKind, c.kind)
			shouldBalance, _, _ := shouldBalance(tc, source, target, resource, kind, oc.GetOpInfluence(tc), "")
			assert.Equal(t, c.expectedResult, shouldBalance)
		}
	}
}

func TestBalanceLimit(t *testing.T) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.AddLeaderStore(1, 10)
	tc.AddLeaderStore(2, 20)
	tc.AddLeaderStore(3, 30)

	// StandDeviation is sqrt((10^2+0+10^2)/3).
	assert.Equal(t, uint64(math.Sqrt(200.0/3.0)), adjustBalanceLimit("", tc, metapb.ShardKind_LeaderKind))

	tc.SetStoreOffline(1)
	// StandDeviation is sqrt((5^2+5^2)/2).
	assert.Equal(t, uint64(math.Sqrt(50.0/2.0)), adjustBalanceLimit("", tc, metapb.ShardKind_LeaderKind))
}

func TestTolerantRatio(t *testing.T) {
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	// create a resource to control average resource size.
	assert.NotNil(t, tc.AddLeaderShard(1, 1, 2))
	resourceSize := int64(96 * KB)
	resource := tc.GetShard(1).Clone(core.SetApproximateSize(resourceSize))

	tc.SetTolerantSizeRatio(0)
	assert.Equal(t, getTolerantShard(tc, resource, core.ScheduleKind{ShardKind: metapb.ShardKind_LeaderKind, Policy: core.ByCount}), int64(leaderTolerantSizeRatio))
	assert.Equal(t, getTolerantShard(tc, resource, core.ScheduleKind{ShardKind: metapb.ShardKind_LeaderKind, Policy: core.BySize}), int64(adjustTolerantRatio("", tc)*float64(resourceSize)))
	assert.Equal(t, getTolerantShard(tc, resource, core.ScheduleKind{ShardKind: metapb.ShardKind_ReplicaKind, Policy: core.ByCount}), int64(adjustTolerantRatio("", tc)*float64(resourceSize)))
	assert.Equal(t, getTolerantShard(tc, resource, core.ScheduleKind{ShardKind: metapb.ShardKind_ReplicaKind, Policy: core.BySize}), int64(adjustTolerantRatio("", tc)*float64(resourceSize)))

	tc.SetTolerantSizeRatio(10)
	assert.Equal(t, getTolerantShard(tc, resource, core.ScheduleKind{ShardKind: metapb.ShardKind_LeaderKind, Policy: core.ByCount}), int64(tc.GetScheduleConfig().TolerantSizeRatio))
	assert.Equal(t, getTolerantShard(tc, resource, core.ScheduleKind{ShardKind: metapb.ShardKind_LeaderKind, Policy: core.BySize}), int64(adjustTolerantRatio("", tc)*float64(resourceSize)))
	assert.Equal(t, getTolerantShard(tc, resource, core.ScheduleKind{ShardKind: metapb.ShardKind_ReplicaKind, Policy: core.ByCount}), int64(adjustTolerantRatio("", tc)*float64(resourceSize)))
	assert.Equal(t, getTolerantShard(tc, resource, core.ScheduleKind{ShardKind: metapb.ShardKind_ReplicaKind, Policy: core.BySize}), int64(adjustTolerantRatio("", tc)*float64(resourceSize)))
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
	s.tc.AddLeaderStore(1, 1)
	s.tc.AddLeaderStore(2, 0)
	s.tc.AddLeaderStore(3, 0)
	s.tc.AddLeaderStore(4, 0)
	s.tc.AddLeaderShard(1, 1, 2, 3, 4)
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
	s.tc.AddLeaderShard(1, 4, 1, 2, 3)
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
	s.tc.AddLeaderStore(1, 10, 10000*MB)
	s.tc.AddLeaderStore(2, 10, 100*MB)
	s.tc.AddLeaderStore(3, 10, 100*MB)
	s.tc.AddLeaderStore(4, 10, 100*MB)
	s.tc.AddLeaderShard(1, 1, 2, 3, 4)
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
	s.tc.AddLeaderStore(1, 14, 100)
	s.tc.AddLeaderStore(2, 10, 100)
	s.tc.AddLeaderStore(3, 10, 100)
	s.tc.AddLeaderStore(4, 10, 100)
	s.tc.AddLeaderShard(1, 1, 2, 3, 4)

	assert.Equal(t, core.ByCount.String(), s.tc.GetScheduleConfig().LeaderSchedulePolicy) // default by count
	assert.Nil(t, s.schedule())
	assert.Equal(t, 14, s.tc.GetStore(1).GetLeaderCount(""))
	s.tc.AddLeaderStore(1, 15, 100)
	assert.Equal(t, 15, s.tc.GetStore(1).GetLeaderCount(""))
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
	s.tc.AddLeaderStore(1, 7)
	s.tc.AddLeaderStore(2, 8)
	s.tc.AddLeaderStore(3, 9)
	s.tc.AddLeaderStore(4, 14)
	s.tc.AddLeaderShard(1, 4, 1, 2, 3)
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
	s.tc.AddLeaderShard(1, 4, 1, 2, 3)
	assert.Nil(t, s.schedule())
}

func TestTransferLeaderOut(t *testing.T) {
	s := &testBalanceLeaderScheduler{}
	s.setup(t)
	defer s.tearDown()

	// containers:     1    2    3    4
	// Leaders:    7    8    9   12
	s.tc.AddLeaderStore(1, 7)
	s.tc.AddLeaderStore(2, 8)
	s.tc.AddLeaderStore(3, 9)
	s.tc.AddLeaderStore(4, 12)
	s.tc.SetTolerantSizeRatio(0.1)
	for i := uint64(1); i <= 7; i++ {
		s.tc.AddLeaderShard(i, 4, 1, 2, 3)
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
			if _, ok := resources[op.ShardID()]; !ok {
				s.oc.SetOperator(op)
				resources[op.ShardID()] = struct{}{}
				tr := op.Step(0).(operator.TransferLeader)
				assert.Equal(t, uint64(4), tr.FromStore)
				targets[tr.ToStore]--
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
	s.tc.AddLeaderStore(1, 1)
	s.tc.AddLeaderStore(2, 2)
	s.tc.AddLeaderStore(3, 3)
	s.tc.AddLeaderStore(4, 16)
	s.tc.AddLeaderShard(1, 4, 1, 2, 3)

	testutil.CheckTransferLeader(t, s.schedule()[0], operator.OpKind(0), 4, 1)
	// Test stateFilter.
	// if container 4 is offline, we should consider it
	// because it still provides services
	s.tc.SetStoreOffline(4)
	testutil.CheckTransferLeader(t, s.schedule()[0], operator.OpKind(0), 4, 1)
	// If container 1 is down, it will be filtered,
	// container 2 becomes the container with least leaders.
	s.tc.SetStoreDown(1)
	testutil.CheckTransferLeader(t, s.schedule()[0], operator.OpKind(0), 4, 2)

	// Test healthFilter.
	// If container 2 is busy, it will be filtered,
	// container 3 becomes the container with least leaders.
	s.tc.SetStoreBusy(2, true)
	testutil.CheckTransferLeader(t, s.schedule()[0], operator.OpKind(0), 4, 3)

	// Test disconnectFilter.
	// If container 3 is disconnected, no operator can be created.
	s.tc.SetStoreDisconnect(3)
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
		s.tc.AddLeaderStore(i, 10)
	}
	s.tc.UpdateStoreLeaderWeight(1, 0.5)
	s.tc.UpdateStoreLeaderWeight(2, 0.9)
	s.tc.UpdateStoreLeaderWeight(3, 1)
	s.tc.UpdateStoreLeaderWeight(4, 2)
	s.tc.AddLeaderShard(1, 1, 2, 3, 4)
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
	s.tc.AddLeaderStore(1, 20, 600*MB)
	s.tc.AddLeaderStore(2, 66, 200*MB)
	s.tc.AddLeaderStore(3, 6, 20*MB)
	s.tc.AddLeaderStore(4, 20, 1*MB)
	s.tc.AddLeaderShard(1, 2, 1, 3, 4)
	s.tc.AddLeaderShard(2, 1, 2, 3, 4)
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
	s.tc.AddLeaderStore(1, 1)
	s.tc.AddLeaderStore(2, 2)
	s.tc.AddLeaderStore(3, 3)
	s.tc.AddLeaderStore(4, 16)
	s.tc.AddLeaderShard(1, 4, 2, 3)
	s.tc.AddLeaderShard(2, 3, 1, 2)
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
	s.tc.AddLeaderStore(2, 2)
	s.tc.AddLeaderShard(1, 3, 2, 4)
	s.tc.AddLeaderShard(2, 1, 2, 3)
	// No leader in container16, no follower in container1. Now source and target are container3 and container2.
	testutil.CheckTransferLeader(t, s.schedule()[0], operator.OpKind(0), 3, 2)

	// containers:     1    2    3    4
	// Leaders:    9    10   10   11
	// resource1:    -    F    F    L
	// resource2:    L    F    F    -
	for i := uint64(1); i <= 4; i++ {
		s.tc.AddLeaderStore(i, 10)
	}
	s.tc.AddLeaderShard(1, 4, 2, 3)
	s.tc.AddLeaderShard(2, 1, 2, 3)
	// The cluster is balanced.
	assert.Empty(t, s.schedule())
	assert.Empty(t, s.schedule())

	// container3's leader drops:
	// containers:     1    2    3    4
	// Leaders:    11   13   0    16
	// resource1:    -    F    F    L
	// resource2:    L    F    F    -
	s.tc.AddLeaderStore(1, 11)
	s.tc.AddLeaderStore(2, 13)
	s.tc.AddLeaderStore(3, 0)
	s.tc.AddLeaderStore(4, 16)
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
		s.tc.AddLeaderStore(i, 10)
	}
	s.tc.UpdateStoreLeaderWeight(1, 0.5)
	s.tc.UpdateStoreLeaderWeight(2, 0.9)
	s.tc.UpdateStoreLeaderWeight(3, 1)
	s.tc.UpdateStoreLeaderWeight(4, 2)
	s.tc.AddLeaderShardWithRange(1, "a", "g", 1, 2, 3, 4)
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

	// Stores:     1       2       3       4
	// Leaders:    10      10      10      10
	// Weight:     0.5     0.9     1       2
	// resource1:    L       F       F       F

	for i := uint64(1); i <= 4; i++ {
		s.tc.AddLeaderStore(i, 10)
	}
	s.tc.UpdateStoreLeaderWeight(1, 0.5)
	s.tc.UpdateStoreLeaderWeight(2, 0.9)
	s.tc.UpdateStoreLeaderWeight(3, 1)
	s.tc.UpdateStoreLeaderWeight(4, 2)
	s.tc.AddLeaderShardWithRange(1, "a", "g", 1, 2, 3, 4)
	lb, err := schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"0", "", "g", "0", "o", "t"}))
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), lb.Schedule(s.tc)[0].ShardID())
	s.tc.RemoveShard(s.tc.GetShard(1))
	s.tc.AddLeaderShardWithRange(2, "p", "r", 1, 2, 3, 4)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), lb.Schedule(s.tc)[0].ShardID())
	s.tc.RemoveShard(s.tc.GetShard(2))
	s.tc.AddLeaderShardWithRange(3, "u", "w", 1, 2, 3, 4)
	assert.NoError(t, err)
	assert.Empty(t, lb.Schedule(s.tc))
	s.tc.RemoveShard(s.tc.GetShard(3))
	s.tc.AddLeaderShardWithRange(4, "", "", 1, 2, 3, 4)
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
	tc.AddLabelsStore(1, 16, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 15, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.AddLabelsStore(3, 14, map[string]string{"zone": "z1", "rack": "r2", "host": "h2"})

	tc.AddLeaderShard(1, 1, 2, 3)
	// This schedule try to replace peer in container 1, but we have no other containers.
	assert.Empty(t, sb.Schedule(tc))

	// container 4 has smaller resource score than container 2.
	tc.AddLabelsStore(4, 2, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 2, 4)

	// container 5 has smaller resource score than container 1.
	tc.AddLabelsStore(5, 2, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 1, 5)

	// container 6 has smaller resource score than container 5.
	tc.AddLabelsStore(6, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 1, 6)

	// container 7 has smaller resource score with container 6.
	tc.AddLabelsStore(7, 0, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 1, 7)

	// If container 7 is not available, will choose container 6.
	tc.SetStoreDown(7)
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 1, 6)

	// container 8 has smaller resource score than container 7, but the distinct score decrease.
	tc.AddLabelsStore(8, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h3"})
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 1, 6)

	// Take down 4,5,6,7
	tc.SetStoreDown(4)
	tc.SetStoreDown(5)
	tc.SetStoreDown(6)
	tc.SetStoreDown(7)
	tc.SetStoreDown(8)

	// container 9 has different zone with other containers but larger resource score than container 1.
	tc.AddLabelsStore(9, 20, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	assert.Empty(t, sb.Schedule(tc))
}

func (s *testBalanceresourceScheduler) checkReplica5(t *testing.T, tc *mockcluster.Cluster, opt *config.PersistOptions, sb schedule.Scheduler) {
	tc.AddLabelsStore(1, 4, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 5, map[string]string{"zone": "z2", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(3, 6, map[string]string{"zone": "z3", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(4, 7, map[string]string{"zone": "z4", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(5, 28, map[string]string{"zone": "z5", "rack": "r1", "host": "h1"})

	tc.AddLeaderShard(1, 1, 2, 3, 4, 5)

	// container 6 has smaller resource score.
	tc.AddLabelsStore(6, 1, map[string]string{"zone": "z5", "rack": "r2", "host": "h1"})
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 5, 6)

	// container 7 has larger resource score and same distinct score with container 6.
	tc.AddLabelsStore(7, 5, map[string]string{"zone": "z6", "rack": "r1", "host": "h1"})
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 5, 6)

	// container 1 has smaller resource score and higher distinct score.
	tc.AddLeaderShard(1, 2, 3, 4, 5, 6)
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 5, 1)

	// container 6 has smaller resource score and higher distinct score.
	tc.AddLabelsStore(11, 29, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.AddLabelsStore(12, 8, map[string]string{"zone": "z2", "rack": "r2", "host": "h1"})
	tc.AddLabelsStore(13, 7, map[string]string{"zone": "z3", "rack": "r2", "host": "h1"})
	tc.AddLeaderShard(1, 2, 3, 11, 12, 13)
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 11, 6)
}

func (s *testBalanceresourceScheduler) checkReplacePendingShard(t *testing.T, tc *mockcluster.Cluster, sb schedule.Scheduler) {
	// container 1 has the largest resource score, so the balance scheduler try to replace peer in container 1.
	tc.AddLabelsStore(1, 16, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	tc.AddLabelsStore(2, 7, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	tc.AddLabelsStore(3, 15, map[string]string{"zone": "z1", "rack": "r2", "host": "h2"})
	// container 4 has smaller resource score than container 1 and more better place than container 2.
	tc.AddLabelsStore(4, 10, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})

	// set pending peer
	tc.AddLeaderShard(1, 1, 2, 3)
	tc.AddLeaderShard(2, 1, 2, 3)
	tc.AddLeaderShard(3, 2, 1, 3)
	resource := tc.GetShard(3)
	p, _ := resource.GetStorePeer(1)
	resource = resource.Clone(core.WithPendingPeers([]metapb.Replica{p}))
	tc.PutShard(resource)

	assert.Equal(t, uint64(3), sb.Schedule(tc)[0].ShardID())
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

	sb, err := schedule.CreateScheduler(BalanceShardType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceShardType, []string{"0", "", ""}))
	assert.NoError(t, err)

	opt.SetMaxReplicas(1)

	// Add containers 1,2,3,4.
	tc.AddShardStore(1, 6)
	tc.AddShardStore(2, 8)
	tc.AddShardStore(3, 8)
	tc.AddShardStore(4, 16)
	// Add resource 1 with leader in container 4.
	tc.AddLeaderShard(1, 4)
	testutil.CheckTransferPeerWithLeaderTransfer(t, sb.Schedule(tc)[0], operator.OpKind(0), 4, 1)

	// Test stateFilter.
	tc.SetStoreOffline(1)
	tc.UpdateShardCount(2, 6)

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

	sb, err := schedule.CreateScheduler(BalanceShardType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceShardType, []string{"0", "", ""}))
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

	sb, err := schedule.CreateScheduler(BalanceShardType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceShardType, []string{"0", "", ""}))
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
	tc.SetShardScheduleLimit(1)
	tc.SetShardScoreFormulaVersion("v1")
	oc := schedule.NewOperatorController(s.ctx, tc, nil)

	source := core.NewCachedShard(
		&metadata.TestShard{
			ResID: 1,
			Start: []byte(""),
			End:   []byte("a"),
			ResPeers: []metapb.Replica{
				{ID: 101, StoreID: 1},
				{ID: 102, StoreID: 2},
			},
		},
		&metapb.Replica{ID: 101, StoreID: 1},
		core.SetApproximateSize(1),
		core.SetApproximateKeys(1),
	)
	target := core.NewCachedShard(
		&metadata.TestShard{
			ResID: 2,
			Start: []byte("a"),
			End:   []byte("t"),
			ResPeers: []metapb.Replica{
				{ID: 103, StoreID: 1},
				{ID: 104, StoreID: 4},
				{ID: 105, StoreID: 5},
			},
		},
		&metapb.Replica{ID: 104, StoreID: 4},
		core.SetApproximateSize(200),
		core.SetApproximateKeys(200),
	)

	sb, err := schedule.CreateScheduler(BalanceShardType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceShardType, []string{"0", "", ""}))
	assert.NoError(t, err)

	tc.AddShardStore(1, 11)
	tc.AddShardStore(2, 9)
	tc.AddShardStore(3, 6)
	tc.AddShardStore(4, 5)
	tc.AddShardStore(5, 2)
	tc.AddLeaderShard(1, 1, 2, 3)
	tc.AddLeaderShard(2, 1, 2, 3)

	// add two merge operator to let the count of opresource to 2.
	ops, err := operator.CreateMergeShardOperator("merge-resource", tc, source, target, operator.OpMerge)
	assert.NoError(t, err)
	oc.SetOperator(ops[0])
	oc.SetOperator(ops[1])

	assert.True(t, sb.IsScheduleAllowed(tc))
	assert.NotNil(t, sb.Schedule(tc)[0])
	// if the space of container 5 is normal, we can balance resource to container 5
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 1, 5)

	// the used size of container 5 reach (highSpace, lowSpace)
	origin := tc.GetStore(5)
	stats := origin.GetStoreStats()
	stats.Capacity = 50
	stats.Available = 28
	stats.UsedSize = 20
	container5 := origin.Clone(core.SetStoreStats(stats))
	tc.PutStore(container5)

	// the scheduler first picks container 1 as source container,
	// and container 5 as target container, but cannot pass `shouldBalance`.
	// Then it will try container 4.
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 1, 4)
}

func TestStoreWeight(t *testing.T) {
	s := &testBalanceresourceScheduler{}
	s.setup()
	defer s.tearDown()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	// TODO: enable placementrules
	tc.SetPlacementRuleEnabled(false)
	tc.DisableJointConsensus()
	oc := schedule.NewOperatorController(s.ctx, tc, nil)

	sb, err := schedule.CreateScheduler(BalanceShardType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceShardType, []string{"0", "", ""}))
	assert.NoError(t, err)
	opt.SetMaxReplicas(1)

	tc.AddShardStore(1, 10)
	tc.AddShardStore(2, 10)
	tc.AddShardStore(3, 10)
	tc.AddShardStore(4, 10)
	tc.UpdateStoreShardWeight(1, 0.5)
	tc.UpdateStoreShardWeight(2, 0.9)
	tc.UpdateStoreShardWeight(3, 1.0)
	tc.UpdateStoreShardWeight(4, 2.0)

	tc.AddLeaderShard(1, 1)
	testutil.CheckTransferPeer(t, sb.Schedule(tc)[0], operator.OpKind(0), 1, 4)

	tc.UpdateShardCount(4, 30)
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

	sb, err := schedule.CreateScheduler(BalanceShardType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceShardType, []string{"0", "", ""}))
	assert.NoError(t, err)

	s.checkReplacePendingShard(t, tc, sb)
	tc.SetEnablePlacementRules(true)
	s.checkReplacePendingShard(t, tc, sb)
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
	sb, err := schedule.CreateScheduler(BalanceShardType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceShardType, []string{"0", "", ""}))
	assert.NoError(t, err)
	opt.SetMaxReplicas(1)
	// Add containers 1,2,3,4.
	tc.AddShardStoreWithLeader(1, 2)
	tc.AddShardStoreWithLeader(2, 8)
	tc.AddShardStoreWithLeader(3, 8)
	tc.AddShardStoreWithLeader(4, 16, 8)

	// add 8 leader resources to container 4 and move them to container 3
	// ensure container score without operator influence : container 4 > container 3
	// and container score with operator influence : container 3 > container 4
	for i := 1; i <= 8; i++ {
		id, _ := tc.AllocID()
		origin := tc.AddLeaderShard(id, 4)
		newPeer := metapb.Replica{StoreID: 3, Role: metapb.ReplicaRole_Voter}
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
	sb, err := schedule.CreateScheduler(BalanceShardType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceShardType, []string{"0", "", ""}))
	assert.NoError(t, err)
	resource := tc.MockCachedShard(1, 0, []uint64{2, 3, 4}, nil, metapb.ShardEpoch{})
	tc.PutShard(resource)
	operators := sb.Schedule(tc)
	if operators != nil {
		assert.Empty(t, operators)
	} else {
		assert.Empty(t, operators)
	}
}

func TestEmptyShard(t *testing.T) {
	s := &testBalanceresourceScheduler{}
	s.setup()
	defer s.tearDown()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableJointConsensus()
	oc := schedule.NewOperatorController(s.ctx, tc, nil)
	sb, err := schedule.CreateScheduler(BalanceShardType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceShardType, []string{"0", "", ""}))
	assert.NoError(t, err)
	tc.AddShardStore(1, 10)
	tc.AddShardStore(2, 9)
	tc.AddShardStore(3, 10)
	tc.AddShardStore(4, 10)
	res := core.NewCachedShard(
		&metadata.TestShard{
			ResID: 5,
			Start: []byte("a"),
			End:   []byte("b"),
			ResPeers: []metapb.Replica{
				{ID: 6, StoreID: 1},
				{ID: 7, StoreID: 3},
				{ID: 8, StoreID: 4},
			},
		},
		&metapb.Replica{ID: 7, StoreID: 3},
		core.SetApproximateSize(1),
		core.SetApproximateKeys(1),
	)
	tc.PutShard(res)
	operators := sb.Schedule(tc)
	assert.NotNil(t, operators)

	for i := uint64(10); i < 60; i++ {
		tc.PutShardStores(i, 1, 3, 4)
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

	tc.AddShardStore(1, 4)
	tc.AddLeaderShard(1, 1)
	tc.AddLeaderShard(2, 1)
	tc.AddLeaderShard(3, 1)
	tc.AddLeaderShard(4, 1)

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
	tc.AddShardStore(1, 0)
	tc.AddShardStore(2, 0)
	tc.AddShardStore(3, 0)
	tc.AddShardStore(4, 0)
	tc.AddShardStore(5, 0)
	var (
		id        uint64
		resources []*metadata.TestShard
	)
	for i := 0; i < 50; i++ {
		peers := []metapb.Replica{
			{ID: id + 1, StoreID: 1},
			{ID: id + 2, StoreID: 2},
			{ID: id + 3, StoreID: 3},
		}
		resources = append(resources, &metadata.TestShard{
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
		resourceInfo := core.NewCachedShard(
			meta,
			&meta.Peers()[leader],
			core.SetApproximateKeys(96),
			core.SetApproximateSize(96),
		)

		tc.Shards.SetShard(resourceInfo)
	}
	for i := 0; i < 100; i++ {
		_, err := tc.AllocPeer(1)
		assert.NoError(t, err)
	}
	for i := 1; i <= 5; i++ {
		tc.UpdateStoreStatus(uint64(i))
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
		leaderCount := tc.Shards.GetStoreLeaderCount("", uint64(i))
		assert.True(t, leaderCount <= 12)
		resourceCount := tc.Shards.GetStoreShardCount("", uint64(i))
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
	tc.AddShardStore(1, 0)
	tc.AddShardStore(2, 0)
	tc.AddShardStore(3, 0)
	var (
		id        uint64
		resources []*metadata.TestShard
	)
	for i := 0; i < 10; i++ {
		peers := []metapb.Replica{
			{ID: id + 1, StoreID: 1},
			{ID: id + 2, StoreID: 2},
			{ID: id + 3, StoreID: 3},
		}
		resources = append(resources, &metadata.TestShard{
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
		resourceInfo := core.NewCachedShard(
			meta,
			leader,
			core.SetApproximateKeys(96),
			core.SetApproximateSize(96),
		)

		tc.Shards.SetShard(resourceInfo)
	}

	for i := 1; i <= 3; i++ {
		tc.UpdateStoreStatus(uint64(i))
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
