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
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/statistics"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/testutil"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestShuffle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)

	sl, err := schedule.CreateScheduler(ShuffleLeaderType, schedule.NewOperatorController(ctx, nil, nil), storage.NewTestStorage(), schedule.ConfigSliceDecoder(ShuffleLeaderType, []string{"", ""}))
	assert.NoError(t, err)
	assert.Empty(t, sl.Schedule(tc))

	// Add containers 1,2,3,4
	tc.AddLeaderStore(1, 6)
	tc.AddLeaderStore(2, 7)
	tc.AddLeaderStore(3, 8)
	tc.AddLeaderStore(4, 9)
	// Add resources 1,2,3,4 with leaders in containers 1,2,3,4
	tc.AddLeaderShard(1, 1, 2, 3, 4)
	tc.AddLeaderShard(2, 2, 3, 4, 1)
	tc.AddLeaderShard(3, 3, 4, 1, 2)
	tc.AddLeaderShard(4, 4, 1, 2, 3)

	for i := 0; i < 4; i++ {
		op := sl.Schedule(tc)
		assert.NotNil(t, op)
		assert.Equal(t, operator.OpLeader|operator.OpAdmin, op[0].Kind())
	}
}

func TestRejectLeader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := config.NewTestOptions()
	opts.SetLabelPropertyConfig(config.LabelPropertyConfig{
		opt.RejectLeader: {{Key: "noleader", Value: "true"}},
	})
	tc := mockcluster.NewCluster(opts)

	// Add 3 containers 1,2,3.
	tc.AddLabelsStore(1, 1, map[string]string{"noleader": "true"})
	tc.UpdateLeaderCount(1, 1)
	tc.AddLeaderStore(2, 10)
	tc.AddLeaderStore(3, 0)
	// Add 2 resources with leader on 1 and 2.
	tc.AddLeaderShard(1, 1, 2, 3)
	tc.AddLeaderShard(2, 2, 1, 3)

	// The label scheduler transfers leader out of container1.
	oc := schedule.NewOperatorController(ctx, tc, nil)
	sl, err := schedule.CreateScheduler(LabelType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(LabelType, []string{"", ""}))
	assert.NoError(t, err)
	op := sl.Schedule(tc)
	testutil.CheckTransferLeaderFrom(t, op[0], operator.OpLeader, 1)

	// If container3 is disconnected, transfer leader to container 2.
	tc.SetStoreDisconnect(3)
	op = sl.Schedule(tc)
	testutil.CheckTransferLeader(t, op[0], operator.OpLeader, 1, 2)

	// As container3 is disconnected, container1 rejects leader. Balancer will not create
	// any operators.
	bs, err := schedule.CreateScheduler(BalanceLeaderType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"", ""}))
	assert.NoError(t, err)
	op = bs.Schedule(tc)
	assert.Nil(t, op)

	// Can't evict leader from container2, neither.
	el, err := schedule.CreateScheduler(EvictLeaderType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(EvictLeaderType, []string{"2"}))
	assert.NoError(t, err)
	op = el.Schedule(tc)
	assert.Nil(t, op)

	// If the peer on container3 is pending, not transfer to container3 neither.
	tc.SetStoreUP(3)
	resource := tc.Shards.GetShard(1)
	for _, p := range resource.Meta.GetReplicas() {
		if p.GetStoreID() == 3 {
			resource = resource.Clone(core.WithPendingPeers(append(resource.GetPendingPeers(), p)))
			break
		}
	}
	tc.Shards.AddShard(resource)
	op = sl.Schedule(tc)
	testutil.CheckTransferLeader(t, op[0], operator.OpLeader, 1, 2)
}

func TestShuffleHotBalance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetMaxReplicas(3)
	tc.SetLocationLabels([]string{"zone", "host"})
	tc.DisableJointConsensus()
	hb, err := schedule.CreateScheduler(ShuffleHotShardType, schedule.NewOperatorController(ctx, tc, nil), storage.NewTestStorage(), schedule.ConfigSliceDecoder("shuffle-hot-resource", []string{"", ""}))
	assert.NoError(t, err)

	checkBalance(t, tc, opt, hb)
	tc.SetEnablePlacementRules(true)
	checkBalance(t, tc, opt, hb)
}

func TestAbnormalReplica(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.SetLeaderScheduleLimit(0)
	hb, err := schedule.CreateScheduler(HotReadShardType, schedule.NewOperatorController(ctx, tc, nil), storage.NewTestStorage(), nil)
	assert.NoError(t, err)

	tc.AddShardStore(1, 3)
	tc.AddShardStore(2, 2)
	tc.AddShardStore(3, 2)

	// Report container read bytes.
	tc.UpdateStorageReadBytes(1, 7.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(2, 4.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageReadBytes(3, 4.5*MB*statistics.StoreHeartBeatReportInterval)

	tc.AddLeaderShardWithReadInfo(1, 1, 512*KB*statistics.ShardHeartBeatReportInterval, 0, statistics.ShardHeartBeatReportInterval, []uint64{2})
	tc.AddLeaderShardWithReadInfo(2, 2, 512*KB*statistics.ShardHeartBeatReportInterval, 0, statistics.ShardHeartBeatReportInterval, []uint64{1, 3})
	tc.AddLeaderShardWithReadInfo(3, 1, 512*KB*statistics.ShardHeartBeatReportInterval, 0, statistics.ShardHeartBeatReportInterval, []uint64{2, 3})
	tc.SetHotShardCacheHitsThreshold(0)
	assert.True(t, tc.IsShardHot(tc.GetShard(1)))
	assert.Empty(t, hb.Schedule(tc))
}

func TestEvictLeader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)

	// Add containers 1, 2, 3
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	// Add resources 1, 2, 3 with leaders in containers 1, 2, 3
	tc.AddLeaderShard(1, 1, 2)
	tc.AddLeaderShard(2, 2, 1)
	tc.AddLeaderShard(3, 3, 1)

	sl, err := schedule.CreateScheduler(EvictLeaderType, schedule.NewOperatorController(ctx, tc, nil), storage.NewTestStorage(), schedule.ConfigSliceDecoder(EvictLeaderType, []string{"1"}))
	assert.NoError(t, err)
	assert.True(t, sl.IsScheduleAllowed(tc))
	op := sl.Schedule(tc)
	testutil.CheckTransferLeader(t, op[0], operator.OpLeader, 1, 2)
}

func TestShuffleresource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)

	sl, err := schedule.CreateScheduler(ShuffleShardType, schedule.NewOperatorController(ctx, tc, nil), storage.NewTestStorage(), schedule.ConfigSliceDecoder(ShuffleShardType, []string{"", ""}))
	assert.NoError(t, err)
	assert.True(t, sl.IsScheduleAllowed(tc))
	assert.Empty(t, sl.Schedule(tc))

	// Add containers 1, 2, 3, 4
	tc.AddShardStore(1, 6)
	tc.AddShardStore(2, 7)
	tc.AddShardStore(3, 8)
	tc.AddShardStore(4, 9)
	// Add resources 1, 2, 3, 4 with leaders in containers 1,2,3,4
	tc.AddLeaderShard(1, 1, 2, 3)
	tc.AddLeaderShard(2, 2, 3, 4)
	tc.AddLeaderShard(3, 3, 4, 1)
	tc.AddLeaderShard(4, 4, 1, 2)

	for i := 0; i < 4; i++ {
		op := sl.Schedule(tc)
		assert.NotNil(t, op)
		assert.Equal(t, operator.OpShard|operator.OpAdmin, op[0].Kind())
	}
}

func TestRole(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	tc.DisableJointConsensus()

	// update rule to 1leader+1follower+1learner
	tc.SetEnablePlacementRules(true)
	tc.RuleManager.SetRule(&placement.Rule{
		GroupID: "prophet",
		ID:      "default",
		Role:    placement.Voter,
		Count:   2,
	})
	tc.RuleManager.SetRule(&placement.Rule{
		GroupID: "prophet",
		ID:      "learner",
		Role:    placement.Learner,
		Count:   1,
	})

	// Add containers 1, 2, 3, 4
	tc.AddShardStore(1, 6)
	tc.AddShardStore(2, 7)
	tc.AddShardStore(3, 8)
	tc.AddShardStore(4, 9)

	// Put a resource with 1leader + 1follower + 1learner
	peers := []metapb.Replica{
		{ID: 1, StoreID: 1},
		{ID: 2, StoreID: 2},
		{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Learner},
	}
	resource := core.NewCachedShard(metapb.Shard{ID: 1, Epoch: metapb.ShardEpoch{ConfigVer: 1, Generation: 1}, Replicas: peers}, &peers[0])
	tc.PutShard(resource)

	sl, err := schedule.CreateScheduler(ShuffleShardType, schedule.NewOperatorController(ctx, tc, nil), storage.NewTestStorage(), schedule.ConfigSliceDecoder(ShuffleShardType, []string{"", ""}))
	assert.NoError(t, err)

	conf := sl.(*shuffleShardScheduler).conf
	conf.Roles = []string{"follower"}
	ops := sl.Schedule(tc)
	assert.Equal(t, 1, len(ops))
	testutil.CheckTransferPeer(t, ops[0], operator.OpKind(0), 2, 4) // transfer follower
	conf.Roles = []string{"learner"}
	ops = sl.Schedule(tc)
	assert.Equal(t, 1, len(ops))
	testutil.CheckTransferLearner(t, ops[0], operator.OpShard, 3, 4) // transfer learner
}

func TestSpecialUseHotresource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	oc := schedule.NewOperatorController(ctx, tc, nil)
	storage := storage.NewTestStorage()
	cd := schedule.ConfigSliceDecoder(BalanceShardType, []string{"", ""})
	bs, err := schedule.CreateScheduler(BalanceShardType, oc, storage, cd)
	assert.NoError(t, err)
	hs, err := schedule.CreateScheduler(HotWriteShardType, oc, storage, cd)
	assert.NoError(t, err)

	tc.SetHotShardCacheHitsThreshold(0)
	tc.DisableJointConsensus()
	tc.AddShardStore(1, 10)
	tc.AddShardStore(2, 4)
	tc.AddShardStore(3, 2)
	tc.AddShardStore(4, 0)
	tc.AddShardStore(5, 10)
	tc.AddLeaderShard(1, 1, 2, 3)
	tc.AddLeaderShard(2, 1, 2, 3)
	tc.AddLeaderShard(3, 1, 2, 3)
	tc.AddLeaderShard(4, 1, 2, 3)
	tc.AddLeaderShard(5, 1, 2, 3)

	// balance resource without label
	ops := bs.Schedule(tc)
	assert.Equal(t, 1, len(ops))
	testutil.CheckTransferPeer(t, ops[0], operator.OpKind(0), 1, 4)

	// cannot balance to container 4 and 5 with label
	tc.AddLabelsStore(4, 0, map[string]string{"specialUse": "hotShard"})
	tc.AddLabelsStore(5, 0, map[string]string{"specialUse": "reserved"})
	ops = bs.Schedule(tc)
	assert.Empty(t, ops)

	// can only move peer to 4
	tc.UpdateStorageWrittenBytes(1, 60*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(4, 0)
	tc.UpdateStorageWrittenBytes(5, 0)
	tc.AddLeaderShardWithWriteInfo(1, 1, 512*KB*statistics.ShardHeartBeatReportInterval, 0, statistics.ShardHeartBeatReportInterval, []uint64{2, 3})
	tc.AddLeaderShardWithWriteInfo(2, 1, 512*KB*statistics.ShardHeartBeatReportInterval, 0, statistics.ShardHeartBeatReportInterval, []uint64{2, 3})
	tc.AddLeaderShardWithWriteInfo(3, 1, 512*KB*statistics.ShardHeartBeatReportInterval, 0, statistics.ShardHeartBeatReportInterval, []uint64{2, 3})
	tc.AddLeaderShardWithWriteInfo(4, 2, 512*KB*statistics.ShardHeartBeatReportInterval, 0, statistics.ShardHeartBeatReportInterval, []uint64{1, 3})
	tc.AddLeaderShardWithWriteInfo(5, 3, 512*KB*statistics.ShardHeartBeatReportInterval, 0, statistics.ShardHeartBeatReportInterval, []uint64{1, 2})
	ops = hs.Schedule(tc)
	assert.Equal(t, 1, len(ops))
	testutil.CheckTransferPeer(t, ops[0], operator.OpHotShard, 1, 4)
}

func TestSpecialUseReserved(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	oc := schedule.NewOperatorController(ctx, tc, nil)
	storage := storage.NewTestStorage()
	cd := schedule.ConfigSliceDecoder(BalanceShardType, []string{"", ""})
	bs, err := schedule.CreateScheduler(BalanceShardType, oc, storage, cd)
	assert.NoError(t, err)

	tc.SetHotShardCacheHitsThreshold(0)
	tc.DisableJointConsensus()
	tc.AddShardStore(1, 10)
	tc.AddShardStore(2, 4)
	tc.AddShardStore(3, 2)
	tc.AddShardStore(4, 0)
	tc.AddLeaderShard(1, 1, 2, 3)
	tc.AddLeaderShard(2, 1, 2, 3)
	tc.AddLeaderShard(3, 1, 2, 3)
	tc.AddLeaderShard(4, 1, 2, 3)
	tc.AddLeaderShard(5, 1, 2, 3)

	// balance resource without label
	ops := bs.Schedule(tc)
	assert.Equal(t, 1, len(ops))
	testutil.CheckTransferPeer(t, ops[0], operator.OpKind(0), 1, 4)

	// cannot balance to container 4 with label
	tc.AddLabelsStore(4, 0, map[string]string{"specialUse": "reserved"})
	ops = bs.Schedule(tc)
	assert.Empty(t, ops)
}

type testBalanceLeaderSchedulerWithRuleEnabled struct {
	ctx    context.Context
	cancel context.CancelFunc
	tc     *mockcluster.Cluster
	lb     schedule.Scheduler
	oc     *schedule.OperatorController
	opt    *config.PersistOptions
}

func (s *testBalanceLeaderSchedulerWithRuleEnabled) setup(t *testing.T) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.opt = config.NewTestOptions()
	s.tc = mockcluster.NewCluster(s.opt)
	s.tc.SetEnablePlacementRules(true)
	s.oc = schedule.NewOperatorController(s.ctx, s.tc, nil)
	lb, err := schedule.CreateScheduler(BalanceLeaderType, s.oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(BalanceLeaderType, []string{"", ""}))
	assert.NoError(t, err)
	s.lb = lb
}

func (s *testBalanceLeaderSchedulerWithRuleEnabled) tearDown() {
	s.cancel()
}

func (s *testBalanceLeaderSchedulerWithRuleEnabled) schedule() []*operator.Operator {
	return s.lb.Schedule(s.tc)
}

func TestBalanceLeaderWithConflictRule(t *testing.T) {
	s := &testBalanceLeaderSchedulerWithRuleEnabled{}
	s.setup(t)
	defer s.tearDown()

	// containers:     1    2    3
	// Leaders:    1    0    0
	// resource1:    L    F    F
	s.tc.AddLeaderStore(1, 1)
	s.tc.AddLeaderStore(2, 0)
	s.tc.AddLeaderStore(3, 0)
	s.tc.AddLeaderShard(1, 1, 2, 3)
	s.tc.SetStoreLabel(1, map[string]string{
		"host": "a",
	})
	s.tc.SetStoreLabel(2, map[string]string{
		"host": "b",
	})
	s.tc.SetStoreLabel(3, map[string]string{
		"host": "c",
	})

	// containers:     1    2    3
	// Leaders:    16   0    0
	// resource1:    L    F    F
	s.tc.UpdateLeaderCount(1, 16)
	testcases := []struct {
		name     string
		rule     placement.Rule
		schedule bool
	}{
		{
			name: "default Rule",
			rule: placement.Rule{
				GroupID:        "prophet",
				ID:             "default",
				Index:          1,
				StartKey:       []byte(""),
				EndKey:         []byte(""),
				Role:           placement.Voter,
				Count:          3,
				LocationLabels: []string{"host"},
			},
			schedule: true,
		},
		{
			name: "single container allowed to be placed leader",
			rule: placement.Rule{
				GroupID:  "prophet",
				ID:       "default",
				Index:    1,
				StartKey: []byte(""),
				EndKey:   []byte(""),
				Role:     placement.Leader,
				Count:    1,
				LabelConstraints: []placement.LabelConstraint{
					{
						Key:    "host",
						Op:     placement.In,
						Values: []string{"a"},
					},
				},
				LocationLabels: []string{"host"},
			},
			schedule: false,
		},
		{
			name: "2 container allowed to be placed leader",
			rule: placement.Rule{
				GroupID:  "prophet",
				ID:       "default",
				Index:    1,
				StartKey: []byte(""),
				EndKey:   []byte(""),
				Role:     placement.Leader,
				Count:    1,
				LabelConstraints: []placement.LabelConstraint{
					{
						Key:    "host",
						Op:     placement.In,
						Values: []string{"a", "b"},
					},
				},
				LocationLabels: []string{"host"},
			},
			schedule: true,
		},
	}

	for _, testcase := range testcases {
		t.Logf(testcase.name)
		assert.Nil(t, s.tc.SetRule(&testcase.rule))
		if testcase.schedule {
			assert.Equal(t, 1, len(s.schedule()))
		} else {
			assert.Empty(t, s.schedule())
		}
	}
}

func checkBalance(t *testing.T, tc *mockcluster.Cluster, opt *config.PersistOptions, hb schedule.Scheduler) {
	// Add containers 1, 2, 3, 4, 5, 6  with hot peer counts 3, 2, 2, 2, 0, 0.
	tc.AddLabelsStore(1, 3, map[string]string{"zone": "z1", "host": "h1"})
	tc.AddLabelsStore(2, 2, map[string]string{"zone": "z2", "host": "h2"})
	tc.AddLabelsStore(3, 2, map[string]string{"zone": "z3", "host": "h3"})
	tc.AddLabelsStore(4, 2, map[string]string{"zone": "z4", "host": "h4"})
	tc.AddLabelsStore(5, 0, map[string]string{"zone": "z5", "host": "h5"})
	tc.AddLabelsStore(6, 0, map[string]string{"zone": "z4", "host": "h6"})

	// Report container written bytes.
	tc.UpdateStorageWrittenBytes(1, 7.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(2, 4.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(3, 4.5*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(4, 6*MB*statistics.StoreHeartBeatReportInterval)
	tc.UpdateStorageWrittenBytes(5, 0)
	tc.UpdateStorageWrittenBytes(6, 0)

	// resource 1, 2 and 3 are hot resources.
	//| resource_id | leader_container | follower_container | follower_container | written_bytes |
	//|-----------|--------------|----------------|----------------|---------------|
	//|     1     |       1      |        2       |       3        |      512KB    |
	//|     2     |       1      |        3       |       4        |      512KB    |
	//|     3     |       1      |        2       |       4        |      512KB    |
	tc.AddLeaderShardWithWriteInfo(1, 1, 512*KB*statistics.ShardHeartBeatReportInterval, 0, statistics.ShardHeartBeatReportInterval, []uint64{2, 3})
	tc.AddLeaderShardWithWriteInfo(2, 1, 512*KB*statistics.ShardHeartBeatReportInterval, 0, statistics.ShardHeartBeatReportInterval, []uint64{3, 4})
	tc.AddLeaderShardWithWriteInfo(3, 1, 512*KB*statistics.ShardHeartBeatReportInterval, 0, statistics.ShardHeartBeatReportInterval, []uint64{2, 4})
	tc.SetHotShardCacheHitsThreshold(0)

	// try to get an operator
	var op []*operator.Operator
	for i := 0; i < 100; i++ {
		op = hb.Schedule(tc)
		if op != nil {
			break
		}
	}
	assert.NotNil(t, op)
	assert.Equal(t, op[0].Step(op[0].Len()-1).(operator.TransferLeader).ToStore, op[0].Step(1).(operator.PromoteLearner).ToStore)
	assert.NotEqual(t, 6, op[0].Step(1).(operator.PromoteLearner).ToStore)
}
