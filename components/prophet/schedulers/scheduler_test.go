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
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
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

func TestShuffleShard(t *testing.T) {
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
