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

package checker

import (
	"encoding/hex"
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/util/cache"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

type testRuleChecker struct {
	cluster     *mockcluster.Cluster
	ruleManager *placement.RuleManager
	rc          *RuleChecker
}

func (s *testRuleChecker) setup() {
	cfg := config.NewTestOptions()
	s.cluster = mockcluster.NewCluster(cfg)
	s.cluster.SetEnablePlacementRules(true)
	s.ruleManager = s.cluster.RuleManager
	s.rc = NewRuleChecker(s.cluster, s.ruleManager, cache.NewDefaultCache(10))
}

func TestFixRange(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLeaderStore(1, 1)
	s.cluster.AddLeaderStore(2, 1)
	s.cluster.AddLeaderStore(3, 1)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:     "test",
		ID:          "test",
		StartKeyHex: "AA",
		EndKeyHex:   "FF",
		Role:        placement.Voter,
		Count:       1,
	})
	s.cluster.AddLeaderShardWithRange(1, "", "", 1, 2, 3)
	op := s.rc.Check(s.cluster.GetShard(1))
	assert.NotNil(t, op)
	assert.Equal(t, 1, op.Len())
	splitKeys := op.Step(0).(operator.SplitShard).SplitKeys
	assert.Equal(t, "aa", hex.EncodeToString(splitKeys[0]))
	assert.Equal(t, "ff", hex.EncodeToString(splitKeys[1]))
}

func TestAddRulePeer(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLeaderStore(1, 1)
	s.cluster.AddLeaderStore(2, 1)
	s.cluster.AddLeaderStore(3, 1)
	s.cluster.AddLeaderShardWithRange(1, "", "", 1, 2)
	op := s.rc.Check(s.cluster.GetShard(1))
	assert.NotNil(t, op)
	assert.Equal(t, "add-rule-peer", op.Desc())
	assert.Equal(t, uint64(3), op.Step(0).(operator.AddLearner).ToStore)
}

func TestFillReplicasWithRule(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLeaderStore(1, 2)
	s.cluster.AddLeaderStore(2, 2)
	s.cluster.AddLeaderStore(3, 1)

	res := core.NewTestCachedShard(nil, nil)
	res.Meta.SetReplicas([]metapb.Replica{{ID: 1, StoreID: 1}})
	err := s.rc.FillReplicas(res, 0)
	assert.Error(t, err)

	res.Meta.SetReplicas(nil)
	err = s.rc.FillReplicas(res, 0)
	assert.NoError(t, err)
	assert.Equal(t, s.rc.cluster.GetOpts().GetMaxReplicas(), len(res.Meta.GetReplicas()))
}

func TestAddRulePeerWithIsolationLevel(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	s.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	s.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	s.cluster.AddLabelsStore(4, 1, map[string]string{"zone": "z1", "rack": "r3", "host": "h1"})
	s.cluster.AddLeaderShardWithRange(1, "", "", 1, 2)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:        "prophet",
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"zone", "rack", "host"},
		IsolationLevel: "zone",
	})
	op := s.rc.Check(s.cluster.GetShard(1))
	assert.Nil(t, op)
	s.cluster.AddLeaderShardWithRange(1, "", "", 1, 3)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:        "prophet",
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"zone", "rack", "host"},
		IsolationLevel: "rack",
	})
	op = s.rc.Check(s.cluster.GetShard(1))
	assert.NotNil(t, op)
	assert.Equal(t, "add-rule-peer", op.Desc())
	assert.Equal(t, uint64(4), op.Step(0).(operator.AddLearner).ToStore)
}

func TestFixPeer(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLeaderStore(1, 1)
	s.cluster.AddLeaderStore(2, 1)
	s.cluster.AddLeaderStore(3, 1)
	s.cluster.AddLeaderStore(4, 1)
	s.cluster.AddLeaderShardWithRange(1, "", "", 1, 2, 3)
	op := s.rc.Check(s.cluster.GetShard(1))
	assert.Nil(t, op)
	s.cluster.SetStoreDown(2)
	r := s.cluster.GetShard(1)
	p, _ := r.GetStorePeer(2)
	r = r.Clone(core.WithDownPeers([]metapb.ReplicaStats{{Replica: p, DownSeconds: 60000}}))
	op = s.rc.Check(r)
	assert.NotNil(t, op)
	assert.Equal(t, "replace-rule-down-peer", op.Desc())
	_, ok := op.Step(0).(operator.AddLearner)
	assert.True(t, ok)
	s.cluster.SetStoreUP(2)
	s.cluster.SetStoreOffline(2)
	op = s.rc.Check(s.cluster.GetShard(1))
	assert.NotNil(t, op)
	assert.Equal(t, "replace-rule-offline-peer", op.Desc())
	_, ok = op.Step(0).(operator.AddLearner)
	assert.True(t, ok)
}

func TestFixOrphanPeers(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLeaderStore(1, 1)
	s.cluster.AddLeaderStore(2, 1)
	s.cluster.AddLeaderStore(3, 1)
	s.cluster.AddLeaderStore(4, 1)
	s.cluster.AddLeaderShardWithRange(1, "", "", 1, 2, 3, 4)
	op := s.rc.Check(s.cluster.GetShard(1))
	assert.NotNil(t, op)
	assert.Equal(t, "remove-orphan-peer", op.Desc())
	assert.Equal(t, uint64(4), op.Step(0).(operator.RemovePeer).FromStore)
}

func TestFixOrphanPeers2(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	// check orphan peers can only be handled when all rules are satisfied.
	s.cluster.AddLabelsStore(1, 1, map[string]string{"foo": "bar"})
	s.cluster.AddLabelsStore(2, 1, map[string]string{"foo": "bar"})
	s.cluster.AddLabelsStore(3, 1, map[string]string{"foo": "baz"})
	s.cluster.AddLeaderShardWithRange(1, "", "", 1, 3)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:  "prophet",
		ID:       "r1",
		Index:    100,
		Override: true,
		Role:     placement.Leader,
		Count:    2,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "foo", Op: "in", Values: []string{"baz"}},
		},
	})
	s.cluster.SetStoreDown(2)
	op := s.rc.Check(s.cluster.GetShard(1))
	assert.Nil(t, op)
}

func TestFixRole(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLeaderStore(1, 1)
	s.cluster.AddLeaderStore(2, 1)
	s.cluster.AddLeaderStore(3, 1)
	s.cluster.AddLeaderShardWithRange(1, "", "", 2, 1, 3)
	r := s.cluster.GetShard(1)
	p, _ := r.GetStorePeer(1)
	p.Role = metapb.ReplicaRole_Learner
	r = r.Clone(core.WithLearners([]metapb.Replica{p}))
	op := s.rc.Check(r)
	assert.NotNil(t, op)
	assert.Equal(t, "fix-peer-role", op.Desc())
	assert.Equal(t, uint64(1), op.Step(0).(operator.PromoteLearner).ToStore)
}

func TestFixRoleLeader(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLabelsStore(1, 1, map[string]string{"role": "follower"})
	s.cluster.AddLabelsStore(2, 1, map[string]string{"role": "follower"})
	s.cluster.AddLabelsStore(3, 1, map[string]string{"role": "voter"})
	s.cluster.AddLeaderShardWithRange(1, "", "", 1, 2, 3)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:  "prophet",
		ID:       "r1",
		Index:    100,
		Override: true,
		Role:     placement.Voter,
		Count:    1,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "role", Op: "in", Values: []string{"voter"}},
		},
	})
	s.ruleManager.SetRule(&placement.Rule{
		GroupID: "prophet",
		ID:      "r2",
		Index:   101,
		Role:    placement.Follower,
		Count:   2,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "role", Op: "in", Values: []string{"follower"}},
		},
	})
	op := s.rc.Check(s.cluster.GetShard(1))
	assert.NotNil(t, op)
	assert.Equal(t, "fix-follower-role", op.Desc())
	assert.Equal(t, uint64(3), op.Step(0).(operator.TransferLeader).ToStore)
}

func TestFixRoleLeaderIssue3130(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLabelsStore(1, 1, map[string]string{"role": "follower"})
	s.cluster.AddLabelsStore(2, 1, map[string]string{"role": "leader"})
	s.cluster.AddLeaderShard(1, 1, 2)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:  "prophet",
		ID:       "r1",
		Index:    100,
		Override: true,
		Role:     placement.Leader,
		Count:    1,
		LabelConstraints: []placement.LabelConstraint{
			{Key: "role", Op: "in", Values: []string{"leader"}},
		},
	})
	op := s.rc.Check(s.cluster.GetShard(1))
	assert.NotNil(t, op)
	assert.Equal(t, "fix-leader-role", op.Desc())
	assert.Equal(t, uint64(2), op.Step(0).(operator.TransferLeader).ToStore)

	s.cluster.SetStoreBusy(2, true)
	op = s.rc.Check(s.cluster.GetShard(1))
	assert.Nil(t, op)
	s.cluster.SetStoreBusy(2, false)

	s.cluster.AddLeaderShard(1, 2, 1)
	op = s.rc.Check(s.cluster.GetShard(1))
	assert.NotNil(t, op)
	assert.Equal(t, "remove-orphan-peer", op.Desc())
	assert.Equal(t, uint64(1), op.Step(0).(operator.RemovePeer).FromStore)
}

func TestBetterReplacement(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsStore(2, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsStore(3, 1, map[string]string{"host": "host2"})
	s.cluster.AddLabelsStore(4, 1, map[string]string{"host": "host3"})
	s.cluster.AddLeaderShardWithRange(1, "", "", 1, 2, 3)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:        "prophet",
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"host"},
	})
	op := s.rc.Check(s.cluster.GetShard(1))
	assert.NotNil(t, op)
	assert.Equal(t, "move-to-better-location", op.Desc())
	assert.Equal(t, uint64(4), op.Step(0).(operator.AddLearner).ToStore)
	s.cluster.AddLeaderShardWithRange(1, "", "", 1, 3, 4)
	op = s.rc.Check(s.cluster.GetShard(1))
	assert.Nil(t, op)
}

func TestBetterReplacement2(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLabelsStore(1, 1, map[string]string{"zone": "z1", "host": "host1"})
	s.cluster.AddLabelsStore(2, 1, map[string]string{"zone": "z1", "host": "host2"})
	s.cluster.AddLabelsStore(3, 1, map[string]string{"zone": "z1", "host": "host3"})
	s.cluster.AddLabelsStore(4, 1, map[string]string{"zone": "z2", "host": "host1"})
	s.cluster.AddLeaderShardWithRange(1, "", "", 1, 2, 3)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:        "prophet",
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"zone", "host"},
	})
	op := s.rc.Check(s.cluster.GetShard(1))
	assert.NotNil(t, op)
	assert.Equal(t, "move-to-better-location", op.Desc())
	assert.Equal(t, uint64(4), op.Step(0).(operator.AddLearner).ToStore)
	s.cluster.AddLeaderShardWithRange(1, "", "", 1, 3, 4)
	op = s.rc.Check(s.cluster.GetShard(1))
	assert.Nil(t, op)
}

func TestNoBetterReplacement(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsStore(2, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsStore(3, 1, map[string]string{"host": "host2"})
	s.cluster.AddLeaderShardWithRange(1, "", "", 1, 2, 3)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:        "prophet",
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"host"},
	})
	op := s.rc.Check(s.cluster.GetShard(1))
	assert.Nil(t, op)
}

func TestIssue2419(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLeaderStore(1, 1)
	s.cluster.AddLeaderStore(2, 1)
	s.cluster.AddLeaderStore(3, 1)
	s.cluster.AddLeaderStore(4, 1)
	s.cluster.SetStoreOffline(3)
	s.cluster.AddLeaderShardWithRange(1, "", "", 1, 2, 3)
	r := s.cluster.GetShard(1)
	r = r.Clone(core.WithAddPeer(metapb.Replica{ID: 5, StoreID: 4, Role: metapb.ReplicaRole_Learner}))
	op := s.rc.Check(r)
	assert.NotNil(t, op)
	assert.Equal(t, "remove-orphan-peer", op.Desc())
	assert.Equal(t, uint64(4), op.Step(0).(operator.RemovePeer).FromStore)

	r = r.Clone(core.WithRemoveStorePeer(4))
	op = s.rc.Check(r)
	assert.NotNil(t, op)
	assert.Equal(t, "replace-rule-offline-peer", op.Desc())
	assert.Equal(t, uint64(4), op.Step(0).(operator.AddLearner).ToStore)
	assert.Equal(t, uint64(4), op.Step(1).(operator.PromoteLearner).ToStore)
	assert.Equal(t, uint64(3), op.Step(2).(operator.RemovePeer).FromStore)
}

// Ref https://github.com/tikv/pd/issues/3521
// The problem is when offline a store, we may add learner multiple times if
// the operator is timeout.
func TestIssue3521_PriorityFixOrphanPeer(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsStore(2, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsStore(3, 1, map[string]string{"host": "host2"})
	s.cluster.AddLabelsStore(4, 1, map[string]string{"host": "host4"})
	s.cluster.AddLabelsStore(5, 1, map[string]string{"host": "host5"})
	s.cluster.AddLeaderShardWithRange(1, "", "", 1, 2, 3)
	op := s.rc.Check(s.cluster.GetShard(1))
	assert.Nil(t, op)
	s.cluster.SetStoreOffline(2)
	op = s.rc.Check(s.cluster.GetShard(1))
	assert.NotNil(t, op)
	_, ok := op.Step(0).(operator.AddLearner)
	assert.True(t, ok)
	assert.Equal(t, "replace-rule-offline-peer", op.Desc())
	r := s.cluster.GetShard(1).Clone(core.WithAddPeer(
		metapb.Replica{
			ID:      5,
			StoreID: 4,
			Role:    metapb.ReplicaRole_Learner,
		}))
	s.cluster.PutShard(r)
	op = s.rc.Check(s.cluster.GetShard(1))
	_, ok = op.Step(0).(operator.RemovePeer)
	assert.True(t, ok)
	assert.Equal(t, "remove-orphan-peer", op.Desc())
}

func TestIssue3293(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsStore(2, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsStore(3, 1, map[string]string{"host": "host2"})
	s.cluster.AddLabelsStore(4, 1, map[string]string{"host": "host4"})
	s.cluster.AddLabelsStore(5, 1, map[string]string{"host": "host5"})
	s.cluster.AddLeaderShardWithRange(1, "", "", 1, 2)
	err := s.ruleManager.SetRule(&placement.Rule{
		GroupID: "DDL_51",
		ID:      "0",
		Role:    placement.Follower,
		Count:   1,
		LabelConstraints: []placement.LabelConstraint{
			{
				Key: "host",
				Values: []string{
					"host5",
				},
				Op: placement.In,
			},
		},
	})
	assert.NoError(t, err)
	s.cluster.DeleteStore(s.cluster.TakeStore(5))
	err = s.ruleManager.SetRule(&placement.Rule{
		GroupID: "DDL_51",
		ID:      "default",
		Role:    placement.Voter,
		Count:   3,
	})
	assert.NoError(t, err)
	err = s.ruleManager.DeleteRule("prophet", "default")
	assert.NoError(t, err)
	op := s.rc.Check(s.cluster.GetShard(1))
	assert.NotNil(t, op)
	assert.Equal(t, "add-rule-peer", op.Desc())
}

func TestIssue3299(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsStore(2, 1, map[string]string{"dc": "sh"})
	s.cluster.AddLeaderShardWithRange(1, "", "", 1, 2)

	testCases := []struct {
		constraints []placement.LabelConstraint
		err         string
	}{
		{
			constraints: []placement.LabelConstraint{
				{
					Key:    "host",
					Values: []string{"host5"},
					Op:     placement.In,
				},
			},
			err: ".*can not match any store",
		},
		{
			constraints: []placement.LabelConstraint{
				{
					Key:    "ho",
					Values: []string{"sh"},
					Op:     placement.In,
				},
			},
			err: ".*can not match any store",
		},
		{
			constraints: []placement.LabelConstraint{
				{
					Key:    "host",
					Values: []string{"host1"},
					Op:     placement.In,
				},
				{
					Key:    "host",
					Values: []string{"host1"},
					Op:     placement.NotIn,
				},
			},
			err: ".*can not match any store",
		},
		{
			constraints: []placement.LabelConstraint{
				{
					Key:    "host",
					Values: []string{"host1"},
					Op:     placement.In,
				},
				{
					Key:    "host",
					Values: []string{"host3"},
					Op:     placement.In,
				},
			},
			err: ".*can not match any store",
		},
		{
			constraints: []placement.LabelConstraint{
				{
					Key:    "host",
					Values: []string{"host1"},
					Op:     placement.In,
				},
				{
					Key:    "host",
					Values: []string{"host1"},
					Op:     placement.In,
				},
			},
			err: "",
		},
	}

	for _, tc := range testCases {
		err := s.ruleManager.SetRule(&placement.Rule{
			GroupID:          "p",
			ID:               "0",
			Role:             placement.Follower,
			Count:            1,
			LabelConstraints: tc.constraints,
		})
		if tc.err != "" {
			assert.Error(t, err)
		} else {
			assert.Empty(t, tc.err)
		}
	}
}
