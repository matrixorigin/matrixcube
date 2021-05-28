package checker

import (
	"encoding/hex"
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/util/cache"
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

	s.cluster.AddLeaderContainer(1, 1)
	s.cluster.AddLeaderContainer(2, 1)
	s.cluster.AddLeaderContainer(3, 1)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:     "test",
		ID:          "test",
		StartKeyHex: "AA",
		EndKeyHex:   "FF",
		Role:        placement.Voter,
		Count:       1,
	})
	s.cluster.AddLeaderResourceWithRange(1, "", "", 1, 2, 3)
	op := s.rc.Check(s.cluster.GetResource(1))
	assert.NotNil(t, op)
	assert.Equal(t, 1, op.Len())
	splitKeys := op.Step(0).(operator.SplitResource).SplitKeys
	assert.Equal(t, "aa", hex.EncodeToString(splitKeys[0]))
	assert.Equal(t, "ff", hex.EncodeToString(splitKeys[1]))
}

func TestAddRulePeer(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLeaderContainer(1, 1)
	s.cluster.AddLeaderContainer(2, 1)
	s.cluster.AddLeaderContainer(3, 1)
	s.cluster.AddLeaderResourceWithRange(1, "", "", 1, 2)
	op := s.rc.Check(s.cluster.GetResource(1))
	assert.NotNil(t, op)
	assert.Equal(t, "add-rule-peer", op.Desc())
	assert.Equal(t, uint64(3), op.Step(0).(operator.AddLearner).ToContainer)
}

func TestFillReplicasWithRule(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLeaderContainer(1, 2)
	s.cluster.AddLeaderContainer(2, 2)
	s.cluster.AddLeaderContainer(3, 1)

	res := core.NewTestCachedResource(nil, nil)
	res.Meta.SetPeers([]metapb.Peer{{ID: 1, ContainerID: 1}})
	err := s.rc.FillReplicas(res, 0)
	assert.Error(t, err)

	res.Meta.SetPeers(nil)
	err = s.rc.FillReplicas(res, 0)
	assert.NoError(t, err)
	assert.Equal(t, s.rc.cluster.GetOpts().GetMaxReplicas(), len(res.Meta.Peers()))
}

func TestAddRulePeerWithIsolationLevel(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLabelsContainer(1, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h1"})
	s.cluster.AddLabelsContainer(2, 1, map[string]string{"zone": "z1", "rack": "r1", "host": "h2"})
	s.cluster.AddLabelsContainer(3, 1, map[string]string{"zone": "z1", "rack": "r2", "host": "h1"})
	s.cluster.AddLabelsContainer(4, 1, map[string]string{"zone": "z1", "rack": "r3", "host": "h1"})
	s.cluster.AddLeaderResourceWithRange(1, "", "", 1, 2)
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
	op := s.rc.Check(s.cluster.GetResource(1))
	assert.Nil(t, op)
	s.cluster.AddLeaderResourceWithRange(1, "", "", 1, 3)
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
	op = s.rc.Check(s.cluster.GetResource(1))
	assert.NotNil(t, op)
	assert.Equal(t, "add-rule-peer", op.Desc())
	assert.Equal(t, uint64(4), op.Step(0).(operator.AddLearner).ToContainer)
}

func TestFixPeer(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLeaderContainer(1, 1)
	s.cluster.AddLeaderContainer(2, 1)
	s.cluster.AddLeaderContainer(3, 1)
	s.cluster.AddLeaderContainer(4, 1)
	s.cluster.AddLeaderResourceWithRange(1, "", "", 1, 2, 3)
	op := s.rc.Check(s.cluster.GetResource(1))
	assert.Nil(t, op)
	s.cluster.SetContainerDown(2)
	r := s.cluster.GetResource(1)
	p, _ := r.GetContainerPeer(2)
	r = r.Clone(core.WithDownPeers([]metapb.PeerStats{{Peer: p, DownSeconds: 60000}}))
	op = s.rc.Check(r)
	assert.NotNil(t, op)
	assert.Equal(t, "replace-rule-down-peer", op.Desc())
	_, ok := op.Step(0).(operator.AddLearner)
	assert.True(t, ok)
	s.cluster.SetContainerUP(2)
	s.cluster.SetContainerOffline(2)
	op = s.rc.Check(s.cluster.GetResource(1))
	assert.NotNil(t, op)
	assert.Equal(t, "replace-rule-offline-peer", op.Desc())
	_, ok = op.Step(0).(operator.AddLearner)
	assert.True(t, ok)
}

func TestFixOrphanPeers(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLeaderContainer(1, 1)
	s.cluster.AddLeaderContainer(2, 1)
	s.cluster.AddLeaderContainer(3, 1)
	s.cluster.AddLeaderContainer(4, 1)
	s.cluster.AddLeaderResourceWithRange(1, "", "", 1, 2, 3, 4)
	op := s.rc.Check(s.cluster.GetResource(1))
	assert.NotNil(t, op)
	assert.Equal(t, "remove-orphan-peer", op.Desc())
	assert.Equal(t, uint64(4), op.Step(0).(operator.RemovePeer).FromContainer)
}

func TestFixOrphanPeers2(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	// check orphan peers can only be handled when all rules are satisfied.
	s.cluster.AddLabelsContainer(1, 1, map[string]string{"foo": "bar"})
	s.cluster.AddLabelsContainer(2, 1, map[string]string{"foo": "bar"})
	s.cluster.AddLabelsContainer(3, 1, map[string]string{"foo": "baz"})
	s.cluster.AddLeaderResourceWithRange(1, "", "", 1, 3)
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
	s.cluster.SetContainerDown(2)
	op := s.rc.Check(s.cluster.GetResource(1))
	assert.Nil(t, op)
}

func TestFixRole(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLeaderContainer(1, 1)
	s.cluster.AddLeaderContainer(2, 1)
	s.cluster.AddLeaderContainer(3, 1)
	s.cluster.AddLeaderResourceWithRange(1, "", "", 2, 1, 3)
	r := s.cluster.GetResource(1)
	p, _ := r.GetContainerPeer(1)
	p.Role = metapb.PeerRole_Learner
	r = r.Clone(core.WithLearners([]metapb.Peer{p}))
	op := s.rc.Check(r)
	assert.NotNil(t, op)
	assert.Equal(t, "fix-peer-role", op.Desc())
	assert.Equal(t, uint64(1), op.Step(0).(operator.PromoteLearner).ToContainer)
}

func TestFixRoleLeader(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLabelsContainer(1, 1, map[string]string{"role": "follower"})
	s.cluster.AddLabelsContainer(2, 1, map[string]string{"role": "follower"})
	s.cluster.AddLabelsContainer(3, 1, map[string]string{"role": "voter"})
	s.cluster.AddLeaderResourceWithRange(1, "", "", 1, 2, 3)
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
	op := s.rc.Check(s.cluster.GetResource(1))
	assert.NotNil(t, op)
	assert.Equal(t, "fix-follower-role", op.Desc())
	assert.Equal(t, uint64(3), op.Step(0).(operator.TransferLeader).ToContainer)
}

func TestFixRoleLeaderIssue3130(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLabelsContainer(1, 1, map[string]string{"role": "follower"})
	s.cluster.AddLabelsContainer(2, 1, map[string]string{"role": "leader"})
	s.cluster.AddLeaderResource(1, 1, 2)
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
	op := s.rc.Check(s.cluster.GetResource(1))
	assert.NotNil(t, op)
	assert.Equal(t, "fix-leader-role", op.Desc())
	assert.Equal(t, uint64(2), op.Step(0).(operator.TransferLeader).ToContainer)

	s.cluster.SetContainerBusy(2, true)
	op = s.rc.Check(s.cluster.GetResource(1))
	assert.Nil(t, op)
	s.cluster.SetContainerBusy(2, false)

	s.cluster.AddLeaderResource(1, 2, 1)
	op = s.rc.Check(s.cluster.GetResource(1))
	assert.NotNil(t, op)
	assert.Equal(t, "remove-orphan-peer", op.Desc())
	assert.Equal(t, uint64(1), op.Step(0).(operator.RemovePeer).FromContainer)
}

func TestBetterReplacement(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLabelsContainer(1, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsContainer(2, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsContainer(3, 1, map[string]string{"host": "host2"})
	s.cluster.AddLabelsContainer(4, 1, map[string]string{"host": "host3"})
	s.cluster.AddLeaderResourceWithRange(1, "", "", 1, 2, 3)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:        "prophet",
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"host"},
	})
	op := s.rc.Check(s.cluster.GetResource(1))
	assert.NotNil(t, op)
	assert.Equal(t, "move-to-better-location", op.Desc())
	assert.Equal(t, uint64(4), op.Step(0).(operator.AddLearner).ToContainer)
	s.cluster.AddLeaderResourceWithRange(1, "", "", 1, 3, 4)
	op = s.rc.Check(s.cluster.GetResource(1))
	assert.Nil(t, op)
}

func TestBetterReplacement2(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLabelsContainer(1, 1, map[string]string{"zone": "z1", "host": "host1"})
	s.cluster.AddLabelsContainer(2, 1, map[string]string{"zone": "z1", "host": "host2"})
	s.cluster.AddLabelsContainer(3, 1, map[string]string{"zone": "z1", "host": "host3"})
	s.cluster.AddLabelsContainer(4, 1, map[string]string{"zone": "z2", "host": "host1"})
	s.cluster.AddLeaderResourceWithRange(1, "", "", 1, 2, 3)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:        "prophet",
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"zone", "host"},
	})
	op := s.rc.Check(s.cluster.GetResource(1))
	assert.NotNil(t, op)
	assert.Equal(t, "move-to-better-location", op.Desc())
	assert.Equal(t, uint64(4), op.Step(0).(operator.AddLearner).ToContainer)
	s.cluster.AddLeaderResourceWithRange(1, "", "", 1, 3, 4)
	op = s.rc.Check(s.cluster.GetResource(1))
	assert.Nil(t, op)
}

func TestNoBetterReplacement(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLabelsContainer(1, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsContainer(2, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsContainer(3, 1, map[string]string{"host": "host2"})
	s.cluster.AddLeaderResourceWithRange(1, "", "", 1, 2, 3)
	s.ruleManager.SetRule(&placement.Rule{
		GroupID:        "prophet",
		ID:             "test",
		Index:          100,
		Override:       true,
		Role:           placement.Voter,
		Count:          3,
		LocationLabels: []string{"host"},
	})
	op := s.rc.Check(s.cluster.GetResource(1))
	assert.Nil(t, op)
}

func TestIssue2419(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLeaderContainer(1, 1)
	s.cluster.AddLeaderContainer(2, 1)
	s.cluster.AddLeaderContainer(3, 1)
	s.cluster.AddLeaderContainer(4, 1)
	s.cluster.SetContainerOffline(3)
	s.cluster.AddLeaderResourceWithRange(1, "", "", 1, 2, 3)
	r := s.cluster.GetResource(1)
	r = r.Clone(core.WithAddPeer(metapb.Peer{ID: 5, ContainerID: 4, Role: metapb.PeerRole_Learner}))
	op := s.rc.Check(r)
	assert.NotNil(t, op)
	assert.Equal(t, "remove-orphan-peer", op.Desc())
	assert.Equal(t, uint64(4), op.Step(0).(operator.RemovePeer).FromContainer)

	r = r.Clone(core.WithRemoveContainerPeer(4))
	op = s.rc.Check(r)
	assert.NotNil(t, op)
	assert.Equal(t, "replace-rule-offline-peer", op.Desc())
	assert.Equal(t, uint64(4), op.Step(0).(operator.AddLearner).ToContainer)
	assert.Equal(t, uint64(4), op.Step(1).(operator.PromoteLearner).ToContainer)
	assert.Equal(t, uint64(3), op.Step(2).(operator.RemovePeer).FromContainer)
}

// Ref https://github.com/tikv/pd/issues/3521
// The problem is when offline a store, we may add learner multiple times if
// the operator is timeout.
func TestIssue3521_PriorityFixOrphanPeer(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLabelsContainer(1, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsContainer(2, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsContainer(3, 1, map[string]string{"host": "host2"})
	s.cluster.AddLabelsContainer(4, 1, map[string]string{"host": "host4"})
	s.cluster.AddLabelsContainer(5, 1, map[string]string{"host": "host5"})
	s.cluster.AddLeaderResourceWithRange(1, "", "", 1, 2, 3)
	op := s.rc.Check(s.cluster.GetResource(1))
	assert.Nil(t, op)
	s.cluster.SetContainerOffline(2)
	op = s.rc.Check(s.cluster.GetResource(1))
	assert.NotNil(t, op)
	_, ok := op.Step(0).(operator.AddLearner)
	assert.True(t, ok)
	assert.Equal(t, "replace-rule-offline-peer", op.Desc())
	r := s.cluster.GetResource(1).Clone(core.WithAddPeer(
		metapb.Peer{
			ID:          5,
			ContainerID: 4,
			Role:        metapb.PeerRole_Learner,
		}))
	s.cluster.PutResource(r)
	op = s.rc.Check(s.cluster.GetResource(1))
	_, ok = op.Step(0).(operator.RemovePeer)
	assert.True(t, ok)
	assert.Equal(t, "remove-orphan-peer", op.Desc())
}

func TestIssue3293(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLabelsContainer(1, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsContainer(2, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsContainer(3, 1, map[string]string{"host": "host2"})
	s.cluster.AddLabelsContainer(4, 1, map[string]string{"host": "host4"})
	s.cluster.AddLabelsContainer(5, 1, map[string]string{"host": "host5"})
	s.cluster.AddLeaderResourceWithRange(1, "", "", 1, 2)
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
	s.cluster.DeleteContainer(s.cluster.TakeContainer(5))
	err = s.ruleManager.SetRule(&placement.Rule{
		GroupID: "DDL_51",
		ID:      "default",
		Role:    placement.Voter,
		Count:   3,
	})
	assert.NoError(t, err)
	err = s.ruleManager.DeleteRule("prophet", "default")
	assert.NoError(t, err)
	op := s.rc.Check(s.cluster.GetResource(1))
	assert.NotNil(t, op)
	assert.Equal(t, "add-rule-peer", op.Desc())
}

func TestIssue3299(t *testing.T) {
	s := &testRuleChecker{}
	s.setup()

	s.cluster.AddLabelsContainer(1, 1, map[string]string{"host": "host1"})
	s.cluster.AddLabelsContainer(2, 1, map[string]string{"dc": "sh"})
	s.cluster.AddLeaderResourceWithRange(1, "", "", 1, 2)

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
