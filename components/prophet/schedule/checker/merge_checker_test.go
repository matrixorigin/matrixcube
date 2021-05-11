package checker

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/stretchr/testify/assert"
)

// func TestMergeChecker(t *testing.T) {
// 	TestingT(t)
// }

// func TestMain(m *testing.M) {
// 	goleak.VerifyTestMain(m, testutil.LeakOptions...)
// }

// var _ = Suite(&testMergeCheckerSuite{})

type testMergeChecker struct {
	ctx       context.Context
	cancel    context.CancelFunc
	cluster   *mockcluster.Cluster
	mc        *MergeChecker
	resources []*core.CachedResource
}

func (s *testMergeChecker) setup() {
	cfg := config.NewTestOptions()
	s.cluster = mockcluster.NewCluster(cfg)
	s.cluster.SetMaxMergeResourceSize(2)
	s.cluster.SetMaxMergeResourceKeys(2)
	s.cluster.SetLabelPropertyConfig(config.LabelPropertyConfig{
		opt.RejectLeader: {{Key: "reject", Value: "leader"}},
	})
	containers := map[uint64][]string{
		1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {},
		7: {"reject", "leader"},
		8: {"reject", "leader"},
	}
	for id, labels := range containers {
		s.cluster.PutContainerWithLabels(id, labels...)
	}
	s.resources = []*core.CachedResource{
		core.NewCachedResource(
			&metadata.TestResource{
				ResID: 1,
				Start: []byte(""),
				End:   []byte("a"),
				ResPeers: []metapb.Peer{
					{ID: 101, ContainerID: 1},
					{ID: 102, ContainerID: 2},
				},
			},
			&metapb.Peer{ID: 101, ContainerID: 1},
			core.SetApproximateSize(1),
			core.SetApproximateKeys(1),
		),
		core.NewCachedResource(
			&metadata.TestResource{
				ResID: 2,
				Start: []byte("a"),
				End:   []byte("t"),
				ResPeers: []metapb.Peer{
					{ID: 103, ContainerID: 1},
					{ID: 104, ContainerID: 4},
					{ID: 105, ContainerID: 5},
				},
			},
			&metapb.Peer{ID: 104, ContainerID: 4},
			core.SetApproximateSize(200),
			core.SetApproximateKeys(200),
		),
		core.NewCachedResource(
			&metadata.TestResource{
				ResID: 3,
				Start: []byte("t"),
				End:   []byte("x"),
				ResPeers: []metapb.Peer{
					{ID: 106, ContainerID: 2},
					{ID: 107, ContainerID: 5},
					{ID: 108, ContainerID: 6},
				},
			},
			&metapb.Peer{ID: 108, ContainerID: 6},
			core.SetApproximateSize(1),
			core.SetApproximateKeys(1),
		),
		core.NewCachedResource(
			&metadata.TestResource{
				ResID: 4,
				Start: []byte("x"),
				End:   []byte(""),
				ResPeers: []metapb.Peer{
					{ID: 109, ContainerID: 4},
				},
			},
			&metapb.Peer{ID: 109, ContainerID: 4},
			core.SetApproximateSize(10),
			core.SetApproximateKeys(10),
		),
	}

	for _, res := range s.resources {
		s.cluster.PutResource(res)
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.mc = NewMergeChecker(s.ctx, s.cluster)
}

func (s *testMergeChecker) tearDown() {
	s.cancel()
}

func (s *testMergeChecker) checkSteps(t *testing.T, op *operator.Operator, steps []operator.OpStep) {
	assert.NotEqual(t, 0, op.Kind()&operator.OpMerge)
	assert.NotNil(t, steps)
	assert.Equal(t, len(steps), op.Len())
	for i := range steps {
		switch op.Step(i).(type) {
		case operator.AddLearner:
			assert.Equal(t, steps[i].(operator.AddLearner).ToContainer, op.Step(i).(operator.AddLearner).ToContainer)
		case operator.PromoteLearner:
			assert.Equal(t, steps[i].(operator.PromoteLearner).ToContainer, op.Step(i).(operator.PromoteLearner).ToContainer)
		case operator.TransferLeader:
			assert.Equal(t, op.Step(i).(operator.TransferLeader).FromContainer, steps[i].(operator.TransferLeader).FromContainer)
			assert.Equal(t, op.Step(i).(operator.TransferLeader).ToContainer, steps[i].(operator.TransferLeader).ToContainer)
		case operator.RemovePeer:
			assert.Equal(t, op.Step(i).(operator.RemovePeer).FromContainer, steps[i].(operator.RemovePeer).FromContainer)
		case operator.MergeResource:
			assert.Equal(t, op.Step(i).(operator.MergeResource).IsPassive, steps[i].(operator.MergeResource).IsPassive)
		default:
			assert.FailNow(t, "unknown operator step type")
		}
	}
}

func TestBasic(t *testing.T) {
	s := &testMergeChecker{}
	s.setup()
	defer s.cancel()

	s.cluster.SetSplitMergeInterval(0)

	// should with same peer count
	ops := s.mc.Check(s.resources[0])
	assert.Empty(t, ops)

	// The size should be small enough.
	ops = s.mc.Check(s.resources[1])
	assert.Empty(t, ops)

	// target resource size is too large
	s.cluster.PutResource(s.resources[1].Clone(core.SetApproximateSize(600)))
	ops = s.mc.Check(s.resources[2])
	assert.Empty(t, ops)

	// change the size back
	s.cluster.PutResource(s.resources[1].Clone(core.SetApproximateSize(200)))
	ops = s.mc.Check(s.resources[2])
	assert.NotNil(t, ops)
	// Check merge with previous resource.
	assert.Equal(t, s.resources[2].Meta.ID(), ops[0].ResourceID())
	assert.Equal(t, s.resources[1].Meta.ID(), ops[1].ResourceID())

	// Enable one way merge
	s.cluster.SetEnableOneWayMerge(true)
	ops = s.mc.Check(s.resources[2])
	assert.Empty(t, ops)
	s.cluster.SetEnableOneWayMerge(false)

	// Make up peers for next resource.
	s.resources[3] = s.resources[3].Clone(core.WithAddPeer(metapb.Peer{ID: 110, ContainerID: 1}),
		core.WithAddPeer(metapb.Peer{ID: 111, ContainerID: 2}))
	s.cluster.PutResource(s.resources[3])
	ops = s.mc.Check(s.resources[2])
	assert.NotNil(t, ops)
	// Now it merges to next resource.
	assert.Equal(t, ops[0].ResourceID(), s.resources[2].Meta.ID())
	assert.Equal(t, ops[1].ResourceID(), s.resources[3].Meta.ID())

	// merge cannot across rule key.
	s.cluster.SetEnablePlacementRules(true)
	s.cluster.RuleManager.SetRule(&placement.Rule{
		GroupID:     "prophet",
		ID:          "test",
		Index:       1,
		Override:    true,
		StartKeyHex: hex.EncodeToString([]byte("x")),
		EndKeyHex:   hex.EncodeToString([]byte("z")),
		Role:        placement.Voter,
		Count:       3,
	})
	// resource 2 can only merge with previous resource now.
	ops = s.mc.Check(s.resources[2])
	assert.NotNil(t, ops)
	assert.Equal(t, ops[0].ResourceID(), s.resources[2].Meta.ID())
	assert.Equal(t, ops[1].ResourceID(), s.resources[1].Meta.ID())
	s.cluster.RuleManager.DeleteRule("test", "test")

	// Skip recently split resources.
	s.cluster.SetSplitMergeInterval(time.Hour)
	s.mc.RecordResourceSplit([]uint64{s.resources[2].Meta.ID()})
	ops = s.mc.Check(s.resources[2])
	assert.Nil(t, ops)
	ops = s.mc.Check(s.resources[3])
	assert.Nil(t, ops)
}

func TestMatchPeers(t *testing.T) {
	s := &testMergeChecker{}
	s.setup()
	defer s.cancel()

	s.cluster.SetSplitMergeInterval(0)
	// partial Container overlap not including leader
	ops := s.mc.Check(s.resources[2])
	assert.NotNil(t, ops)
	s.checkSteps(t, ops[0], []operator.OpStep{
		operator.AddLearner{ToContainer: 1},
		operator.PromoteLearner{ToContainer: 1},
		operator.RemovePeer{FromContainer: 2},
		operator.AddLearner{ToContainer: 4},
		operator.PromoteLearner{ToContainer: 4},
		operator.TransferLeader{FromContainer: 6, ToContainer: 5},
		operator.RemovePeer{FromContainer: 6},
		operator.MergeResource{
			FromResource: s.resources[2].Meta,
			ToResource:   s.resources[1].Meta,
			IsPassive:    false,
		},
	})
	s.checkSteps(t, ops[1], []operator.OpStep{
		operator.MergeResource{
			FromResource: s.resources[2].Meta,
			ToResource:   s.resources[1].Meta,
			IsPassive:    true,
		},
	})

	// partial Container overlap including leader
	newresource := s.resources[2].Clone(
		core.SetPeers([]metapb.Peer{
			{ID: 106, ContainerID: 1},
			{ID: 107, ContainerID: 5},
			{ID: 108, ContainerID: 6},
		}),
		core.WithLeader(&metapb.Peer{ID: 106, ContainerID: 1}),
	)
	s.resources[2] = newresource
	s.cluster.PutResource(s.resources[2])
	ops = s.mc.Check(s.resources[2])
	s.checkSteps(t, ops[0], []operator.OpStep{
		operator.AddLearner{ToContainer: 4},
		operator.PromoteLearner{ToContainer: 4},
		operator.RemovePeer{FromContainer: 6},
		operator.MergeResource{
			FromResource: s.resources[2].Meta,
			ToResource:   s.resources[1].Meta,
			IsPassive:    false,
		},
	})
	s.checkSteps(t, ops[1], []operator.OpStep{
		operator.MergeResource{
			FromResource: s.resources[2].Meta,
			ToResource:   s.resources[1].Meta,
			IsPassive:    true,
		},
	})

	// all Containers overlap
	s.resources[2] = s.resources[2].Clone(core.SetPeers([]metapb.Peer{
		{ID: 106, ContainerID: 1},
		{ID: 107, ContainerID: 5},
		{ID: 108, ContainerID: 4},
	}))
	s.cluster.PutResource(s.resources[2])
	ops = s.mc.Check(s.resources[2])
	s.checkSteps(t, ops[0], []operator.OpStep{
		operator.MergeResource{
			FromResource: s.resources[2].Meta,
			ToResource:   s.resources[1].Meta,
			IsPassive:    false,
		},
	})
	s.checkSteps(t, ops[1], []operator.OpStep{
		operator.MergeResource{
			FromResource: s.resources[2].Meta,
			ToResource:   s.resources[1].Meta,
			IsPassive:    true,
		},
	})

	// all Containers not overlap
	s.resources[2] = s.resources[2].Clone(core.SetPeers([]metapb.Peer{
		{ID: 109, ContainerID: 2},
		{ID: 110, ContainerID: 3},
		{ID: 111, ContainerID: 6},
	}), core.WithLeader(&metapb.Peer{ID: 109, ContainerID: 2}))
	s.cluster.PutResource(s.resources[2])
	ops = s.mc.Check(s.resources[2])
	s.checkSteps(t, ops[0], []operator.OpStep{
		operator.AddLearner{ToContainer: 1},
		operator.PromoteLearner{ToContainer: 1},
		operator.RemovePeer{FromContainer: 3},
		operator.AddLearner{ToContainer: 4},
		operator.PromoteLearner{ToContainer: 4},
		operator.RemovePeer{FromContainer: 6},
		operator.AddLearner{ToContainer: 5},
		operator.PromoteLearner{ToContainer: 5},
		operator.TransferLeader{FromContainer: 2, ToContainer: 1},
		operator.RemovePeer{FromContainer: 2},
		operator.MergeResource{
			FromResource: s.resources[2].Meta,
			ToResource:   s.resources[1].Meta,
			IsPassive:    false,
		},
	})
	s.checkSteps(t, ops[1], []operator.OpStep{
		operator.MergeResource{
			FromResource: s.resources[2].Meta,
			ToResource:   s.resources[1].Meta,
			IsPassive:    true,
		},
	})

	// no overlap with reject leader label
	s.resources[1] = s.resources[1].Clone(
		core.SetPeers([]metapb.Peer{
			{ID: 112, ContainerID: 7},
			{ID: 113, ContainerID: 8},
			{ID: 114, ContainerID: 1},
		}),
		core.WithLeader(&metapb.Peer{ID: 114, ContainerID: 1}),
	)
	s.cluster.PutResource(s.resources[1])
	ops = s.mc.Check(s.resources[2])
	s.checkSteps(t, ops[0], []operator.OpStep{
		operator.AddLearner{ToContainer: 1},
		operator.PromoteLearner{ToContainer: 1},
		operator.RemovePeer{FromContainer: 3},

		operator.AddLearner{ToContainer: 7},
		operator.PromoteLearner{ToContainer: 7},
		operator.RemovePeer{FromContainer: 6},

		operator.AddLearner{ToContainer: 8},
		operator.PromoteLearner{ToContainer: 8},
		operator.TransferLeader{FromContainer: 2, ToContainer: 1},
		operator.RemovePeer{FromContainer: 2},

		operator.MergeResource{
			FromResource: s.resources[2].Meta,
			ToResource:   s.resources[1].Meta,
			IsPassive:    false,
		},
	})
	s.checkSteps(t, ops[1], []operator.OpStep{
		operator.MergeResource{
			FromResource: s.resources[2].Meta,
			ToResource:   s.resources[1].Meta,
			IsPassive:    true,
		},
	})

	// overlap with reject leader label
	s.resources[1] = s.resources[1].Clone(
		core.SetPeers([]metapb.Peer{
			{ID: 115, ContainerID: 7},
			{ID: 116, ContainerID: 8},
			{ID: 117, ContainerID: 1},
		}),
		core.WithLeader(&metapb.Peer{ID: 117, ContainerID: 1}),
	)
	s.resources[2] = s.resources[2].Clone(
		core.SetPeers([]metapb.Peer{
			{ID: 118, ContainerID: 7},
			{ID: 119, ContainerID: 3},
			{ID: 120, ContainerID: 2},
		}),
		core.WithLeader(&metapb.Peer{ID: 120, ContainerID: 2}),
	)
	s.cluster.PutResource(s.resources[1])
	ops = s.mc.Check(s.resources[2])
	s.checkSteps(t, ops[0], []operator.OpStep{
		operator.AddLearner{ToContainer: 1},
		operator.PromoteLearner{ToContainer: 1},
		operator.RemovePeer{FromContainer: 3},
		operator.AddLearner{ToContainer: 8},
		operator.PromoteLearner{ToContainer: 8},
		operator.TransferLeader{FromContainer: 2, ToContainer: 1},
		operator.RemovePeer{FromContainer: 2},
		operator.MergeResource{
			FromResource: s.resources[2].Meta,
			ToResource:   s.resources[1].Meta,
			IsPassive:    false,
		},
	})
	s.checkSteps(t, ops[1], []operator.OpStep{
		operator.MergeResource{
			FromResource: s.resources[2].Meta,
			ToResource:   s.resources[1].Meta,
			IsPassive:    true,
		},
	})
}

func TestCache(t *testing.T) {
	s := &testMergeChecker{}
	s.setup()
	defer s.cancel()

	cfg := config.NewTestOptions()
	s.cluster = mockcluster.NewCluster(cfg)
	s.cluster.SetMaxMergeResourceSize(2)
	s.cluster.SetMaxMergeResourceKeys(2)
	s.cluster.SetSplitMergeInterval(time.Hour)
	containers := map[uint64][]string{
		1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {},
	}
	for ContainerID, labels := range containers {
		s.cluster.PutContainerWithLabels(ContainerID, labels...)
	}
	s.resources = []*core.CachedResource{
		core.NewCachedResource(
			&metadata.TestResource{
				ResID: 2,
				Start: []byte("a"),
				End:   []byte("t"),
				ResPeers: []metapb.Peer{
					{ID: 103, ContainerID: 1},
					{ID: 104, ContainerID: 4},
					{ID: 105, ContainerID: 5},
				},
			},
			&metapb.Peer{ID: 104, ContainerID: 4},
			core.SetApproximateSize(200),
			core.SetApproximateKeys(200),
		),
		core.NewCachedResource(
			&metadata.TestResource{
				ResID: 3,
				Start: []byte("t"),
				End:   []byte("x"),
				ResPeers: []metapb.Peer{
					{ID: 106, ContainerID: 2},
					{ID: 107, ContainerID: 5},
					{ID: 108, ContainerID: 6},
				},
			},
			&metapb.Peer{ID: 108, ContainerID: 6},
			core.SetApproximateSize(1),
			core.SetApproximateKeys(1),
		),
	}

	for _, res := range s.resources {
		s.cluster.PutResource(res)
	}

	s.mc = NewMergeChecker(s.ctx, s.cluster)
	ops := s.mc.Check(s.resources[1])
	assert.Empty(t, ops)
	s.cluster.SetSplitMergeInterval(0)
	time.Sleep(time.Second)
	ops = s.mc.Check(s.resources[1])
	assert.NotNil(t, ops)
}
