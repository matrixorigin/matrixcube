package operator

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/stretchr/testify/assert"
)

type testCreateOperator struct {
	cluster *mockcluster.Cluster
}

func (s *testCreateOperator) setup() {
	opts := config.NewTestOptions()
	opts.SetEnableJointConsensus(true)
	s.cluster = mockcluster.NewCluster(opts)
	s.cluster.SetLabelPropertyConfig(config.LabelPropertyConfig{
		opt.RejectLeader: {{Key: "noleader", Value: "true"}},
	})
	s.cluster.SetLocationLabels([]string{"zone", "host"})
	s.cluster.AddLabelsContainer(1, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsContainer(2, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsContainer(3, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsContainer(4, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsContainer(5, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsContainer(6, 0, map[string]string{"zone": "z1", "host": "h2"})
	s.cluster.AddLabelsContainer(7, 0, map[string]string{"zone": "z1", "host": "h2"})
	s.cluster.AddLabelsContainer(8, 0, map[string]string{"zone": "z2", "host": "h1"})
	s.cluster.AddLabelsContainer(9, 0, map[string]string{"zone": "z2", "host": "h2"})
	s.cluster.AddLabelsContainer(10, 0, map[string]string{"zone": "z3", "host": "h1", "noleader": "true"})
}

func TestCreateSplitResourceOperator(t *testing.T) {
	s := &testCreateOperator{}
	s.setup()

	type testCase struct {
		startKey      []byte
		endKey        []byte
		originPeers   []metapb.Peer // first is leader
		policy        metapb.CheckPolicy
		keys          [][]byte
		expectedError bool
	}
	cases := []testCase{
		{
			startKey: []byte("a"),
			endKey:   []byte("b"),
			originPeers: []metapb.Peer{
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
			},
			policy:        metapb.CheckPolicy_APPROXIMATE,
			expectedError: false,
		},
		{
			startKey: []byte("c"),
			endKey:   []byte("d"),
			originPeers: []metapb.Peer{
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
			},
			policy:        metapb.CheckPolicy_SCAN,
			expectedError: false,
		},
		{
			startKey: []byte("e"),
			endKey:   []byte("h"),
			originPeers: []metapb.Peer{
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
			},
			policy:        metapb.CheckPolicy_USEKEY,
			keys:          [][]byte{[]byte("f"), []byte("g")},
			expectedError: false,
		},
		{
			startKey: []byte("i"),
			endKey:   []byte("j"),
			originPeers: []metapb.Peer{
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_IncomingVoter},
			},
			policy:        metapb.CheckPolicy_APPROXIMATE,
			expectedError: true,
		},
		{
			startKey: []byte("k"),
			endKey:   []byte("l"),
			originPeers: []metapb.Peer{
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_DemotingVoter},
			},
			policy:        metapb.CheckPolicy_APPROXIMATE,
			expectedError: true,
		},
	}

	for _, tc := range cases {
		resource := core.NewCachedResource(&metadata.TestResource{
			ResID:    1,
			Start:    tc.startKey,
			End:      tc.endKey,
			ResPeers: tc.originPeers,
		}, &tc.originPeers[0])
		op, err := CreateSplitResourceOperator("test", resource, 0, tc.policy, tc.keys)
		if tc.expectedError {
			assert.Error(t, err)
			continue
		}
		assert.NoError(t, err)
		assert.Equal(t, OpSplit, op.Kind())
		assert.Equal(t, 1, len(op.steps))
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case SplitResource:
				assert.True(t, reflect.DeepEqual(tc.startKey, step.StartKey))
				assert.True(t, reflect.DeepEqual(tc.endKey, step.EndKey))
				assert.Equal(t, tc.policy, step.Policy)
				assert.True(t, reflect.DeepEqual(tc.keys, step.SplitKeys))
			default:
				t.Fatalf("unexpected type: %s", step.String())
			}
		}
	}
}

func TestCreateMergeResourceOperator(t *testing.T) {
	s := &testCreateOperator{}
	s.setup()

	type testCase struct {
		sourcePeers   []metapb.Peer // first is leader
		targetPeers   []metapb.Peer // first is leader
		kind          OpKind
		expectedError bool
		prepareSteps  []OpStep
	}
	cases := []testCase{
		{
			[]metapb.Peer{
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_Voter},
			},
			[]metapb.Peer{
				{ID: 3, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 4, ContainerID: 2, Role: metapb.PeerRole_Voter},
			},
			OpMerge,
			false,
			[]OpStep{},
		},
		{
			[]metapb.Peer{
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_Voter},
			},
			[]metapb.Peer{
				{ID: 4, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
			},
			OpMerge | OpLeader | OpResource,
			false,
			[]OpStep{
				AddLearner{ToContainer: 3},
				TransferLeader{FromContainer: 1, ToContainer: 2},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToContainer: 3}},
					DemoteVoters:    []DemoteVoter{{ToContainer: 1}},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToContainer: 3}},
					DemoteVoters:    []DemoteVoter{{ToContainer: 1}},
				},
				RemovePeer{FromContainer: 1},
			},
		},
		{
			[]metapb.Peer{
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_DemotingVoter},
			},
			[]metapb.Peer{
				{ID: 3, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 4, ContainerID: 2, Role: metapb.PeerRole_Voter},
			},
			0,
			true,
			nil,
		},
		{
			[]metapb.Peer{
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_Voter},
			},
			[]metapb.Peer{
				{ID: 3, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 4, ContainerID: 2, Role: metapb.PeerRole_IncomingVoter},
			},
			0,
			true,
			nil,
		},
	}

	for _, tc := range cases {
		source := core.NewCachedResource(&metadata.TestResource{ResID: 68, ResPeers: tc.sourcePeers}, &tc.sourcePeers[0])
		target := core.NewCachedResource(&metadata.TestResource{ResID: 86, ResPeers: tc.targetPeers}, &tc.targetPeers[0])
		ops, err := CreateMergeResourceOperator("test", s.cluster, source, target, 0)
		if tc.expectedError {
			assert.Error(t, err)
			continue
		}
		assert.NoError(t, err)
		assert.Equal(t, len(ops), 2)
		assert.Equal(t, ops[0].kind, tc.kind)
		assert.Equal(t, ops[0].Len(), len(tc.prepareSteps)+1)
		assert.Equal(t, ops[1].kind, tc.kind)
		assert.Equal(t, ops[1].Len(), 1)
		assert.True(t, reflect.DeepEqual(ops[1].Step(0).(MergeResource), MergeResource{source.Meta, target.Meta, true}))

		expectedSteps := append(tc.prepareSteps, MergeResource{source.Meta, target.Meta, false})
		for i := 0; i < ops[0].Len(); i++ {
			switch step := ops[0].Step(i).(type) {
			case TransferLeader:
				assert.Equal(t, step.FromContainer, expectedSteps[i].(TransferLeader).FromContainer)
				assert.Equal(t, step.ToContainer, expectedSteps[i].(TransferLeader).ToContainer)
			case AddLearner:
				assert.Equal(t, step.ToContainer, expectedSteps[i].(AddLearner).ToContainer)
			case RemovePeer:
				assert.Equal(t, step.FromContainer, expectedSteps[i].(RemovePeer).FromContainer)
			case ChangePeerV2Enter:
				assert.Equal(t, len(step.PromoteLearners), len(expectedSteps[i].(ChangePeerV2Enter).PromoteLearners))
				assert.Equal(t, len(step.DemoteVoters), len(expectedSteps[i].(ChangePeerV2Enter).DemoteVoters))
				for j, p := range expectedSteps[i].(ChangePeerV2Enter).PromoteLearners {
					assert.Equal(t, step.PromoteLearners[j].ToContainer, p.ToContainer)
				}
				for j, d := range expectedSteps[i].(ChangePeerV2Enter).DemoteVoters {
					assert.Equal(t, step.DemoteVoters[j].ToContainer, d.ToContainer)
				}
			case ChangePeerV2Leave:
				assert.Equal(t, len(step.PromoteLearners), len(expectedSteps[i].(ChangePeerV2Leave).PromoteLearners))
				assert.Equal(t, len(step.DemoteVoters), len(expectedSteps[i].(ChangePeerV2Leave).DemoteVoters))
				for j, p := range expectedSteps[i].(ChangePeerV2Leave).PromoteLearners {
					assert.Equal(t, step.PromoteLearners[j].ToContainer, p.ToContainer)
				}
				for j, d := range expectedSteps[i].(ChangePeerV2Leave).DemoteVoters {
					assert.Equal(t, step.DemoteVoters[j].ToContainer, d.ToContainer)
				}
			case MergeResource:
				assert.True(t, reflect.DeepEqual(expectedSteps[i].(MergeResource), step))
			}
		}
	}
}

func TestCreateLeaveJointStateOperator(t *testing.T) {
	s := &testCreateOperator{}
	s.setup()

	type testCase struct {
		originPeers []metapb.Peer // first is leader
		kind        OpKind
		steps       []OpStep // empty means error
	}
	cases := []testCase{
		{
			originPeers: []metapb.Peer{
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_DemotingVoter},
				{ID: 4, ContainerID: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			kind: 0,
			steps: []OpStep{
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToContainer: 4}},
					DemoteVoters:    []DemoteVoter{{ToContainer: 3}},
				},
			},
		},
		{
			originPeers: []metapb.Peer{
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_IncomingVoter},
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_DemotingVoter},
				{ID: 4, ContainerID: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			kind: 0,
			steps: []OpStep{
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToContainer: 1}, {ToContainer: 4}},
					DemoteVoters:    []DemoteVoter{{ToContainer: 3}},
				},
			},
		},
		{
			originPeers: []metapb.Peer{
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_DemotingVoter},
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_DemotingVoter},
				{ID: 4, ContainerID: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			kind: OpLeader,
			steps: []OpStep{
				TransferLeader{FromContainer: 1, ToContainer: 2},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToContainer: 4}},
					DemoteVoters:    []DemoteVoter{{ToContainer: 1}, {ToContainer: 3}},
				},
			},
		},
		{
			originPeers: []metapb.Peer{
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_DemotingVoter},
				{ID: 10, ContainerID: 10, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_DemotingVoter},
				{ID: 4, ContainerID: 4, Role: metapb.PeerRole_IncomingVoter},
			},
			kind: OpLeader,
			steps: []OpStep{
				TransferLeader{FromContainer: 1, ToContainer: 4},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToContainer: 4}},
					DemoteVoters:    []DemoteVoter{{ToContainer: 1}, {ToContainer: 3}},
				},
			},
		},
	}

	for _, tc := range cases {
		resource := core.NewCachedResource(&metadata.TestResource{ResID: 1, ResPeers: tc.originPeers}, &tc.originPeers[0])
		op, err := CreateLeaveJointStateOperator("test", s.cluster, resource)
		if len(tc.steps) == 0 {
			assert.Error(t, err)
			continue
		}
		assert.NoError(t, err)
		assert.Equal(t, op.Kind(), tc.kind)
		assert.Equal(t, len(op.steps), len(tc.steps))
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case TransferLeader:
				assert.Equal(t, step.FromContainer, tc.steps[i].(TransferLeader).FromContainer)
				assert.Equal(t, step.ToContainer, tc.steps[i].(TransferLeader).ToContainer)
			case ChangePeerV2Leave:
				assert.Equal(t, len(step.PromoteLearners), len(tc.steps[i].(ChangePeerV2Leave).PromoteLearners))
				assert.Equal(t, len(step.DemoteVoters), len(tc.steps[i].(ChangePeerV2Leave).DemoteVoters))
				for j, p := range tc.steps[i].(ChangePeerV2Leave).PromoteLearners {
					assert.Equal(t, step.PromoteLearners[j].ToContainer, p.ToContainer)
				}
				for j, d := range tc.steps[i].(ChangePeerV2Leave).DemoteVoters {
					assert.Equal(t, step.DemoteVoters[j].ToContainer, d.ToContainer)
				}
			default:
				t.Fatalf("unexpected type: %s", step.String())
			}
		}
	}
}

func TestCreateMoveresourceOperator(t *testing.T) {
	s := &testCreateOperator{}
	s.setup()

	type testCase struct {
		name            string
		originPeers     []metapb.Peer // first is leader
		targetPeerRoles map[uint64]placement.PeerRoleType
		steps           []OpStep
		expectedError   error
	}
	tt := []testCase{
		{
			name: "move resource partially with incoming voter, demote existed voter",
			originPeers: []metapb.Peer{
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Leader,
				3: placement.Learner,
				4: placement.Voter,
			},
			steps: []OpStep{
				AddLearner{ToContainer: 4, PeerID: 4},
				TransferLeader{FromContainer: 1, ToContainer: 2},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToContainer: 4, PeerID: 4}},
					DemoteVoters: []DemoteVoter{
						{ToContainer: 1, PeerID: 1},
						{ToContainer: 3, PeerID: 3},
					},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToContainer: 4, PeerID: 4}},
					DemoteVoters: []DemoteVoter{
						{ToContainer: 1, PeerID: 1},
						{ToContainer: 3, PeerID: 3},
					},
				},
				RemovePeer{FromContainer: 1, PeerID: 1},
			},
			expectedError: nil,
		},
		{
			name: "move resource partially with incoming leader",
			originPeers: []metapb.Peer{
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Voter,
				3: placement.Voter,
				4: placement.Leader,
			},
			steps: []OpStep{
				AddLearner{ToContainer: 4, PeerID: 4},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToContainer: 4, PeerID: 4}},
					DemoteVoters:    []DemoteVoter{{ToContainer: 1, PeerID: 1}},
				},
				TransferLeader{FromContainer: 1, ToContainer: 4},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToContainer: 4, PeerID: 4}},
					DemoteVoters:    []DemoteVoter{{ToContainer: 1, PeerID: 1}},
				},
				RemovePeer{FromContainer: 1, PeerID: 1},
			},
			expectedError: nil,
		},
		{
			name: "move resource partially with incoming voter",
			originPeers: []metapb.Peer{
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Voter,
				3: placement.Voter,
				4: placement.Voter,
			},
			steps: []OpStep{
				AddLearner{ToContainer: 4, PeerID: 4},
				TransferLeader{FromContainer: 1, ToContainer: 2},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToContainer: 4, PeerID: 4}},
					DemoteVoters:    []DemoteVoter{{ToContainer: 1, PeerID: 1}},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToContainer: 4, PeerID: 4}},
					DemoteVoters:    []DemoteVoter{{ToContainer: 1, PeerID: 1}},
				},
				RemovePeer{FromContainer: 1, PeerID: 1},
			},
			expectedError: nil,
		},
		{
			name: "move resource partially with incoming learner, demote leader",
			originPeers: []metapb.Peer{
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Learner,
				3: placement.Voter,
				4: placement.Learner,
			},
			steps: []OpStep{
				AddLearner{ToContainer: 4, PeerID: 4},
				TransferLeader{FromContainer: 2, ToContainer: 3},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{},
					DemoteVoters: []DemoteVoter{
						{ToContainer: 1, PeerID: 1},
						{ToContainer: 2, PeerID: 2},
					},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{},
					DemoteVoters: []DemoteVoter{
						{ToContainer: 1, PeerID: 1},
						{ToContainer: 2, PeerID: 2},
					},
				},
				RemovePeer{FromContainer: 1, PeerID: 1},
			},
			expectedError: nil,
		},
		{
			name: "move entirely with incoming voter",
			originPeers: []metapb.Peer{
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				4: placement.Leader,
				5: placement.Voter,
				6: placement.Voter,
			},
			steps: []OpStep{
				AddLearner{ToContainer: 4, PeerID: 4},
				AddLearner{ToContainer: 5, PeerID: 5},
				AddLearner{ToContainer: 6, PeerID: 6},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{
						{ToContainer: 4, PeerID: 4},
						{ToContainer: 5, PeerID: 5},
						{ToContainer: 6, PeerID: 6},
					},
					DemoteVoters: []DemoteVoter{
						{ToContainer: 1, PeerID: 1},
						{ToContainer: 2, PeerID: 2},
						{ToContainer: 3, PeerID: 3},
					},
				},
				TransferLeader{FromContainer: 1, ToContainer: 4},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{
						{ToContainer: 4, PeerID: 4},
						{ToContainer: 5, PeerID: 5},
						{ToContainer: 6, PeerID: 6},
					},
					DemoteVoters: []DemoteVoter{
						{ToContainer: 1, PeerID: 1},
						{ToContainer: 2, PeerID: 2},
						{ToContainer: 3, PeerID: 3},
					},
				},
				RemovePeer{FromContainer: 1, PeerID: 1},
				RemovePeer{FromContainer: 2, PeerID: 2},
				RemovePeer{FromContainer: 3, PeerID: 3},
			},
			expectedError: nil,
		},
		{
			name: "move resource partially with incoming and old voter, leader step down",
			originPeers: []metapb.Peer{
				{ID: 4, ContainerID: 4, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
				{ID: 5, ContainerID: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Voter,
				3: placement.Voter,
				4: placement.Follower,
			},
			steps: []OpStep{
				AddLearner{ToContainer: 2, PeerID: 8},
				TransferLeader{FromContainer: 4, ToContainer: 3},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{
						{ToContainer: 2, PeerID: 8},
					},
					DemoteVoters: []DemoteVoter{
						{ToContainer: 5, PeerID: 5},
					},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{
						{ToContainer: 2, PeerID: 8},
					},
					DemoteVoters: []DemoteVoter{
						{ToContainer: 5, PeerID: 5},
					},
				},
				RemovePeer{FromContainer: 5, PeerID: 5},
			},
			expectedError: nil,
		},
		{
			name: "move resource partially with incoming voter and follower, leader step down",
			originPeers: []metapb.Peer{
				{ID: 4, ContainerID: 4, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
				{ID: 5, ContainerID: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				1: placement.Follower,
				2: placement.Voter,
				4: placement.Follower,
			},
			steps: []OpStep{
				AddLearner{ToContainer: 1, PeerID: 9},
				AddLearner{ToContainer: 2, PeerID: 10},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{
						{ToContainer: 1, PeerID: 9},
						{ToContainer: 2, PeerID: 10},
					},
					DemoteVoters: []DemoteVoter{
						{ToContainer: 3, PeerID: 3},
						{ToContainer: 5, PeerID: 5},
					},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{
						{ToContainer: 1, PeerID: 9},
						{ToContainer: 2, PeerID: 10},
					},
					DemoteVoters: []DemoteVoter{
						{ToContainer: 3, PeerID: 3},
						{ToContainer: 5, PeerID: 5},
					},
				},
				TransferLeader{FromContainer: 4, ToContainer: 2},
				RemovePeer{FromContainer: 3, PeerID: 3},
				RemovePeer{FromContainer: 5, PeerID: 5},
			},
			expectedError: nil,
		},
		{
			name: "move resource partially with all incoming follower, leader step down",
			originPeers: []metapb.Peer{
				{ID: 4, ContainerID: 4, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
				{ID: 5, ContainerID: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				1: placement.Follower,
				2: placement.Follower,
				4: placement.Follower,
			},
			steps:         []OpStep{},
			expectedError: errors.New("resource need at least 1 voter or leader"),
		},
		{
			name: "only leader transfer",
			originPeers: []metapb.Peer{
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
				{ID: 4, ContainerID: 4, Role: metapb.PeerRole_Voter},
				{ID: 5, ContainerID: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				3: placement.Follower,
				4: placement.Voter,
				5: placement.Follower,
			},
			steps: []OpStep{
				TransferLeader{FromContainer: 3, ToContainer: 4},
			},
		},
		{
			name: "add peer and transfer leader",
			originPeers: []metapb.Peer{
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
				{ID: 4, ContainerID: 4, Role: metapb.PeerRole_Voter},
				{ID: 5, ContainerID: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				3: placement.Follower,
				4: placement.Voter,
				5: placement.Follower,
				6: placement.Follower,
			},
			steps: []OpStep{
				AddLearner{ToContainer: 6},
				PromoteLearner{ToContainer: 6},
				TransferLeader{FromContainer: 3, ToContainer: 4},
			},
		},
	}
	for _, tc := range tt {
		t.Log(tc.name)
		resource := core.NewCachedResource(&metadata.TestResource{ResID: 10, ResPeers: tc.originPeers}, &tc.originPeers[0])
		op, err := CreateMoveResourceOperator("test", s.cluster, resource, OpAdmin, tc.targetPeerRoles)

		if tc.expectedError == nil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), tc.expectedError.Error()))
			continue
		}
		assert.NotNil(t, op)
		assert.Equal(t, op.Len(), len(tc.steps))
		// Since the peer id may be generated by allocator in runtime, we only check container id.
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case TransferLeader:
				assert.Equal(t, step.FromContainer, tc.steps[i].(TransferLeader).FromContainer)
				assert.Equal(t, step.ToContainer, tc.steps[i].(TransferLeader).ToContainer)
			case ChangePeerV2Leave:
				assert.Equal(t, len(step.PromoteLearners), len(tc.steps[i].(ChangePeerV2Leave).PromoteLearners))
				assert.Equal(t, len(step.DemoteVoters), len(tc.steps[i].(ChangePeerV2Leave).DemoteVoters))
				for j, p := range tc.steps[i].(ChangePeerV2Leave).PromoteLearners {
					assert.Equal(t, step.PromoteLearners[j].ToContainer, p.ToContainer)
				}
				for j, d := range tc.steps[i].(ChangePeerV2Leave).DemoteVoters {
					assert.Equal(t, step.DemoteVoters[j].ToContainer, d.ToContainer)
				}
			case ChangePeerV2Enter:
				assert.Equal(t, len(step.PromoteLearners), len(tc.steps[i].(ChangePeerV2Enter).PromoteLearners))
				assert.Equal(t, len(step.DemoteVoters), len(tc.steps[i].(ChangePeerV2Enter).DemoteVoters))
				for j, p := range tc.steps[i].(ChangePeerV2Enter).PromoteLearners {
					assert.Equal(t, step.PromoteLearners[j].ToContainer, p.ToContainer)
				}
				for j, d := range tc.steps[i].(ChangePeerV2Enter).DemoteVoters {
					assert.Equal(t, step.DemoteVoters[j].ToContainer, d.ToContainer)
				}
			case AddLearner:
				assert.Equal(t, step.ToContainer, tc.steps[i].(AddLearner).ToContainer)
			case PromoteLearner:
				assert.Equal(t, step.ToContainer, tc.steps[i].(PromoteLearner).ToContainer)
			case RemovePeer:
				assert.Equal(t, step.FromContainer, tc.steps[i].(RemovePeer).FromContainer)
			default:
				t.Fatalf("unexpected type: %s", step.String())
			}
		}
	}
}

func TestMoveresourceWithoutJointConsensus(t *testing.T) {
	s := &testCreateOperator{}
	s.setup()

	type testCase struct {
		name            string
		originPeers     []metapb.Peer // first is leader
		targetPeerRoles map[uint64]placement.PeerRoleType
		steps           []OpStep
		expectedError   error
	}
	tt := []testCase{
		{
			name: "move resource partially with incoming voter, demote existed voter",
			originPeers: []metapb.Peer{
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Leader,
				3: placement.Learner,
				4: placement.Voter,
			},
			steps: []OpStep{
				AddLearner{ToContainer: 4},
				PromoteLearner{ToContainer: 4},
				TransferLeader{FromContainer: 1, ToContainer: 2},
				RemovePeer{FromContainer: 1},
				RemovePeer{FromContainer: 3},
				AddLearner{ToContainer: 3},
			},
		},
		{
			name: "move resource partially with incoming leader",
			originPeers: []metapb.Peer{
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Voter,
				3: placement.Voter,
				4: placement.Leader,
			},
			steps: []OpStep{
				AddLearner{ToContainer: 4},
				PromoteLearner{ToContainer: 4},
				TransferLeader{FromContainer: 1, ToContainer: 2},
				RemovePeer{FromContainer: 1},
				TransferLeader{FromContainer: 2, ToContainer: 4},
			},
		},
		{
			name: "move resource partially with incoming learner, demote leader",
			originPeers: []metapb.Peer{
				{ID: 2, ContainerID: 2, Role: metapb.PeerRole_Voter},
				{ID: 1, ContainerID: 1, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				2: placement.Learner,
				3: placement.Voter,
				4: placement.Learner,
			},
			steps: []OpStep{
				RemovePeer{FromContainer: 1},
				TransferLeader{FromContainer: 2, ToContainer: 3},
				RemovePeer{FromContainer: 2},
				AddLearner{ToContainer: 2},
				AddLearner{ToContainer: 4},
			},
		},
		{
			name: "move resource partially with all incoming follower, leader step down",
			originPeers: []metapb.Peer{
				{ID: 4, ContainerID: 4, Role: metapb.PeerRole_Voter},
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
				{ID: 5, ContainerID: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				1: placement.Follower,
				2: placement.Follower,
				4: placement.Follower,
			},
			steps:         []OpStep{},
			expectedError: errors.New("resource need at least 1 voter or leader"),
		},
		{
			name: "only leader transfer",
			originPeers: []metapb.Peer{
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
				{ID: 4, ContainerID: 4, Role: metapb.PeerRole_Voter},
				{ID: 5, ContainerID: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				3: placement.Follower,
				4: placement.Voter,
				5: placement.Follower,
			},
			steps: []OpStep{
				TransferLeader{FromContainer: 3, ToContainer: 4},
			},
		},
		{
			name: "add peer and transfer leader",
			originPeers: []metapb.Peer{
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Voter},
				{ID: 4, ContainerID: 4, Role: metapb.PeerRole_Voter},
				{ID: 5, ContainerID: 5, Role: metapb.PeerRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.PeerRoleType{
				3: placement.Follower,
				4: placement.Voter,
				5: placement.Follower,
				6: placement.Follower,
			},
			steps: []OpStep{
				AddLearner{ToContainer: 6},
				PromoteLearner{ToContainer: 6},
				TransferLeader{FromContainer: 3, ToContainer: 4},
			},
		},
	}

	s.cluster.DisableJointConsensus()
	for _, tc := range tt {
		t.Log(tc.name)
		resource := core.NewCachedResource(&metadata.TestResource{ResID: 10, ResPeers: tc.originPeers}, &tc.originPeers[0])
		op, err := CreateMoveResourceOperator("test", s.cluster, resource, OpAdmin, tc.targetPeerRoles)

		if tc.expectedError == nil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), tc.expectedError.Error()))
			continue
		}
		t.Log(op)
		assert.NotNil(t, op)
		assert.Equal(t, op.Len(), len(tc.steps))
		// Since the peer id may be generated by allocator in runtime, we only check container id.
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case TransferLeader:
				assert.Equal(t, step.FromContainer, tc.steps[i].(TransferLeader).FromContainer)
				assert.Equal(t, step.ToContainer, tc.steps[i].(TransferLeader).ToContainer)
			case AddLearner:
				assert.Equal(t, step.ToContainer, tc.steps[i].(AddLearner).ToContainer)
			case PromoteLearner:
				assert.Equal(t, step.ToContainer, tc.steps[i].(PromoteLearner).ToContainer)
			case RemovePeer:
				assert.Equal(t, step.FromContainer, tc.steps[i].(RemovePeer).FromContainer)
			default:
				t.Fatalf("unexpected type: %s", step.String())
			}
		}
	}
}
