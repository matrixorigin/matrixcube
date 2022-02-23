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

package operator

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

type testCreateOperator struct {
	cluster *mockcluster.Cluster
}

func (s *testCreateOperator) setup() {
	opts := config.NewTestOptions()
	opts.GetScheduleConfig().EnableJointConsensus = true
	s.cluster = mockcluster.NewCluster(opts)
	s.cluster.SetLabelPropertyConfig(config.LabelPropertyConfig{
		opt.RejectLeader: {{Key: "noleader", Value: "true"}},
	})
	s.cluster.SetLocationLabels([]string{"zone", "host"})
	s.cluster.AddLabelsStore(1, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsStore(2, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsStore(3, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsStore(4, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsStore(5, 0, map[string]string{"zone": "z1", "host": "h1"})
	s.cluster.AddLabelsStore(6, 0, map[string]string{"zone": "z1", "host": "h2"})
	s.cluster.AddLabelsStore(7, 0, map[string]string{"zone": "z1", "host": "h2"})
	s.cluster.AddLabelsStore(8, 0, map[string]string{"zone": "z2", "host": "h1"})
	s.cluster.AddLabelsStore(9, 0, map[string]string{"zone": "z2", "host": "h2"})
	s.cluster.AddLabelsStore(10, 0, map[string]string{"zone": "z3", "host": "h1", "noleader": "true"})
}

func TestCreateSplitShardOperator(t *testing.T) {
	s := &testCreateOperator{}
	s.setup()

	type testCase struct {
		startKey      []byte
		endKey        []byte
		originPeers   []metapb.Replica // first is leader
		policy        metapb.CheckPolicy
		keys          [][]byte
		expectedError bool
	}
	cases := []testCase{
		{
			startKey: []byte("a"),
			endKey:   []byte("b"),
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			policy:        metapb.CheckPolicy_APPROXIMATE,
			expectedError: false,
		},
		{
			startKey: []byte("c"),
			endKey:   []byte("d"),
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			policy:        metapb.CheckPolicy_SCAN,
			expectedError: false,
		},
		{
			startKey: []byte("e"),
			endKey:   []byte("h"),
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			policy:        metapb.CheckPolicy_USEKEY,
			keys:          [][]byte{[]byte("f"), []byte("g")},
			expectedError: false,
		},
		{
			startKey: []byte("i"),
			endKey:   []byte("j"),
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_IncomingVoter},
			},
			policy:        metapb.CheckPolicy_APPROXIMATE,
			expectedError: true,
		},
		{
			startKey: []byte("k"),
			endKey:   []byte("l"),
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_DemotingVoter},
			},
			policy:        metapb.CheckPolicy_APPROXIMATE,
			expectedError: true,
		},
	}

	for _, tc := range cases {
		resource := core.NewCachedShard(&metapb.Shard{
			ID:       1,
			Start:    tc.startKey,
			End:      tc.endKey,
			Replicas: tc.originPeers,
		}, &tc.originPeers[0])
		op, err := CreateSplitShardOperator("test", resource, 0, tc.policy, tc.keys)
		if tc.expectedError {
			assert.Error(t, err)
			continue
		}
		assert.NoError(t, err)
		assert.Equal(t, OpSplit, op.Kind())
		assert.Equal(t, 1, len(op.steps))
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case SplitShard:
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

func TestCreateMergeShardOperator(t *testing.T) {
	s := &testCreateOperator{}
	s.setup()

	type testCase struct {
		sourcePeers   []metapb.Replica // first is leader
		targetPeers   []metapb.Replica // first is leader
		kind          OpKind
		expectedError bool
		prepareSteps  []OpStep
	}
	cases := []testCase{
		{
			[]metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
			},
			[]metapb.Replica{
				{ID: 3, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 4, StoreID: 2, Role: metapb.ReplicaRole_Voter},
			},
			OpMerge,
			false,
			[]OpStep{},
		},
		{
			[]metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
			},
			[]metapb.Replica{
				{ID: 4, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			OpMerge | OpLeader | OpShard,
			false,
			[]OpStep{
				AddLearner{ToStore: 3},
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToStore: 3}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 3}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
				RemovePeer{FromStore: 1},
			},
		},
		{
			[]metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_DemotingVoter},
			},
			[]metapb.Replica{
				{ID: 3, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 4, StoreID: 2, Role: metapb.ReplicaRole_Voter},
			},
			0,
			true,
			nil,
		},
		{
			[]metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
			},
			[]metapb.Replica{
				{ID: 3, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 4, StoreID: 2, Role: metapb.ReplicaRole_IncomingVoter},
			},
			0,
			true,
			nil,
		},
	}

	for _, tc := range cases {
		source := core.NewCachedShard(&metapb.Shard{ID: 68, Replicas: tc.sourcePeers}, &tc.sourcePeers[0])
		target := core.NewCachedShard(&metapb.Shard{ID: 86, Replicas: tc.targetPeers}, &tc.targetPeers[0])
		ops, err := CreateMergeShardOperator("test", s.cluster, source, target, 0)
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
		assert.True(t, reflect.DeepEqual(ops[1].Step(0).(MergeShard), MergeShard{source.Meta, target.Meta, true}))

		expectedSteps := append(tc.prepareSteps, MergeShard{source.Meta, target.Meta, false})
		for i := 0; i < ops[0].Len(); i++ {
			switch step := ops[0].Step(i).(type) {
			case TransferLeader:
				assert.Equal(t, step.FromStore, expectedSteps[i].(TransferLeader).FromStore)
				assert.Equal(t, step.ToStore, expectedSteps[i].(TransferLeader).ToStore)
			case AddLearner:
				assert.Equal(t, step.ToStore, expectedSteps[i].(AddLearner).ToStore)
			case RemovePeer:
				assert.Equal(t, step.FromStore, expectedSteps[i].(RemovePeer).FromStore)
			case ChangePeerV2Enter:
				assert.Equal(t, len(step.PromoteLearners), len(expectedSteps[i].(ChangePeerV2Enter).PromoteLearners))
				assert.Equal(t, len(step.DemoteVoters), len(expectedSteps[i].(ChangePeerV2Enter).DemoteVoters))
				for j, p := range expectedSteps[i].(ChangePeerV2Enter).PromoteLearners {
					assert.Equal(t, step.PromoteLearners[j].ToStore, p.ToStore)
				}
				for j, d := range expectedSteps[i].(ChangePeerV2Enter).DemoteVoters {
					assert.Equal(t, step.DemoteVoters[j].ToStore, d.ToStore)
				}
			case ChangePeerV2Leave:
				assert.Equal(t, len(step.PromoteLearners), len(expectedSteps[i].(ChangePeerV2Leave).PromoteLearners))
				assert.Equal(t, len(step.DemoteVoters), len(expectedSteps[i].(ChangePeerV2Leave).DemoteVoters))
				for j, p := range expectedSteps[i].(ChangePeerV2Leave).PromoteLearners {
					assert.Equal(t, step.PromoteLearners[j].ToStore, p.ToStore)
				}
				for j, d := range expectedSteps[i].(ChangePeerV2Leave).DemoteVoters {
					assert.Equal(t, step.DemoteVoters[j].ToStore, d.ToStore)
				}
			case MergeShard:
				assert.True(t, reflect.DeepEqual(expectedSteps[i].(MergeShard), step))
			}
		}
	}
}

func TestCreateTransferLeaderOperator(t *testing.T) {
	s := &testCreateOperator{}
	s.setup()

	type testCase struct {
		originPeers         []metapb.Replica // first is leader
		targetLeaderStoreID uint64
		isErr               bool
	}
	cases := []testCase{
		{
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			targetLeaderStoreID: 3,
			isErr:               false,
		},
		{
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			targetLeaderStoreID: 1,
			isErr:               true,
		},
		{
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			targetLeaderStoreID: 4,
			isErr:               true,
		},
		{
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Learner},
			},
			targetLeaderStoreID: 3,
			isErr:               true,
		},
		{
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
			},
			targetLeaderStoreID: 3,
			isErr:               true,
		},
		{
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
			},
			targetLeaderStoreID: 4,
			isErr:               false,
		},
		{
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
			},
			targetLeaderStoreID: 3,
			isErr:               false,
		},
	}
	for _, tc := range cases {
		region := core.NewCachedShard(&metapb.Shard{ID: 1, Replicas: tc.originPeers}, &tc.originPeers[0])
		op, err := CreateTransferLeaderOperator("test", s.cluster, region, tc.originPeers[0].StoreID, tc.targetLeaderStoreID, 0)

		if tc.isErr {
			assert.Error(t, err)
			continue
		}

		assert.NoError(t, err)
		assert.Equal(t, OpLeader, op.Kind())
		assert.Equal(t, 1, len(op.steps))
		switch step := op.Step(0).(type) {
		case TransferLeader:
			assert.Equal(t, tc.originPeers[0].StoreID, step.FromStore)
			assert.Equal(t, tc.targetLeaderStoreID, step.ToStore)
		default:
			assert.Failf(t, "unexpected type: %s", step.String())
		}
	}
}

func TestCreateLeaveJointStateOperator(t *testing.T) {
	s := &testCreateOperator{}
	s.setup()

	type testCase struct {
		originPeers   []metapb.Replica // first is leader
		offlineStores []uint64
		kind          OpKind
		steps         []OpStep // empty means error
	}
	cases := []testCase{
		{
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
			},
			kind: 0,
			steps: []OpStep{
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 3}},
				},
			},
		},
		{
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
			},
			kind: OpLeader,
			steps: []OpStep{
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
			},
		},
		{
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
			},
			offlineStores: []uint64{2},
			kind:          OpLeader,
			steps: []OpStep{
				TransferLeader{FromStore: 1, ToStore: 3},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
			},
		},
		{
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
			},
			offlineStores: []uint64{2, 3},
			kind:          OpLeader,
			steps: []OpStep{
				TransferLeader{FromStore: 1, ToStore: 4},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
			},
		},
		{
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
			},
			offlineStores: []uint64{1, 2, 3, 4},
			kind:          OpLeader,
			steps: []OpStep{
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
			},
		},
		{
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_IncomingVoter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
			},
			kind: 0,
			steps: []OpStep{
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 1}, {ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 3}},
				},
			},
		},
		{
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
			},
			kind: OpLeader,
			steps: []OpStep{
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}, {ToStore: 3}},
				},
			},
		},
		{
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 10, StoreID: 10, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_IncomingVoter},
			},
			kind: OpLeader,
			steps: []OpStep{
				TransferLeader{FromStore: 1, ToStore: 4},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}, {ToStore: 3}},
				},
			},
		},
	}

	for _, tc := range cases {
		for _, containerID := range tc.offlineStores {
			s.cluster.SetStoreOffline(containerID)
		}

		revertOffline := func() {
			for _, storeID := range tc.offlineStores {
				s.cluster.SetStoreUP(storeID)
			}
		}

		resource := core.NewCachedShard(&metapb.Shard{ID: 1, Replicas: tc.originPeers}, &tc.originPeers[0])
		op, err := CreateLeaveJointStateOperator("test", s.cluster, resource)
		if len(tc.steps) == 0 {
			assert.Error(t, err)
			revertOffline()
			continue
		}
		assert.NoError(t, err)
		assert.Equal(t, op.Kind(), tc.kind)
		assert.Equal(t, len(op.steps), len(tc.steps))
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case TransferLeader:
				assert.Equal(t, step.FromStore, tc.steps[i].(TransferLeader).FromStore)
				assert.Equal(t, step.ToStore, tc.steps[i].(TransferLeader).ToStore)
			case ChangePeerV2Leave:
				assert.Equal(t, len(step.PromoteLearners), len(tc.steps[i].(ChangePeerV2Leave).PromoteLearners))
				assert.Equal(t, len(step.DemoteVoters), len(tc.steps[i].(ChangePeerV2Leave).DemoteVoters))
				for j, p := range tc.steps[i].(ChangePeerV2Leave).PromoteLearners {
					assert.Equal(t, step.PromoteLearners[j].ToStore, p.ToStore)
				}
				for j, d := range tc.steps[i].(ChangePeerV2Leave).DemoteVoters {
					assert.Equal(t, step.DemoteVoters[j].ToStore, d.ToStore)
				}
			default:
				t.Fatalf("unexpected type: %s", step.String())
			}
		}

		revertOffline()
	}
}

func TestCreateMoveresourceOperator(t *testing.T) {
	s := &testCreateOperator{}
	s.setup()

	type testCase struct {
		name            string
		originPeers     []metapb.Replica // first is leader
		targetPeerRoles map[uint64]placement.ReplicaRoleType
		steps           []OpStep
		expectedError   error
	}
	tt := []testCase{
		{
			name: "move resource partially with incoming voter, demote existed voter",
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.ReplicaRoleType{
				2: placement.Leader,
				3: placement.Learner,
				4: placement.Voter,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4, PeerID: 4},
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToStore: 4, PeerID: 4}},
					DemoteVoters: []DemoteVoter{
						{ToStore: 1, PeerID: 1},
						{ToStore: 3, PeerID: 3},
					},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4, PeerID: 4}},
					DemoteVoters: []DemoteVoter{
						{ToStore: 1, PeerID: 1},
						{ToStore: 3, PeerID: 3},
					},
				},
				RemovePeer{FromStore: 1, PeerID: 1},
			},
			expectedError: nil,
		},
		{
			name: "move resource partially with incoming leader",
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.ReplicaRoleType{
				2: placement.Voter,
				3: placement.Voter,
				4: placement.Leader,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4, PeerID: 4},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToStore: 4, PeerID: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1, PeerID: 1}},
				},
				TransferLeader{FromStore: 1, ToStore: 4},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4, PeerID: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1, PeerID: 1}},
				},
				RemovePeer{FromStore: 1, PeerID: 1},
			},
			expectedError: nil,
		},
		{
			name: "move resource partially with incoming voter",
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.ReplicaRoleType{
				2: placement.Voter,
				3: placement.Voter,
				4: placement.Voter,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4, PeerID: 4},
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToStore: 4, PeerID: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1, PeerID: 1}},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4, PeerID: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1, PeerID: 1}},
				},
				RemovePeer{FromStore: 1, PeerID: 1},
			},
			expectedError: nil,
		},
		{
			name: "move resource partially with incoming learner, demote leader",
			originPeers: []metapb.Replica{
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.ReplicaRoleType{
				2: placement.Learner,
				3: placement.Voter,
				4: placement.Learner,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4, PeerID: 4},
				TransferLeader{FromStore: 2, ToStore: 3},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{},
					DemoteVoters: []DemoteVoter{
						{ToStore: 1, PeerID: 1},
						{ToStore: 2, PeerID: 2},
					},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{},
					DemoteVoters: []DemoteVoter{
						{ToStore: 1, PeerID: 1},
						{ToStore: 2, PeerID: 2},
					},
				},
				RemovePeer{FromStore: 1, PeerID: 1},
			},
			expectedError: nil,
		},
		{
			name: "move entirely with incoming voter",
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.ReplicaRoleType{
				4: placement.Leader,
				5: placement.Voter,
				6: placement.Voter,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4, PeerID: 4},
				AddLearner{ToStore: 5, PeerID: 5},
				AddLearner{ToStore: 6, PeerID: 6},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{
						{ToStore: 4, PeerID: 4},
						{ToStore: 5, PeerID: 5},
						{ToStore: 6, PeerID: 6},
					},
					DemoteVoters: []DemoteVoter{
						{ToStore: 1, PeerID: 1},
						{ToStore: 2, PeerID: 2},
						{ToStore: 3, PeerID: 3},
					},
				},
				TransferLeader{FromStore: 1, ToStore: 4},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{
						{ToStore: 4, PeerID: 4},
						{ToStore: 5, PeerID: 5},
						{ToStore: 6, PeerID: 6},
					},
					DemoteVoters: []DemoteVoter{
						{ToStore: 1, PeerID: 1},
						{ToStore: 2, PeerID: 2},
						{ToStore: 3, PeerID: 3},
					},
				},
				RemovePeer{FromStore: 1, PeerID: 1},
				RemovePeer{FromStore: 2, PeerID: 2},
				RemovePeer{FromStore: 3, PeerID: 3},
			},
			expectedError: nil,
		},
		{
			name: "move resource partially with incoming and old voter, leader step down",
			originPeers: []metapb.Replica{
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
				{ID: 5, StoreID: 5, Role: metapb.ReplicaRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.ReplicaRoleType{
				2: placement.Voter,
				3: placement.Voter,
				4: placement.Follower,
			},
			steps: []OpStep{
				AddLearner{ToStore: 2, PeerID: 8},
				TransferLeader{FromStore: 4, ToStore: 3},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{
						{ToStore: 2, PeerID: 8},
					},
					DemoteVoters: []DemoteVoter{
						{ToStore: 5, PeerID: 5},
					},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{
						{ToStore: 2, PeerID: 8},
					},
					DemoteVoters: []DemoteVoter{
						{ToStore: 5, PeerID: 5},
					},
				},
				RemovePeer{FromStore: 5, PeerID: 5},
			},
			expectedError: nil,
		},
		{
			name: "move resource partially with incoming voter and follower, leader step down",
			originPeers: []metapb.Replica{
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
				{ID: 5, StoreID: 5, Role: metapb.ReplicaRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.ReplicaRoleType{
				1: placement.Follower,
				2: placement.Voter,
				4: placement.Follower,
			},
			steps: []OpStep{
				AddLearner{ToStore: 1, PeerID: 9},
				AddLearner{ToStore: 2, PeerID: 10},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{
						{ToStore: 1, PeerID: 9},
						{ToStore: 2, PeerID: 10},
					},
					DemoteVoters: []DemoteVoter{
						{ToStore: 3, PeerID: 3},
						{ToStore: 5, PeerID: 5},
					},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{
						{ToStore: 1, PeerID: 9},
						{ToStore: 2, PeerID: 10},
					},
					DemoteVoters: []DemoteVoter{
						{ToStore: 3, PeerID: 3},
						{ToStore: 5, PeerID: 5},
					},
				},
				TransferLeader{FromStore: 4, ToStore: 2},
				RemovePeer{FromStore: 3, PeerID: 3},
				RemovePeer{FromStore: 5, PeerID: 5},
			},
			expectedError: nil,
		},
		{
			name: "move resource partially with all incoming follower, leader step down",
			originPeers: []metapb.Replica{
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
				{ID: 5, StoreID: 5, Role: metapb.ReplicaRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.ReplicaRoleType{
				1: placement.Follower,
				2: placement.Follower,
				4: placement.Follower,
			},
			steps:         []OpStep{},
			expectedError: errors.New("resource need at least 1 voter or leader"),
		},
		{
			name: "only leader transfer",
			originPeers: []metapb.Replica{
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_Voter},
				{ID: 5, StoreID: 5, Role: metapb.ReplicaRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.ReplicaRoleType{
				3: placement.Follower,
				4: placement.Voter,
				5: placement.Follower,
			},
			steps: []OpStep{
				TransferLeader{FromStore: 3, ToStore: 4},
			},
		},
		{
			name: "add peer and transfer leader",
			originPeers: []metapb.Replica{
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_Voter},
				{ID: 5, StoreID: 5, Role: metapb.ReplicaRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.ReplicaRoleType{
				3: placement.Follower,
				4: placement.Voter,
				5: placement.Follower,
				6: placement.Follower,
			},
			steps: []OpStep{
				AddLearner{ToStore: 6},
				PromoteLearner{ToStore: 6},
				TransferLeader{FromStore: 3, ToStore: 4},
			},
		},
	}
	for _, tc := range tt {
		t.Log(tc.name)
		resource := core.NewCachedShard(&metapb.Shard{ID: 10, Replicas: tc.originPeers}, &tc.originPeers[0])
		op, err := CreateMoveShardOperator("test", s.cluster, resource, OpAdmin, tc.targetPeerRoles)

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
				assert.Equal(t, step.FromStore, tc.steps[i].(TransferLeader).FromStore)
				assert.Equal(t, step.ToStore, tc.steps[i].(TransferLeader).ToStore)
			case ChangePeerV2Leave:
				assert.Equal(t, len(step.PromoteLearners), len(tc.steps[i].(ChangePeerV2Leave).PromoteLearners))
				assert.Equal(t, len(step.DemoteVoters), len(tc.steps[i].(ChangePeerV2Leave).DemoteVoters))
				for j, p := range tc.steps[i].(ChangePeerV2Leave).PromoteLearners {
					assert.Equal(t, step.PromoteLearners[j].ToStore, p.ToStore)
				}
				for j, d := range tc.steps[i].(ChangePeerV2Leave).DemoteVoters {
					assert.Equal(t, step.DemoteVoters[j].ToStore, d.ToStore)
				}
			case ChangePeerV2Enter:
				assert.Equal(t, len(step.PromoteLearners), len(tc.steps[i].(ChangePeerV2Enter).PromoteLearners))
				assert.Equal(t, len(step.DemoteVoters), len(tc.steps[i].(ChangePeerV2Enter).DemoteVoters))
				for j, p := range tc.steps[i].(ChangePeerV2Enter).PromoteLearners {
					assert.Equal(t, step.PromoteLearners[j].ToStore, p.ToStore)
				}
				for j, d := range tc.steps[i].(ChangePeerV2Enter).DemoteVoters {
					assert.Equal(t, step.DemoteVoters[j].ToStore, d.ToStore)
				}
			case AddLearner:
				assert.Equal(t, step.ToStore, tc.steps[i].(AddLearner).ToStore)
			case PromoteLearner:
				assert.Equal(t, step.ToStore, tc.steps[i].(PromoteLearner).ToStore)
			case RemovePeer:
				assert.Equal(t, step.FromStore, tc.steps[i].(RemovePeer).FromStore)
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
		originPeers     []metapb.Replica // first is leader
		targetPeerRoles map[uint64]placement.ReplicaRoleType
		steps           []OpStep
		expectedError   error
	}
	tt := []testCase{
		{
			name: "move resource partially with incoming voter, demote existed voter",
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.ReplicaRoleType{
				2: placement.Leader,
				3: placement.Learner,
				4: placement.Voter,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4},
				PromoteLearner{ToStore: 4},
				TransferLeader{FromStore: 1, ToStore: 2},
				RemovePeer{FromStore: 1},
				RemovePeer{FromStore: 3},
				AddLearner{ToStore: 3},
			},
		},
		{
			name: "move resource partially with incoming leader",
			originPeers: []metapb.Replica{
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.ReplicaRoleType{
				2: placement.Voter,
				3: placement.Voter,
				4: placement.Leader,
			},
			steps: []OpStep{
				AddLearner{ToStore: 4},
				PromoteLearner{ToStore: 4},
				TransferLeader{FromStore: 1, ToStore: 2},
				RemovePeer{FromStore: 1},
				TransferLeader{FromStore: 2, ToStore: 4},
			},
		},
		{
			name: "move resource partially with incoming learner, demote leader",
			originPeers: []metapb.Replica{
				{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.ReplicaRoleType{
				2: placement.Learner,
				3: placement.Voter,
				4: placement.Learner,
			},
			steps: []OpStep{
				RemovePeer{FromStore: 1},
				TransferLeader{FromStore: 2, ToStore: 3},
				RemovePeer{FromStore: 2},
				AddLearner{ToStore: 2},
				AddLearner{ToStore: 4},
			},
		},
		{
			name: "move resource partially with all incoming follower, leader step down",
			originPeers: []metapb.Replica{
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_Voter},
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
				{ID: 5, StoreID: 5, Role: metapb.ReplicaRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.ReplicaRoleType{
				1: placement.Follower,
				2: placement.Follower,
				4: placement.Follower,
			},
			steps:         []OpStep{},
			expectedError: errors.New("resource need at least 1 voter or leader"),
		},
		{
			name: "only leader transfer",
			originPeers: []metapb.Replica{
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_Voter},
				{ID: 5, StoreID: 5, Role: metapb.ReplicaRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.ReplicaRoleType{
				3: placement.Follower,
				4: placement.Voter,
				5: placement.Follower,
			},
			steps: []OpStep{
				TransferLeader{FromStore: 3, ToStore: 4},
			},
		},
		{
			name: "add peer and transfer leader",
			originPeers: []metapb.Replica{
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter},
				{ID: 4, StoreID: 4, Role: metapb.ReplicaRole_Voter},
				{ID: 5, StoreID: 5, Role: metapb.ReplicaRole_Voter},
			},
			targetPeerRoles: map[uint64]placement.ReplicaRoleType{
				3: placement.Follower,
				4: placement.Voter,
				5: placement.Follower,
				6: placement.Follower,
			},
			steps: []OpStep{
				AddLearner{ToStore: 6},
				PromoteLearner{ToStore: 6},
				TransferLeader{FromStore: 3, ToStore: 4},
			},
		},
	}

	s.cluster.DisableJointConsensus()
	for _, tc := range tt {
		t.Log(tc.name)
		resource := core.NewCachedShard(&metapb.Shard{ID: 10, Replicas: tc.originPeers}, &tc.originPeers[0])
		op, err := CreateMoveShardOperator("test", s.cluster, resource, OpAdmin, tc.targetPeerRoles)

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
				assert.Equal(t, step.FromStore, tc.steps[i].(TransferLeader).FromStore)
				assert.Equal(t, step.ToStore, tc.steps[i].(TransferLeader).ToStore)
			case AddLearner:
				assert.Equal(t, step.ToStore, tc.steps[i].(AddLearner).ToStore)
			case PromoteLearner:
				assert.Equal(t, step.ToStore, tc.steps[i].(PromoteLearner).ToStore)
			case RemovePeer:
				assert.Equal(t, step.FromStore, tc.steps[i].(RemovePeer).FromStore)
			default:
				t.Fatalf("unexpected type: %s", step.String())
			}
		}
	}
}
