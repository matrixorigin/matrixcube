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
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

type testBuilder struct {
	cluster *mockcluster.Cluster
}

func (s *testBuilder) setup() {
	opts := config.NewTestOptions()
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

func (s *testBuilder) newBuilder() *Builder {
	peers := []metapb.Replica{
		{ID: 11, StoreID: 1},
		{ID: 12, StoreID: 2},
		{ID: 13, StoreID: 3, Role: metapb.ReplicaRole_Learner},
	}
	resource := core.NewCachedShard(metapb.Shard{ID: 1, Replicas: peers}, &peers[0])
	return NewBuilder("test", s.cluster, resource)
}

func TestRecord(t *testing.T) {
	s := &testBuilder{}
	s.setup()

	assert.Error(t, s.newBuilder().AddPeer(metapb.Replica{StoreID: 1}).err)
	assert.NoError(t, s.newBuilder().AddPeer(metapb.Replica{StoreID: 4}).err)
	assert.Error(t, s.newBuilder().PromoteLearner(1).err)
	assert.NoError(t, s.newBuilder().PromoteLearner(3).err)
	assert.NoError(t, s.newBuilder().SetLeader(1).SetLeader(2).err)
	assert.Error(t, s.newBuilder().SetLeader(3).err)
	assert.Error(t, s.newBuilder().RemovePeer(4).err)
	assert.NoError(t, s.newBuilder().AddPeer(metapb.Replica{StoreID: 4, Role: metapb.ReplicaRole_Learner}).RemovePeer(4).err)
	assert.Error(t, s.newBuilder().SetLeader(2).RemovePeer(2).err)
	assert.Error(t, s.newBuilder().PromoteLearner(4).err)
	assert.Error(t, s.newBuilder().SetLeader(4).err)
	assert.Error(t, s.newBuilder().SetPeers(map[uint64]metapb.Replica{2: {ID: 2}}).err)

	m := map[uint64]metapb.Replica{
		2: {StoreID: 2},
		3: {StoreID: 3, Role: metapb.ReplicaRole_Learner},
		4: {StoreID: 4},
	}
	builder := s.newBuilder().SetPeers(m).EnableLightWeight()
	assert.Equal(t, 3, len(builder.targetPeers))
	assert.True(t, reflect.DeepEqual(m[2], builder.targetPeers[2]))
	assert.True(t, reflect.DeepEqual(m[3], builder.targetPeers[3]))
	assert.True(t, reflect.DeepEqual(m[4], builder.targetPeers[4]))
	assert.Equal(t, uint64(0), builder.targetLeaderStoreID)
	assert.True(t, builder.lightWeight)
}

func TestNewBuilder(t *testing.T) {
	s := &testBuilder{}
	s.setup()

	peers := []metapb.Replica{{ID: 11, StoreID: 1}, {ID: 12, StoreID: 2, Role: metapb.ReplicaRole_Learner}}
	resource := core.NewCachedShard(metapb.Shard{ID: 42, Replicas: peers}, &peers[0])
	builder := NewBuilder("test", s.cluster, resource)
	assert.NoError(t, builder.err)
	assert.Equal(t, 2, len(builder.originPeers))
	assert.True(t, reflect.DeepEqual(peers[0], builder.originPeers[1]))
	assert.True(t, reflect.DeepEqual(peers[1], builder.originPeers[2]))
	assert.Equal(t, uint64(1), builder.originLeaderStoreID)
	assert.Equal(t, 2, len(builder.targetPeers))
	assert.True(t, reflect.DeepEqual(peers[0], builder.targetPeers[1]))
	assert.True(t, reflect.DeepEqual(peers[1], builder.targetPeers[2]))

	resource = resource.Clone(core.WithLeader(nil))
	builder = NewBuilder("test", s.cluster, resource)
	assert.Error(t, builder.err)
}

func TestPrepareBuild(t *testing.T) {
	s := &testBuilder{}
	s.setup()

	// no voter.
	_, err := s.newBuilder().SetPeers(map[uint64]metapb.Replica{4: {StoreID: 4, Role: metapb.ReplicaRole_Learner}}).prepareBuild()
	assert.Error(t, err)

	// use joint consensus
	builder := s.newBuilder().SetPeers(map[uint64]metapb.Replica{
		1: {StoreID: 1, Role: metapb.ReplicaRole_Learner},
		3: {StoreID: 3},
		4: {StoreID: 4, ID: 14},
		5: {StoreID: 5, Role: metapb.ReplicaRole_Learner},
	})
	_, err = builder.prepareBuild()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(builder.toAdd))
	assert.NotEqual(t, metapb.ReplicaRole_Learner, builder.toAdd[4].Role)
	assert.Equal(t, uint64(14), builder.toAdd[4].ID)
	assert.Equal(t, metapb.ReplicaRole_Learner, builder.toAdd[5].Role)
	assert.NotEqual(t, uint64(0), builder.toAdd[5].ID)
	assert.Equal(t, 1, len(builder.toRemove))
	assert.NotNil(t, builder.toRemove[2])
	assert.Equal(t, 1, len(builder.toPromote))
	assert.NotNil(t, builder.toPromote[3])
	assert.Equal(t, 1, len(builder.toDemote))
	assert.NotNil(t, builder.toDemote[1])
	assert.Equal(t, uint64(1), builder.currentLeaderStoreID)

	// do not use joint consensus
	builder = s.newBuilder().SetPeers(map[uint64]metapb.Replica{
		1: {StoreID: 1, Role: metapb.ReplicaRole_Learner},
		2: {StoreID: 2},
		3: {StoreID: 3},
		4: {StoreID: 4, ID: 14},
		5: {StoreID: 5, Role: metapb.ReplicaRole_Learner},
	})
	builder.allowDemote = false
	builder.useJointConsensus = false
	_, err = builder.prepareBuild()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(builder.toAdd))
	assert.Equal(t, metapb.ReplicaRole_Learner, builder.toAdd[1].Role)
	assert.NotEqual(t, uint64(0), builder.toAdd[1].ID)
	assert.NotEqual(t, metapb.ReplicaRole_Learner, builder.toAdd[4].Role)
	assert.Equal(t, uint64(14), builder.toAdd[4].ID)
	assert.Equal(t, metapb.ReplicaRole_Learner, builder.toAdd[5].Role)
	assert.NotEqual(t, uint64(0), builder.toAdd[5].ID)
	assert.Equal(t, 1, len(builder.toRemove))
	assert.NotNil(t, builder.toRemove[1])
	assert.Equal(t, 1, len(builder.toPromote))
	assert.NotNil(t, builder.toPromote[3])
	assert.Equal(t, uint64(1), builder.currentLeaderStoreID)
}

func TestBuild(t *testing.T) {
	s := &testBuilder{}
	s.setup()

	type testCase struct {
		allowDemote       bool
		useJointConsensus bool
		originPeers       []metapb.Replica // first is leader
		targetPeers       []metapb.Replica // first is leader
		kind              OpKind
		steps             []OpStep // empty means error
	}
	cases := []testCase{
		{ // empty step
			false, false,
			[]metapb.Replica{{ID: 1, StoreID: 1}, {ID: 2, StoreID: 2}},
			[]metapb.Replica{{ID: 1, StoreID: 1}, {ID: 2, StoreID: 2}},
			0,
			[]OpStep{},
		},
		{ // empty step
			true, true,
			[]metapb.Replica{{ID: 1, StoreID: 1}, {ID: 2, StoreID: 2}},
			[]metapb.Replica{{ID: 1, StoreID: 1}, {ID: 2, StoreID: 2}},
			0,
			[]OpStep{},
		},
		{ // no valid leader
			false, false,
			[]metapb.Replica{{ID: 1, StoreID: 1}},
			[]metapb.Replica{{ID: 10, StoreID: 10}},
			0,
			[]OpStep{},
		},
		{ // no valid leader
			true, true,
			[]metapb.Replica{{ID: 1, StoreID: 1}},
			[]metapb.Replica{{ID: 10, StoreID: 10}},
			0,
			[]OpStep{},
		},
		{ // promote learner
			false, false,
			[]metapb.Replica{{ID: 1, StoreID: 1}, {ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Learner}},
			[]metapb.Replica{{ID: 2, StoreID: 2}, {ID: 1, StoreID: 1}},
			OpLeader,
			[]OpStep{
				PromoteLearner{ToStore: 2},
				TransferLeader{FromStore: 1, ToStore: 2},
			},
		},
		{ // promote learner
			true, true,
			[]metapb.Replica{{ID: 1, StoreID: 1}, {ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Learner}},
			[]metapb.Replica{{ID: 2, StoreID: 2}, {ID: 1, StoreID: 1}},
			OpLeader,
			[]OpStep{
				PromoteLearner{ToStore: 2},
				TransferLeader{FromStore: 1, ToStore: 2},
			},
		},
		{ // not use joint consensus: prefer replace
			false, false,
			[]metapb.Replica{{ID: 1, StoreID: 1}, {ID: 2, StoreID: 2}, {ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Learner}},
			[]metapb.Replica{{StoreID: 4}, {StoreID: 5, Role: metapb.ReplicaRole_Learner}},
			OpLeader | OpShard,
			[]OpStep{
				AddLearner{ToStore: 4},
				PromoteLearner{ToStore: 4},
				RemovePeer{FromStore: 2},
				AddLearner{ToStore: 5},
				RemovePeer{FromStore: 3},
				TransferLeader{FromStore: 1, ToStore: 4},
				RemovePeer{FromStore: 1},
			},
		},
		{ // use joint consensus: transfer leader in joint state
			true, true,
			[]metapb.Replica{{ID: 1, StoreID: 1}, {ID: 2, StoreID: 2}, {ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Learner}},
			[]metapb.Replica{{StoreID: 4}, {StoreID: 5, Role: metapb.ReplicaRole_Learner}},
			OpLeader | OpShard,
			[]OpStep{
				AddLearner{ToStore: 4},
				AddLearner{ToStore: 5},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}, {ToStore: 2}},
				},
				TransferLeader{FromStore: 1, ToStore: 4},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 4}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}, {ToStore: 2}},
				},
				RemovePeer{FromStore: 1},
				RemovePeer{FromStore: 2},
				RemovePeer{FromStore: 3},
			},
		},
		{ // not use joint consensus: transfer leader before remove leader
			false, false,
			[]metapb.Replica{{ID: 1, StoreID: 1}},
			[]metapb.Replica{{StoreID: 2}},
			OpLeader | OpShard,
			[]OpStep{
				AddLearner{ToStore: 2},
				PromoteLearner{ToStore: 2},
				TransferLeader{FromStore: 1, ToStore: 2},
				RemovePeer{FromStore: 1},
			},
		},
		{ // use joint consensus: transfer leader in joint state
			true, true,
			[]metapb.Replica{{ID: 1, StoreID: 1}},
			[]metapb.Replica{{StoreID: 2}},
			OpLeader | OpShard,
			[]OpStep{
				AddLearner{ToStore: 2},
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToStore: 2}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
				TransferLeader{FromStore: 1, ToStore: 2},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 2}},
					DemoteVoters:    []DemoteVoter{{ToStore: 1}},
				},
				RemovePeer{FromStore: 1},
			},
		},
		{ // not use joint consensus: replace voter with learner
			false, false,
			[]metapb.Replica{{ID: 1, StoreID: 1}, {ID: 2, StoreID: 2}},
			[]metapb.Replica{{ID: 1, StoreID: 1}, {ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Learner}},
			OpShard,
			[]OpStep{
				RemovePeer{FromStore: 2},
				AddLearner{ToStore: 2},
			},
		},
		{ // use joint consensus: demote directly
			true, true,
			[]metapb.Replica{{ID: 1, StoreID: 1}, {ID: 2, StoreID: 2}},
			[]metapb.Replica{{StoreID: 1}, {StoreID: 2, Role: metapb.ReplicaRole_Learner}},
			0, // Note that there is no OpShard here
			[]OpStep{
				DemoteFollower{ToStore: 2},
			},
		},
		// not use joint consensus
		{ // prefer replace with nearest peer
			false, false,
			[]metapb.Replica{{ID: 1, StoreID: 1}, {ID: 6, StoreID: 6}, {ID: 8, StoreID: 8}},
			//             z1,h1                z1,h2                 z2,h1
			[]metapb.Replica{{StoreID: 9}, {StoreID: 7}, {StoreID: 10}},
			//             z2,h2         z1,h1         z3,h1
			OpLeader | OpShard,
			[]OpStep{
				// 6->7
				AddLearner{ToStore: 7},
				PromoteLearner{ToStore: 7},
				RemovePeer{FromStore: 6},
				// 8->9
				AddLearner{ToStore: 9},
				PromoteLearner{ToStore: 9},
				RemovePeer{FromStore: 8},
				// 1->10
				AddLearner{ToStore: 10},
				PromoteLearner{ToStore: 10},
				TransferLeader{FromStore: 1, ToStore: 7}, // transfer oldest voter
				RemovePeer{FromStore: 1},
				// transfer leader
				TransferLeader{FromStore: 7, ToStore: 9},
			},
		},
		{ // promote learner + demote voter
			true, false,
			[]metapb.Replica{{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter}, {ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Learner}},
			[]metapb.Replica{{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter}, {ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Learner}, {ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Learner}},
			OpLeader | OpShard,
			[]OpStep{
				PromoteLearner{ToStore: 2},
				TransferLeader{FromStore: 1, ToStore: 2},
				DemoteFollower{ToStore: 1},
				AddLearner{ToStore: 3},
			},
		},
		{ // add learner + promote learner + remove voter
			false, false,
			[]metapb.Replica{{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter}, {ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Learner}},
			[]metapb.Replica{{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter}, {ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Learner}},
			OpLeader | OpShard,
			[]OpStep{
				AddLearner{ToStore: 3},
				PromoteLearner{ToStore: 2},
				TransferLeader{FromStore: 1, ToStore: 2},
				RemovePeer{FromStore: 1},
			},
		},
		{ // add voter + demote voter + remove learner.
			true, false,
			[]metapb.Replica{{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Voter}, {ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Learner}},
			[]metapb.Replica{{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Voter}, {ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Learner}},
			OpLeader | OpShard,
			[]OpStep{
				AddLearner{ToStore: 3},
				PromoteLearner{ToStore: 3},
				TransferLeader{FromStore: 1, ToStore: 3},
				DemoteFollower{ToStore: 1},
				RemovePeer{FromStore: 2},
			},
		},
		// use joint consensus
		{ // transfer leader before entering joint state
			true, true,
			[]metapb.Replica{{ID: 1, StoreID: 1}, {ID: 2, StoreID: 2}, {ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Learner}},
			[]metapb.Replica{{ID: 2, StoreID: 2}, {ID: 3, StoreID: 3}},
			OpLeader | OpShard,
			[]OpStep{
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
		{ // transfer leader after leaving joint state
			true, true,
			[]metapb.Replica{{ID: 1, StoreID: 1}, {ID: 2, StoreID: 2}, {ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Learner}},
			[]metapb.Replica{{ID: 3, StoreID: 3}, {ID: 1, StoreID: 1}},
			OpLeader | OpShard,
			[]OpStep{
				ChangePeerV2Enter{
					PromoteLearners: []PromoteLearner{{ToStore: 3}},
					DemoteVoters:    []DemoteVoter{{ToStore: 2}},
				},
				ChangePeerV2Leave{
					PromoteLearners: []PromoteLearner{{ToStore: 3}},
					DemoteVoters:    []DemoteVoter{{ToStore: 2}},
				},
				TransferLeader{FromStore: 1, ToStore: 3},
				RemovePeer{FromStore: 2},
			},
		},
	}

	for _, tc := range cases {
		resource := core.NewCachedShard(metapb.Shard{ID: 1, Replicas: tc.originPeers}, &tc.originPeers[0])
		builder := NewBuilder("test", s.cluster, resource)
		builder.allowDemote = tc.allowDemote
		builder.useJointConsensus = tc.useJointConsensus
		m := make(map[uint64]metapb.Replica)
		for _, p := range tc.targetPeers {
			m[p.GetStoreID()] = p
		}
		builder.SetPeers(m).SetLeader(tc.targetPeers[0].GetStoreID())
		op, err := builder.Build(0)
		if len(tc.steps) == 0 {
			assert.Error(t, err)
			continue
		}
		assert.NoError(t, err)
		assert.Equal(t, tc.kind, op.Kind())
		assert.Equal(t, len(tc.steps), op.Len())
		for i := 0; i < op.Len(); i++ {
			switch step := op.Step(i).(type) {
			case TransferLeader:
				assert.Equal(t, tc.steps[i].(TransferLeader).FromStore, step.FromStore)
				assert.Equal(t, tc.steps[i].(TransferLeader).ToStore, step.ToStore)
			case AddPeer:
				assert.Equal(t, tc.steps[i].(AddPeer).ToStore, step.ToStore)
			case AddLightPeer:
				assert.Equal(t, tc.steps[i].(AddLightPeer).ToStore, step.ToStore)
			case RemovePeer:
				assert.Equal(t, step.FromStore, tc.steps[i].(RemovePeer).FromStore)
			case AddLearner:
				assert.Equal(t, step.ToStore, tc.steps[i].(AddLearner).ToStore)
			case AddLightLearner:
				assert.Equal(t, step.ToStore, tc.steps[i].(AddLightLearner).ToStore)
			case PromoteLearner:
				assert.Equal(t, step.ToStore, tc.steps[i].(PromoteLearner).ToStore)
			case DemoteFollower:
				assert.Equal(t, step.ToStore, tc.steps[i].(DemoteFollower).ToStore)
			case ChangePeerV2Enter:
				assert.Equal(t, len(step.PromoteLearners), len(tc.steps[i].(ChangePeerV2Enter).PromoteLearners))
				assert.Equal(t, len(step.DemoteVoters), len(tc.steps[i].(ChangePeerV2Enter).DemoteVoters))
				for j, p := range tc.steps[i].(ChangePeerV2Enter).PromoteLearners {
					assert.Equal(t, step.PromoteLearners[j].ToStore, p.ToStore)
				}
				for j, d := range tc.steps[i].(ChangePeerV2Enter).DemoteVoters {
					assert.Equal(t, step.DemoteVoters[j].ToStore, d.ToStore)
				}
			case ChangePeerV2Leave:
				assert.Equal(t, len(step.PromoteLearners), len(tc.steps[i].(ChangePeerV2Leave).PromoteLearners))
				assert.Equal(t, len(step.DemoteVoters), len(tc.steps[i].(ChangePeerV2Leave).DemoteVoters))
				for j, p := range tc.steps[i].(ChangePeerV2Leave).PromoteLearners {
					assert.Equal(t, step.PromoteLearners[j].ToStore, p.ToStore)
				}
				for j, d := range tc.steps[i].(ChangePeerV2Leave).DemoteVoters {
					assert.Equal(t, step.DemoteVoters[j].ToStore, d.ToStore)
				}
			}
		}
	}
}

// Test for not set unhealthy peer as target for promote learner and transfer leader
func TestTargetUnhealthyPeer(t *testing.T) {
	s := &testBuilder{}
	s.setup()

	p := metapb.Replica{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Learner}
	resource := core.NewCachedShard(metapb.Shard{
		ID: 1, Replicas: []metapb.Replica{{ID: 1, StoreID: 1}, p},
	},
		&metapb.Replica{ID: 1, StoreID: 1}, core.WithPendingPeers([]metapb.Replica{p}))
	builder := NewBuilder("test", s.cluster, resource)
	builder.PromoteLearner(2)
	assert.Error(t, builder.err)
	resource = core.NewCachedShard(metapb.Shard{
		ID: 1, Replicas: []metapb.Replica{{ID: 1, StoreID: 1}, p},
	}, &metapb.Replica{ID: 1, StoreID: 1}, core.WithDownPeers([]metapb.ReplicaStats{{Replica: p}}))
	builder = NewBuilder("test", s.cluster, resource)
	builder.PromoteLearner(2)
	assert.Error(t, builder.err)

	p = metapb.Replica{ID: 2, StoreID: 2, Role: metapb.ReplicaRole_Voter}
	resource = core.NewCachedShard(metapb.Shard{
		ID: 1, Replicas: []metapb.Replica{{ID: 1, StoreID: 1}, p},
	}, &metapb.Replica{ID: 1, StoreID: 1}, core.WithPendingPeers([]metapb.Replica{p}))
	builder = NewBuilder("test", s.cluster, resource)
	builder.SetLeader(2)
	assert.Error(t, builder.err)
	resource = core.NewCachedShard(metapb.Shard{
		ID: 1, Replicas: []metapb.Replica{{ID: 1, StoreID: 1}, p},
	}, &metapb.Replica{ID: 1, StoreID: 1}, core.WithDownPeers([]metapb.ReplicaStats{{Replica: p}}))
	builder = NewBuilder("test", s.cluster, resource)
	builder.SetLeader(2)
	assert.Error(t, builder.err)
}
