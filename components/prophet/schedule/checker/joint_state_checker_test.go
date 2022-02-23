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
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestLeaveJointState(t *testing.T) {
	cluster := mockcluster.NewCluster(config.NewTestOptions())
	jsc := NewJointStateChecker(cluster)
	for id := uint64(1); id <= 10; id++ {
		cluster.PutStoreWithLabels(id)
	}

	type testCase struct {
		Peers   []metapb.Replica // first is leader
		OpSteps []operator.OpStep
	}
	cases := []testCase{
		{
			[]metapb.Replica{
				{ID: 101, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 102, StoreID: 2, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 103, StoreID: 3, Role: metapb.ReplicaRole_IncomingVoter},
			},
			[]operator.OpStep{
				operator.ChangePeerV2Leave{
					PromoteLearners: []operator.PromoteLearner{{ToStore: 3}},
					DemoteVoters:    []operator.DemoteVoter{{ToStore: 2}},
				},
			},
		},
		{
			[]metapb.Replica{
				{ID: 101, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 102, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 103, StoreID: 3, Role: metapb.ReplicaRole_IncomingVoter},
			},
			[]operator.OpStep{
				operator.ChangePeerV2Leave{
					PromoteLearners: []operator.PromoteLearner{{ToStore: 3}},
					DemoteVoters:    []operator.DemoteVoter{},
				},
			},
		},
		{
			[]metapb.Replica{
				{ID: 101, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 102, StoreID: 2, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 103, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			[]operator.OpStep{
				operator.ChangePeerV2Leave{
					PromoteLearners: []operator.PromoteLearner{},
					DemoteVoters:    []operator.DemoteVoter{{ToStore: 2}},
				},
			},
		},
		{
			[]metapb.Replica{
				{ID: 101, StoreID: 1, Role: metapb.ReplicaRole_DemotingVoter},
				{ID: 102, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 103, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			[]operator.OpStep{
				operator.TransferLeader{FromStore: 1, ToStore: 2},
				operator.ChangePeerV2Leave{
					PromoteLearners: []operator.PromoteLearner{},
					DemoteVoters:    []operator.DemoteVoter{{ToStore: 1}},
				},
			},
		},
		{
			[]metapb.Replica{
				{ID: 101, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 102, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 103, StoreID: 3, Role: metapb.ReplicaRole_Voter},
			},
			nil,
		},
		{
			[]metapb.Replica{
				{ID: 101, StoreID: 1, Role: metapb.ReplicaRole_Voter},
				{ID: 102, StoreID: 2, Role: metapb.ReplicaRole_Voter},
				{ID: 103, StoreID: 3, Role: metapb.ReplicaRole_Learner},
			},
			nil,
		},
	}

	for _, tc := range cases {
		res := core.NewCachedShard(&metapb.Shard{ID: 1, Replicas: tc.Peers}, &tc.Peers[0])
		op := jsc.Check(res)
		checkSteps(t, op, tc.OpSteps)
	}
}

func checkSteps(t *testing.T, op *operator.Operator, steps []operator.OpStep) {
	if len(steps) == 0 {
		assert.Nil(t, op)
		return
	}

	assert.NotNil(t, op)
	assert.Equal(t, "leave-joint-state", op.Desc())
	assert.Equal(t, len(steps), op.Len())

	for i := range steps {
		switch obtain := op.Step(i).(type) {
		case operator.ChangePeerV2Leave:
			expect := steps[i].(operator.ChangePeerV2Leave)
			assert.Equal(t, len(expect.PromoteLearners), len(obtain.PromoteLearners))
			assert.Equal(t, len(expect.DemoteVoters), len(obtain.DemoteVoters))
			for j, p := range expect.PromoteLearners {
				assert.Equal(t, expect.PromoteLearners[j].ToStore, p.ToStore)
			}
			for j, d := range expect.DemoteVoters {
				assert.Equal(t, expect.DemoteVoters[j].ToStore, d.ToStore)
			}
		case operator.TransferLeader:
			expect := steps[i].(operator.TransferLeader)
			assert.Equal(t, expect.FromStore, obtain.FromStore)
			assert.Equal(t, expect.ToStore, obtain.ToStore)
		default:
			assert.FailNow(t, "unknown operator step type")
		}
	}
}
