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
	"encoding/json"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

type testOperator struct {
	cluster *mockcluster.Cluster
}

func (s *testOperator) setup() {
	cfg := config.NewTestOptions()
	s.cluster = mockcluster.NewCluster(cfg)
	s.cluster.SetMaxMergeShardSize(2)
	s.cluster.SetMaxMergeShardKeys(2)
	s.cluster.SetLabelPropertyConfig(config.LabelPropertyConfig{
		opt.RejectLeader: {{Key: "reject", Value: "leader"}},
	})
	containers := map[uint64][]string{
		1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {},
		7: {"reject", "leader"},
		8: {"reject", "leader"},
	}
	for containerID, labels := range containers {
		s.cluster.PutStoreWithLabels(containerID, labels...)
	}
}

func (s *testOperator) newTestShard(resourceID uint64, leaderPeer uint64, peers ...[2]uint64) *core.CachedShard {
	var (
		resource = &metadata.Shard{}
		leader   *metapb.Replica
	)
	resource.SetID(resourceID)
	for i := range peers {
		peer := metapb.Replica{
			ID:      peers[i][1],
			StoreID: peers[i][0],
		}
		resource.AppendReplica(peer)
		if peer.ID == leaderPeer {
			leader = &peer
		}
	}
	resourceInfo := core.NewCachedShard(resource, leader,
		core.SetApproximateSize(50),
		core.SetApproximateKeys(50))
	return resourceInfo
}

func (s *testOperator) newTestOperator(resourceID uint64, kind OpKind, steps ...OpStep) *Operator {
	return NewOperator("test", "test", resourceID, metapb.ShardEpoch{}, OpAdmin|kind, steps...)
}

func (s *testOperator) checkSteps(t *testing.T, op *Operator, steps []OpStep) {
	assert.Equal(t, len(steps), op.Len())
	for i := range steps {
		assert.Equal(t, op.Step(i), steps[i])
	}
}

func TestOperatorStep(t *testing.T) {
	s := &testOperator{}
	s.setup()

	resource := s.newTestShard(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
	assert.False(t, TransferLeader{FromStore: 1, ToStore: 2}.IsFinish(resource))
	assert.True(t, TransferLeader{FromStore: 2, ToStore: 1}.IsFinish(resource))
	assert.False(t, AddPeer{ToStore: 3, PeerID: 3}.IsFinish(resource))
	assert.True(t, AddPeer{ToStore: 1, PeerID: 1}.IsFinish(resource))
	assert.False(t, RemovePeer{FromStore: 1}.IsFinish(resource))
	assert.True(t, RemovePeer{FromStore: 3}.IsFinish(resource))
}

func TestOperator(t *testing.T) {
	s := &testOperator{}
	s.setup()

	resource := s.newTestShard(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
	// addPeer1, transferLeader1, removePeer3
	steps := []OpStep{
		AddPeer{ToStore: 1, PeerID: 1},
		TransferLeader{FromStore: 3, ToStore: 1},
		RemovePeer{FromStore: 3},
	}
	op := s.newTestOperator(1, OpLeader|OpShard, steps...)
	assert.Equal(t, core.HighPriority, op.GetPriorityLevel())
	s.checkSteps(t, op, steps)
	op.Start()
	assert.Nil(t, op.Check(resource))
	assert.Equal(t, SUCCESS, op.Status())
	SetOperatorStatusReachTime(op, STARTED, time.Now().Add(-SlowOperatorWaitTime-time.Second))
	assert.False(t, op.CheckTimeout())

	// addPeer1, transferLeader1, removePeer2
	steps = []OpStep{
		AddPeer{ToStore: 1, PeerID: 1},
		TransferLeader{FromStore: 2, ToStore: 1},
		RemovePeer{FromStore: 2},
	}
	op = s.newTestOperator(1, OpLeader|OpShard, steps...)
	s.checkSteps(t, op, steps)
	op.Start()
	assert.Equal(t, op.Check(resource), RemovePeer{FromStore: 2})
	assert.Equal(t, atomic.LoadInt32(&op.currentStep), int32(2))
	assert.False(t, op.CheckTimeout())
	SetOperatorStatusReachTime(op, STARTED, op.GetStartTime().Add(-FastOperatorWaitTime-time.Second))
	assert.False(t, op.CheckTimeout())
	SetOperatorStatusReachTime(op, STARTED, op.GetStartTime().Add(-SlowOperatorWaitTime-time.Second))
	assert.True(t, op.CheckTimeout())
	res, err := json.Marshal(op)
	assert.NoError(t, err)
	assert.Equal(t, len(res), len(op.String())+2)

	// check short timeout for transfer leader only operators.
	steps = []OpStep{TransferLeader{FromStore: 2, ToStore: 1}}
	op = s.newTestOperator(1, OpLeader, steps...)
	op.Start()
	assert.False(t, op.CheckTimeout())
	SetOperatorStatusReachTime(op, STARTED, op.GetStartTime().Add(-FastOperatorWaitTime-time.Second))
	assert.True(t, op.CheckTimeout())
}

func TestInfluence(t *testing.T) {
	s := &testOperator{}
	s.setup()

	resource := s.newTestShard(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
	opInfluence := OpInfluence{StoresInfluence: make(map[uint64]*StoreInfluence)}
	containerOpInfluence := opInfluence.StoresInfluence
	containerOpInfluence[1] = &StoreInfluence{InfluenceStats: map[string]InfluenceStats{}}
	containerOpInfluence[2] = &StoreInfluence{InfluenceStats: map[string]InfluenceStats{}}

	AddPeer{ToStore: 2, PeerID: 2}.Influence(opInfluence, resource)
	assert.True(t, reflect.DeepEqual(*containerOpInfluence[2], StoreInfluence{
		InfluenceStats: map[string]InfluenceStats{
			"": {
				LeaderSize:  0,
				LeaderCount: 0,
				ShardSize:   50,
				ShardCount:  1,
			},
		},
		StepCost: map[limit.Type]int64{limit.AddPeer: 1000},
	}))

	TransferLeader{FromStore: 1, ToStore: 2}.Influence(opInfluence, resource)
	assert.True(t, reflect.DeepEqual(*containerOpInfluence[1], StoreInfluence{
		InfluenceStats: map[string]InfluenceStats{
			"": {
				LeaderSize:  -50,
				LeaderCount: -1,
				ShardSize:   0,
				ShardCount:  0,
			},
		},
		StepCost: nil,
	}))
	assert.True(t, reflect.DeepEqual(*containerOpInfluence[2], StoreInfluence{
		InfluenceStats: map[string]InfluenceStats{
			"": {
				LeaderSize:  50,
				LeaderCount: 1,
				ShardSize:   50,
				ShardCount:  1,
			},
		},
		StepCost: map[limit.Type]int64{limit.AddPeer: 1000},
	}))

	RemovePeer{FromStore: 1}.Influence(opInfluence, resource)
	assert.True(t, reflect.DeepEqual(*containerOpInfluence[1], StoreInfluence{
		InfluenceStats: map[string]InfluenceStats{
			"": {
				LeaderSize:  -50,
				LeaderCount: -1,
				ShardSize:   -50,
				ShardCount:  -1,
			},
		},
		StepCost: map[limit.Type]int64{limit.RemovePeer: 1000},
	}))
	assert.True(t, reflect.DeepEqual(*containerOpInfluence[2], StoreInfluence{
		InfluenceStats: map[string]InfluenceStats{
			"": {
				LeaderSize:  50,
				LeaderCount: 1,
				ShardSize:   50,
				ShardCount:  1,
			},
		},

		StepCost: map[limit.Type]int64{limit.AddPeer: 1000},
	}))

	MergeShard{IsPassive: false}.Influence(opInfluence, resource)
	assert.True(t, reflect.DeepEqual(*containerOpInfluence[1], StoreInfluence{
		InfluenceStats: map[string]InfluenceStats{
			"": {
				LeaderSize:  -50,
				LeaderCount: -1,
				ShardSize:   -50,
				ShardCount:  -1,
			},
		},

		StepCost: map[limit.Type]int64{limit.RemovePeer: 1000},
	}))
	assert.True(t, reflect.DeepEqual(*containerOpInfluence[2], StoreInfluence{
		InfluenceStats: map[string]InfluenceStats{
			"": {
				LeaderSize:  50,
				LeaderCount: 1,
				ShardSize:   50,
				ShardCount:  1,
			},
		},

		StepCost: map[limit.Type]int64{limit.AddPeer: 1000},
	}))

	MergeShard{IsPassive: true}.Influence(opInfluence, resource)
	assert.True(t, reflect.DeepEqual(*containerOpInfluence[1], StoreInfluence{
		InfluenceStats: map[string]InfluenceStats{
			"": {
				LeaderSize:  -50,
				LeaderCount: -2,
				ShardSize:   -50,
				ShardCount:  -2,
			},
		},

		StepCost: map[limit.Type]int64{limit.RemovePeer: 1000},
	}))
	assert.True(t, reflect.DeepEqual(*containerOpInfluence[2], StoreInfluence{
		InfluenceStats: map[string]InfluenceStats{
			"": {
				LeaderSize:  50,
				LeaderCount: 1,
				ShardSize:   50,
				ShardCount:  0,
			},
		},

		StepCost: map[limit.Type]int64{limit.AddPeer: 1000},
	}))
}

func TestOperatorKind(t *testing.T) {
	s := &testOperator{}
	s.setup()

	assert.Equal(t, "leader,replica", (OpLeader | OpReplica).String())
	assert.Equal(t, "unknown", OpKind(0).String())
	k, err := ParseOperatorKind("resource,leader")
	assert.NoError(t, err)
	assert.Equal(t, OpShard|OpLeader, k)
	_, err = ParseOperatorKind("leader,resource")
	assert.NoError(t, err)
	_, err = ParseOperatorKind("foobar")
	assert.Error(t, err)
}

func TestCheckSuccess(t *testing.T) {
	s := &testOperator{}
	s.setup()

	{
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := s.newTestOperator(1, OpLeader|OpShard, steps...)
		assert.Equal(t, CREATED, op.Status())
		assert.False(t, op.CheckSuccess())
		assert.True(t, op.Start())
		assert.False(t, op.CheckSuccess())
		op.currentStep = int32(len(op.steps))
		assert.True(t, op.CheckSuccess())
	}
	{
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := s.newTestOperator(1, OpLeader|OpShard, steps...)
		op.currentStep = int32(len(op.steps))
		assert.Equal(t, CREATED, op.Status())
		assert.False(t, op.CheckSuccess())
		assert.True(t, op.Start())
		assert.True(t, op.CheckSuccess())
	}
}

func TestCheckTimeout(t *testing.T) {
	s := &testOperator{}
	s.setup()

	{
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := s.newTestOperator(1, OpLeader|OpShard, steps...)
		assert.Equal(t, CREATED, op.Status())
		assert.True(t, op.Start())
		op.currentStep = int32(len(op.steps))
		assert.False(t, op.CheckTimeout())
		assert.Equal(t, SUCCESS, op.Status())
	}
	{
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := s.newTestOperator(1, OpLeader|OpShard, steps...)
		assert.Equal(t, CREATED, op.Status())
		assert.True(t, op.Start())
		op.currentStep = int32(len(op.steps))
		SetOperatorStatusReachTime(op, STARTED, time.Now().Add(-SlowOperatorWaitTime))
		assert.False(t, op.CheckTimeout())
		assert.Equal(t, SUCCESS, op.Status())
	}
}

func TestStart(t *testing.T) {
	s := &testOperator{}
	s.setup()

	steps := []OpStep{
		AddPeer{ToStore: 1, PeerID: 1},
		TransferLeader{FromStore: 2, ToStore: 1},
		RemovePeer{FromStore: 2},
	}
	op := s.newTestOperator(1, OpLeader|OpShard, steps...)
	assert.Equal(t, 0, op.GetStartTime().Nanosecond())
	assert.Equal(t, CREATED, op.Status())
	assert.True(t, op.Start())
	assert.NotEqual(t, 0, op.GetStartTime().Nanosecond())
	assert.Equal(t, STARTED, op.Status())
}

func TestCheckExpired(t *testing.T) {
	s := &testOperator{}
	s.setup()

	steps := []OpStep{
		AddPeer{ToStore: 1, PeerID: 1},
		TransferLeader{FromStore: 2, ToStore: 1},
		RemovePeer{FromStore: 2},
	}
	op := s.newTestOperator(1, OpLeader|OpShard, steps...)
	assert.False(t, op.CheckExpired())
	assert.Equal(t, CREATED, op.Status())
	SetOperatorStatusReachTime(op, CREATED, time.Now().Add(-OperatorExpireTime))
	assert.True(t, op.CheckExpired())
	assert.Equal(t, EXPIRED, op.Status())
}

func TestCheck(t *testing.T) {
	s := &testOperator{}
	s.setup()

	{
		resource := s.newTestShard(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := s.newTestOperator(1, OpLeader|OpShard, steps...)
		assert.True(t, op.Start())
		assert.NotNil(t, op.Check(resource))
		assert.Equal(t, STARTED, op.Status())
		resource = s.newTestShard(1, 1, [2]uint64{1, 1})
		assert.Nil(t, op.Check(resource))
		assert.Equal(t, SUCCESS, op.Status())
	}
	{
		resource := s.newTestShard(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := s.newTestOperator(1, OpLeader|OpShard, steps...)
		assert.True(t, op.Start())
		assert.NotNil(t, op.Check(resource))
		assert.Equal(t, STARTED, op.Status())
		op.status.setTime(STARTED, time.Now().Add(-SlowOperatorWaitTime))
		assert.NotNil(t, op.Check(resource))
		assert.Equal(t, TIMEOUT, op.Status())
	}
	{
		resource := s.newTestShard(1, 1, [2]uint64{1, 1}, [2]uint64{2, 2})
		steps := []OpStep{
			AddPeer{ToStore: 1, PeerID: 1},
			TransferLeader{FromStore: 2, ToStore: 1},
			RemovePeer{FromStore: 2},
		}
		op := s.newTestOperator(1, OpLeader|OpShard, steps...)
		assert.True(t, op.Start())
		assert.NotNil(t, op.Check(resource))
		assert.Equal(t, STARTED, op.Status())
		op.status.setTime(STARTED, time.Now().Add(-SlowOperatorWaitTime))
		resource = s.newTestShard(1, 1, [2]uint64{1, 1})
		assert.Nil(t, op.Check(resource))
		assert.Equal(t, SUCCESS, op.Status())
	}
}
