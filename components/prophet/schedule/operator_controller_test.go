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

package schedule

import (
	"container/heap"
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/checker"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/hbstream"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

type testOperatorController struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testOperatorController) setup(t *testing.T) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testOperatorController) tearDown() {
	s.cancel()
}

// issue #1338
func TestGetOpInfluence(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	oc := NewOperatorController(s.ctx, tc, nil)
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderShard(1, 1, 2)
	tc.AddLeaderShard(2, 1, 2)
	steps := []operator.OpStep{
		operator.RemovePeer{FromStore: 2},
	}
	op1 := operator.NewOperator("test", "test", 1, metapb.ShardEpoch{}, operator.OpShard, steps...)
	op2 := operator.NewOperator("test", "test", 2, metapb.ShardEpoch{}, operator.OpShard, steps...)
	assert.True(t, op1.Start())
	oc.SetOperator(op1)
	assert.True(t, op2.Start())
	oc.SetOperator(op2)
	go func(ctx context.Context) {
		checkRemoveOperatorSuccess(t, oc, op1)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				assert.False(t, oc.RemoveOperator(op1, ""))
			}
		}
	}(s.ctx)
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				oc.GetOpInfluence(tc)
			}
		}
	}(s.ctx)
	time.Sleep(1 * time.Second)
	assert.NotNil(t, oc.GetOperator(2))
}

func TestOperatorStatus(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, tc.ID, tc, false /* no need to run */, nil)
	oc := NewOperatorController(s.ctx, tc, stream)
	tc.AddLeaderStore(1, 2)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderShard(1, 1, 2)
	tc.AddLeaderShard(2, 1, 2)
	steps := []operator.OpStep{
		operator.RemovePeer{FromStore: 2},
		operator.AddPeer{ToStore: 2, PeerID: 4},
	}
	op1 := operator.NewOperator("test", "test", 1, metapb.ShardEpoch{}, operator.OpShard, steps...)
	op2 := operator.NewOperator("test", "test", 2, metapb.ShardEpoch{}, operator.OpShard, steps...)
	res1 := tc.GetShard(1)
	res2 := tc.GetShard(2)
	assert.True(t, op1.Start())
	oc.SetOperator(op1)
	assert.True(t, op2.Start())
	oc.SetOperator(op2)
	assert.Equal(t, oc.GetOperatorStatus(1).Status, metapb.OperatorStatus_RUNNING)
	assert.Equal(t, oc.GetOperatorStatus(2).Status, metapb.OperatorStatus_RUNNING)
	operator.SetOperatorStatusReachTime(op1, operator.STARTED, time.Now().Add(-10*time.Minute))
	res2 = ApplyOperatorStep(res2, op2)
	tc.PutShard(res2)
	oc.Dispatch(res1, "test")
	oc.Dispatch(res2, "test")
	assert.Equal(t, oc.GetOperatorStatus(1).Status, metapb.OperatorStatus_TIMEOUT)
	assert.Equal(t, oc.GetOperatorStatus(2).Status, metapb.OperatorStatus_RUNNING)
	ApplyOperator(tc, op2)
	oc.Dispatch(res2, "test")
	assert.Equal(t, oc.GetOperatorStatus(2).Status, metapb.OperatorStatus_SUCCESS)
}

func TestFastFailOperator(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, tc.ID, tc, false /* no need to run */, nil)
	oc := NewOperatorController(s.ctx, tc, stream)
	tc.AddLeaderStore(1, 2)
	tc.AddLeaderStore(2, 0)
	tc.AddLeaderStore(3, 0)
	tc.AddLeaderShard(1, 1, 2)
	steps := []operator.OpStep{
		operator.RemovePeer{FromStore: 2},
		operator.AddPeer{ToStore: 3, PeerID: 4},
	}
	op := operator.NewOperator("test", "test", 1, metapb.ShardEpoch{}, operator.OpShard, steps...)
	res := tc.GetShard(1)
	assert.True(t, op.Start())
	oc.SetOperator(op)
	oc.Dispatch(res, "test")
	assert.Equal(t, metapb.OperatorStatus_RUNNING, oc.GetOperatorStatus(1).Status)
	// change the leader
	p, _ := res.GetPeer(2)
	res = res.Clone(core.WithLeader(&p))
	oc.Dispatch(res, DispatchFromHeartBeat)
	assert.Equal(t, operator.CANCELED, op.Status())
	assert.Nil(t, oc.GetOperator(res.Meta.ID()))

	// transfer leader to an illegal container.
	op = operator.NewOperator("test", "test", 1, metapb.ShardEpoch{}, operator.OpShard, operator.TransferLeader{ToStore: 5})
	oc.SetOperator(op)
	oc.Dispatch(res, DispatchFromHeartBeat)
	assert.Equal(t, operator.CANCELED, op.Status())
	assert.Nil(t, oc.GetOperator(res.Meta.ID()))
}

func TestCheckAddUnexpectedStatus(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, tc.ID, tc, false /* no need to run */, nil)
	oc := NewOperatorController(s.ctx, tc, stream)
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderShard(1, 2, 1)
	tc.AddLeaderShard(2, 2, 1)
	res1 := tc.GetShard(1)
	steps := []operator.OpStep{
		operator.RemovePeer{FromStore: 1},
		operator.AddPeer{ToStore: 1, PeerID: 4},
	}
	{
		// finished op
		op := operator.NewOperator("test", "test", 1, metapb.ShardEpoch{}, operator.OpShard, operator.TransferLeader{ToStore: 2})
		assert.True(t, oc.checkAddOperator(op))
		op.Start()
		assert.False(t, oc.checkAddOperator(op)) // started
		assert.Nil(t, op.Check(res1))
		assert.Equal(t, operator.SUCCESS, op.Status())
		assert.False(t, oc.checkAddOperator(op)) // success
	}
	{
		// finished op canceled
		op := operator.NewOperator("test", "test", 1, metapb.ShardEpoch{}, operator.OpShard, operator.TransferLeader{ToStore: 2})
		assert.True(t, oc.checkAddOperator(op))
		assert.True(t, op.Cancel())
		assert.False(t, oc.checkAddOperator(op))
	}
	{
		// finished op replaced
		op := operator.NewOperator("test", "test", 1, metapb.ShardEpoch{}, operator.OpShard, operator.TransferLeader{ToStore: 2})
		assert.True(t, oc.checkAddOperator(op))
		assert.True(t, op.Start())
		assert.True(t, op.Replace())
		assert.False(t, oc.checkAddOperator(op))
	}
	{
		// finished op expired
		op1 := operator.NewOperator("test", "test", 1, metapb.ShardEpoch{}, operator.OpShard, operator.TransferLeader{ToStore: 2})
		op2 := operator.NewOperator("test", "test", 2, metapb.ShardEpoch{}, operator.OpShard, operator.TransferLeader{ToStore: 1})
		assert.True(t, oc.checkAddOperator(op1, op2))
		operator.SetOperatorStatusReachTime(op1, operator.CREATED, time.Now().Add(-operator.OperatorExpireTime))
		operator.SetOperatorStatusReachTime(op2, operator.CREATED, time.Now().Add(-operator.OperatorExpireTime))
		assert.False(t, oc.checkAddOperator(op1, op2))
		assert.Equal(t, op1.Status(), operator.EXPIRED)
		assert.Equal(t, op2.Status(), operator.EXPIRED)
	}
	// finished op never timeout

	{
		// unfinished op timeout
		op := operator.NewOperator("test", "test", 1, metapb.ShardEpoch{}, operator.OpShard, steps...)
		assert.True(t, oc.checkAddOperator(op))
		op.Start()
		operator.SetOperatorStatusReachTime(op, operator.STARTED, time.Now().Add(-operator.SlowOperatorWaitTime))
		assert.True(t, op.CheckTimeout())
		assert.False(t, oc.checkAddOperator(op))
	}
}

// issue #1716
func TestConcurrentRemoveOperator(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, tc.ID, tc, false /* no need to run */, nil)
	oc := NewOperatorController(s.ctx, tc, stream)
	tc.AddLeaderStore(1, 0)
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderShard(1, 2, 1)
	res1 := tc.GetShard(1)
	steps := []operator.OpStep{
		operator.RemovePeer{FromStore: 1},
		operator.AddPeer{ToStore: 1, PeerID: 4},
	}
	// finished op with normal priority
	op1 := operator.NewOperator("test", "test", 1, metapb.ShardEpoch{}, operator.OpShard, operator.TransferLeader{ToStore: 2})
	// unfinished op with high priority
	op2 := operator.NewOperator("test", "test", 1, metapb.ShardEpoch{}, operator.OpShard|operator.OpAdmin, steps...)

	assert.True(t, op1.Start())
	oc.SetOperator(op1)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		oc.Dispatch(res1, "test")
		wg.Done()
	}()
	go func() {
		time.Sleep(50 * time.Millisecond)
		success := oc.AddOperator(op2)
		// If the assert failed before wg.Done, the test will be blocked.
		defer assert.True(t, success)
		wg.Done()
	}()
	wg.Wait()

	assert.Equal(t, op2, oc.GetOperator(1))
}

func TestPollDispatchShard(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, tc.ID, tc, false /* no need to run */, nil)
	oc := NewOperatorController(s.ctx, tc, stream)
	tc.AddLeaderStore(1, 2)
	tc.AddLeaderStore(2, 1)
	tc.AddLeaderShard(1, 1, 2)
	tc.AddLeaderShard(2, 1, 2)
	tc.AddLeaderShard(4, 2, 1)
	steps := []operator.OpStep{
		operator.RemovePeer{FromStore: 2},
		operator.AddPeer{ToStore: 2, PeerID: 4},
	}
	op1 := operator.NewOperator("test", "test", 1, metapb.ShardEpoch{}, operator.OpShard, operator.TransferLeader{ToStore: 2})
	op2 := operator.NewOperator("test", "test", 2, metapb.ShardEpoch{}, operator.OpShard, steps...)
	op3 := operator.NewOperator("test", "test", 3, metapb.ShardEpoch{}, operator.OpShard, steps...)
	op4 := operator.NewOperator("test", "test", 4, metapb.ShardEpoch{}, operator.OpShard, operator.TransferLeader{ToStore: 2})
	res1 := tc.GetShard(1)
	res2 := tc.GetShard(2)
	res4 := tc.GetShard(4)
	// Adds operator and pushes to the notifier queue.
	{
		assert.True(t, op1.Start())
		oc.SetOperator(op1)
		assert.True(t, op3.Start())
		oc.SetOperator(op3)
		assert.True(t, op4.Start())
		oc.SetOperator(op4)
		assert.True(t, op2.Start())
		oc.SetOperator(op2)
		heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op1, time: time.Now().Add(100 * time.Millisecond)})
		heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op3, time: time.Now().Add(300 * time.Millisecond)})
		heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op4, time: time.Now().Add(499 * time.Millisecond)})
		heap.Push(&oc.opNotifierQueue, &operatorWithTime{op: op2, time: time.Now().Add(500 * time.Millisecond)})
	}
	// first poll got nil
	r, next := oc.pollNeedDispatchShard()
	assert.Nil(t, r)
	assert.False(t, next)

	// after wait 100 millisecond, the resource1 need to dispatch, but not resource2.
	time.Sleep(100 * time.Millisecond)
	r, next = oc.pollNeedDispatchShard()
	assert.NotNil(t, r)
	assert.True(t, next)
	assert.Equal(t, res1.Meta.ID(), r.Meta.ID())

	// find op3 with nil resource, remove it
	assert.NotNil(t, oc.GetOperator(3))
	r, next = oc.pollNeedDispatchShard()
	assert.Nil(t, r)
	assert.True(t, next)
	assert.Nil(t, oc.GetOperator(3))

	// find op4 finished
	r, next = oc.pollNeedDispatchShard()
	assert.NotNil(t, r)
	assert.True(t, next)
	assert.Equal(t, res4.Meta.ID(), r.Meta.ID())

	// after waiting 500 milliseconds, the resource2 need to dispatch
	time.Sleep(400 * time.Millisecond)
	r, next = oc.pollNeedDispatchShard()
	assert.NotNil(t, r)
	assert.True(t, next)
	assert.Equal(t, res2.Meta.ID(), r.Meta.ID())
	r, next = oc.pollNeedDispatchShard()
	assert.Nil(t, r)
	assert.False(t, next)
}

func TestStoreLimit(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, tc.ID, tc, false /* no need to run */, nil)
	oc := NewOperatorController(s.ctx, tc, stream)
	tc.AddLeaderStore(1, 0)
	tc.UpdateLeaderCount(1, 1000)
	tc.AddLeaderStore(2, 0)
	for i := uint64(1); i <= 1000; i++ {
		tc.AddLeaderShard(i, i)
		// make it small resource
		tc.PutShard(tc.GetShard(i).Clone(core.SetApproximateSize(10)))
	}

	tc.SetStoreLimit(2, limit.AddPeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op := operator.NewOperator("test", "test", 1, metapb.ShardEpoch{}, operator.OpShard, operator.AddPeer{ToStore: 2, PeerID: i})
		assert.True(t, oc.AddOperator(op))
		checkRemoveOperatorSuccess(t, oc, op)
	}
	op := operator.NewOperator("test", "test", 1, metapb.ShardEpoch{}, operator.OpShard, operator.AddPeer{ToStore: 2, PeerID: 1})
	assert.False(t, oc.AddOperator(op))
	assert.False(t, oc.RemoveOperator(op, ""))

	tc.SetStoreLimit(2, limit.AddPeer, 120)
	for i := uint64(1); i <= 10; i++ {
		op = operator.NewOperator("test", "test", i, metapb.ShardEpoch{}, operator.OpShard, operator.AddPeer{ToStore: 2, PeerID: i})
		assert.True(t, oc.AddOperator(op))
		checkRemoveOperatorSuccess(t, oc, op)
	}
	tc.SetAllStoresLimit(limit.AddPeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op = operator.NewOperator("test", "test", i, metapb.ShardEpoch{}, operator.OpShard, operator.AddPeer{ToStore: 2, PeerID: i})
		assert.True(t, oc.AddOperator(op))
		checkRemoveOperatorSuccess(t, oc, op)
	}
	op = operator.NewOperator("test", "test", 1, metapb.ShardEpoch{}, operator.OpShard, operator.AddPeer{ToStore: 2, PeerID: 1})
	assert.False(t, oc.AddOperator(op))
	assert.False(t, oc.RemoveOperator(op, ""))

	tc.SetStoreLimit(2, limit.RemovePeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op := operator.NewOperator("test", "test", 1, metapb.ShardEpoch{}, operator.OpShard, operator.RemovePeer{FromStore: 2})
		assert.True(t, oc.AddOperator(op))
		checkRemoveOperatorSuccess(t, oc, op)
	}
	op = operator.NewOperator("test", "test", 1, metapb.ShardEpoch{}, operator.OpShard, operator.RemovePeer{FromStore: 2})
	assert.False(t, oc.AddOperator(op))
	assert.False(t, oc.RemoveOperator(op, ""))

	tc.SetStoreLimit(2, limit.RemovePeer, 120)
	for i := uint64(1); i <= 10; i++ {
		op = operator.NewOperator("test", "test", i, metapb.ShardEpoch{}, operator.OpShard, operator.RemovePeer{FromStore: 2})
		assert.True(t, oc.AddOperator(op))
		checkRemoveOperatorSuccess(t, oc, op)
	}
	tc.SetAllStoresLimit(limit.RemovePeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op = operator.NewOperator("test", "test", i, metapb.ShardEpoch{}, operator.OpShard, operator.RemovePeer{FromStore: 2})
		assert.True(t, oc.AddOperator(op))
		checkRemoveOperatorSuccess(t, oc, op)
	}
	op = operator.NewOperator("test", "test", 1, metapb.ShardEpoch{}, operator.OpShard, operator.RemovePeer{FromStore: 2})
	assert.False(t, oc.AddOperator(op))
	assert.False(t, oc.RemoveOperator(op, ""))
}

// #1652
func TestDispatchOutdatedresource(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	cluster := mockcluster.NewCluster(config.NewTestOptions())
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, cluster.ID, cluster, false /* no need to run */, nil)
	controller := NewOperatorController(s.ctx, cluster, stream)

	cluster.AddLeaderStore(1, 2)
	cluster.AddLeaderStore(2, 0)
	cluster.SetAllStoresLimit(limit.RemovePeer, 600)
	cluster.AddLeaderShard(1, 1, 2)
	steps := []operator.OpStep{
		operator.TransferLeader{FromStore: 1, ToStore: 2},
		operator.RemovePeer{FromStore: 1},
	}

	op := operator.NewOperator("test", "test", 1,
		metapb.ShardEpoch{ConfVer: 0, Version: 0},
		operator.OpShard, steps...)
	assert.True(t, controller.AddOperator(op))
	assert.Equal(t, 1, stream.MsgLength())

	// report the result of transferring leader
	resource := cluster.MockCachedShard(1, 2, []uint64{1, 2}, []uint64{},
		metapb.ShardEpoch{ConfVer: 0, Version: 0})

	controller.Dispatch(resource, DispatchFromHeartBeat)
	assert.Equal(t, uint64(0), op.ConfVerChanged(resource))
	assert.Equal(t, 2, stream.MsgLength())

	// report the result of removing peer
	resource = cluster.MockCachedShard(1, 2, []uint64{2}, []uint64{},
		metapb.ShardEpoch{ConfVer: 0, Version: 0})

	controller.Dispatch(resource, DispatchFromHeartBeat)
	assert.Equal(t, uint64(1), op.ConfVerChanged(resource))
	assert.Equal(t, 2, stream.MsgLength())

	// add and dispatch op again, the op should be stale
	op = operator.NewOperator("test", "test", 1,
		metapb.ShardEpoch{ConfVer: 0, Version: 0},
		operator.OpShard, steps...)
	assert.True(t, controller.AddOperator(op))
	assert.Equal(t, uint64(0), op.ConfVerChanged(resource))
	assert.Equal(t, 3, stream.MsgLength())

	// report resource with an abnormal confver
	resource = cluster.MockCachedShard(1, 1, []uint64{1, 2}, []uint64{},
		metapb.ShardEpoch{ConfVer: 1, Version: 0})
	controller.Dispatch(resource, DispatchFromHeartBeat)
	assert.Equal(t, uint64(0), op.ConfVerChanged(resource))
	// no new step
	assert.Equal(t, 3, stream.MsgLength())
}

func TestDispatchUnfinishedStep(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	cluster := mockcluster.NewCluster(config.NewTestOptions())
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, cluster.ID, cluster, false /* no need to run */, nil)
	controller := NewOperatorController(s.ctx, cluster, stream)

	// Create a new resource with epoch(0, 0)
	// the resource has two peers with its peer id allocated incrementally.
	// so the two peers are {peerID: 1, StoreID:  1}, {peerID: 2, StoreID:  2}
	// The peer on container 1 is the leader
	epoch := metapb.ShardEpoch{ConfVer: 0, Version: 0}
	resource := cluster.MockCachedShard(1, 1, []uint64{2}, []uint64{}, epoch)
	// Put resource into cluster, otherwise, AddOperator will fail because of
	// missing resource
	cluster.PutShard(resource)

	// The next allocated peer should have peerid 3, so we add this peer
	// to container 3
	testSteps := [][]operator.OpStep{
		{
			operator.AddLearner{ToStore: 3, PeerID: 3},
			operator.PromoteLearner{ToStore: 3, PeerID: 3},
			operator.TransferLeader{ToStore: 3},
			operator.RemovePeer{FromStore: 1},
		},
		{
			operator.AddLightLearner{ToStore: 3, PeerID: 3},
			operator.PromoteLearner{ToStore: 3, PeerID: 3},
			operator.TransferLeader{ToStore: 3},
			operator.RemovePeer{FromStore: 1},
		},
	}

	for _, steps := range testSteps {
		// Create an operator
		op := operator.NewOperator("test", "test", 1, epoch,
			operator.OpShard, steps...)
		assert.True(t, controller.AddOperator(op))
		assert.Equal(t, 1, stream.MsgLength())

		// Create resource2 which is cloned from the original resource.
		// resource2 has peer 2 in pending state, so the AddPeer step
		// is left unfinished
		resource2 := resource.Clone(
			core.WithAddPeer(metapb.Replica{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Learner}),
			core.WithPendingPeers([]metapb.Replica{
				{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Learner},
			}),
			core.WithIncConfVer(),
		)
		assert.NotEmpty(t, resource2.GetPendingPeers())
		assert.False(t, steps[0].IsFinish(resource2))
		controller.Dispatch(resource2, DispatchFromHeartBeat)

		// In this case, the conf version has been changed, but the
		// peer added is in pending state, the operator should not be
		// removed by the stale checker
		assert.Equal(t, uint64(1), op.ConfVerChanged(resource2))
		assert.NotNil(t, controller.GetOperator(1))
		// The operator is valid yet, but the step should not be sent
		// again, because it is in pending state, so the message channel
		// should not be increased
		assert.Equal(t, 1, stream.MsgLength())

		// Finish the step by clearing the pending state
		resource3 := resource.Clone(
			core.WithAddPeer(metapb.Replica{ID: 3, StoreID: 3, Role: metapb.ReplicaRole_Learner}),
			core.WithIncConfVer(),
		)
		assert.True(t, steps[0].IsFinish(resource3))
		controller.Dispatch(resource3, DispatchFromHeartBeat)
		assert.Equal(t, uint64(1), op.ConfVerChanged(resource3))
		assert.Equal(t, 2, stream.MsgLength())

		resource4 := resource3.Clone(
			core.WithPromoteLearner(3),
			core.WithIncConfVer(),
		)
		assert.True(t, steps[1].IsFinish(resource4))
		controller.Dispatch(resource4, DispatchFromHeartBeat)
		assert.Equal(t, uint64(2), op.ConfVerChanged(resource4))
		assert.Equal(t, 3, stream.MsgLength())

		// Transfer leader
		p, _ := resource4.GetStorePeer(3)
		resource5 := resource4.Clone(
			core.WithLeader(&p),
		)
		assert.True(t, steps[2].IsFinish(resource5))
		controller.Dispatch(resource5, DispatchFromHeartBeat)
		assert.Equal(t, uint64(2), op.ConfVerChanged(resource5))
		assert.Equal(t, 4, stream.MsgLength())

		// Remove peer
		resource6 := resource5.Clone(
			core.WithRemoveStorePeer(1),
			core.WithIncConfVer(),
		)
		assert.True(t, steps[3].IsFinish(resource6))
		controller.Dispatch(resource6, DispatchFromHeartBeat)
		assert.Equal(t, uint64(3), op.ConfVerChanged(resource6))

		// The Operator has finished, so no message should be sent
		assert.Equal(t, 4, stream.MsgLength())
		assert.Nil(t, controller.GetOperator(1))
		e := stream.Drain(4)
		assert.NoError(t, e)
	}
}

func TestStoreLimitWithMerge(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	cfg := config.NewTestOptions()
	tc := mockcluster.NewCluster(cfg)
	tc.SetMaxMergeShardSize(2)
	tc.SetMaxMergeShardKeys(2)
	tc.SetSplitMergeInterval(0)
	resources := []*core.CachedShard{
		newresourceInfo(1, "", "a", 1, 1, []uint64{101, 1}, []uint64{101, 1}, []uint64{102, 2}),
		newresourceInfo(2, "a", "t", 200, 200, []uint64{104, 4}, []uint64{103, 1}, []uint64{104, 4}, []uint64{105, 5}),
		newresourceInfo(3, "t", "x", 1, 1, []uint64{108, 6}, []uint64{106, 2}, []uint64{107, 5}, []uint64{108, 6}),
		newresourceInfo(4, "x", "", 10, 10, []uint64{109, 4}, []uint64{109, 4}),
	}

	for i := uint64(1); i <= 6; i++ {
		tc.AddLeaderStore(i, 10)
	}

	for _, resource := range resources {
		tc.PutShard(resource)
	}

	mc := checker.NewMergeChecker(s.ctx, tc)
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, tc.ID, tc, false /* no need to run */, nil)
	oc := NewOperatorController(s.ctx, tc, stream)

	resources[2] = resources[2].Clone(
		core.SetPeers([]metapb.Replica{
			{ID: 109, StoreID: 2},
			{ID: 110, StoreID: 3},
			{ID: 111, StoreID: 6},
		}),
		core.WithLeader(&metapb.Replica{ID: 109, StoreID: 2}),
	)
	tc.PutShard(resources[2])
	// The size of resource is less or equal than 1MB.
	for i := 0; i < 50; i++ {
		ops := mc.Check(resources[2])
		assert.NotEmpty(t, ops)
		assert.True(t, oc.AddOperator(ops...))
		for _, op := range ops {
			oc.RemoveOperator(op, "")
		}
	}
	resources[2] = resources[2].Clone(
		core.SetApproximateSize(2),
		core.SetApproximateKeys(2),
	)
	tc.PutShard(resources[2])
	// The size of resource is more than 1MB but no more than 20MB.
	for i := 0; i < 5; i++ {
		ops := mc.Check(resources[2])
		assert.NotEmpty(t, ops)
		assert.True(t, oc.AddOperator(ops...))
		for _, op := range ops {
			oc.RemoveOperator(op, "")
		}
	}
	{
		ops := mc.Check(resources[2])
		assert.NotEmpty(t, ops)
		assert.False(t, oc.AddOperator(ops...))
	}
}

func TestAddWaitingOperator(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	cluster := mockcluster.NewCluster(config.NewTestOptions())
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, cluster.ID, cluster, false /* no need to run */, nil)
	controller := NewOperatorController(s.ctx, cluster, stream)
	cluster.AddLabelsStore(1, 1, map[string]string{"host": "host1"})
	cluster.AddLabelsStore(2, 1, map[string]string{"host": "host2"})
	cluster.AddLabelsStore(3, 1, map[string]string{"host": "host3"})
	addPeerOp := func(i uint64) *operator.Operator {
		start := fmt.Sprintf("%da", i)
		end := fmt.Sprintf("%db", i)
		resource := newresourceInfo(i, start, end, 1, 1, []uint64{101, 1}, []uint64{101, 1})
		cluster.PutShard(resource)
		peer := metapb.Replica{
			StoreID: 2,
		}
		op, err := operator.CreateAddPeerOperator("add-peer", cluster, resource, peer, operator.OpKind(0))
		assert.NoError(t, err)
		assert.NotNil(t, op)
		return op
	}

	// a batch of operators should be added atomically
	var batch []*operator.Operator
	for i := uint64(0); i < cluster.GetSchedulerMaxWaitingOperator()-1; i++ {
		batch = append(batch, addPeerOp(i))
	}
	added := controller.AddWaitingOperator(batch...)
	assert.Equal(t, int(cluster.GetSchedulerMaxWaitingOperator()-1), added)

	source := newresourceInfo(1, "1a", "1b", 1, 1, []uint64{101, 1}, []uint64{101, 1})
	target := newresourceInfo(0, "0a", "0b", 1, 1, []uint64{101, 1}, []uint64{101, 1})
	// now there is one operator being allowed to add, if it is a merge operator
	// both of the pair are allowed
	ops, err := operator.CreateMergeShardOperator("merge-resource", cluster, source, target, operator.OpMerge)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(ops))
	assert.Equal(t, 2, controller.AddWaitingOperator(ops...))

	// no space left, new operator can not be added.
	assert.Equal(t, 0, controller.AddWaitingOperator(addPeerOp(0)))
}

func checkRemoveOperatorSuccess(t *testing.T, oc *OperatorController, op *operator.Operator) {
	assert.True(t, oc.RemoveOperator(op, ""))
	assert.True(t, op.IsEnd())
	assert.True(t, reflect.DeepEqual(op, oc.GetOperatorStatus(op.ShardID()).Op))
}

func newresourceInfo(id uint64, startKey, endKey string, size, keys int64, leader []uint64, peers ...[]uint64) *core.CachedShard {
	prs := make([]metapb.Replica, 0, len(peers))
	for _, peer := range peers {
		prs = append(prs, metapb.Replica{ID: peer[0], StoreID: peer[1]})
	}
	return core.NewCachedShard(
		&metadata.TestShard{
			ResID:    id,
			Start:    []byte(startKey),
			End:      []byte(endKey),
			ResPeers: prs,
		},
		&metapb.Replica{ID: leader[0], StoreID: leader[1]},
		core.SetApproximateSize(size),
		core.SetApproximateKeys(keys),
	)
}
