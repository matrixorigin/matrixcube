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
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/checker"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/hbstream"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
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
	tc.AddLeaderContainer(2, 1)
	tc.AddLeaderResource(1, 1, 2)
	tc.AddLeaderResource(2, 1, 2)
	steps := []operator.OpStep{
		operator.RemovePeer{FromContainer: 2},
	}
	op1 := operator.NewOperator("test", "test", 1, metapb.ResourceEpoch{}, operator.OpResource, steps...)
	op2 := operator.NewOperator("test", "test", 2, metapb.ResourceEpoch{}, operator.OpResource, steps...)
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
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, tc.ID, tc, false /* no need to run */)
	oc := NewOperatorController(s.ctx, tc, stream)
	tc.AddLeaderContainer(1, 2)
	tc.AddLeaderContainer(2, 0)
	tc.AddLeaderResource(1, 1, 2)
	tc.AddLeaderResource(2, 1, 2)
	steps := []operator.OpStep{
		operator.RemovePeer{FromContainer: 2},
		operator.AddPeer{ToContainer: 2, PeerID: 4},
	}
	op1 := operator.NewOperator("test", "test", 1, metapb.ResourceEpoch{}, operator.OpResource, steps...)
	op2 := operator.NewOperator("test", "test", 2, metapb.ResourceEpoch{}, operator.OpResource, steps...)
	res1 := tc.GetResource(1)
	res2 := tc.GetResource(2)
	assert.True(t, op1.Start())
	oc.SetOperator(op1)
	assert.True(t, op2.Start())
	oc.SetOperator(op2)
	assert.Equal(t, oc.GetOperatorStatus(1).Status, metapb.OperatorStatus_RUNNING)
	assert.Equal(t, oc.GetOperatorStatus(2).Status, metapb.OperatorStatus_RUNNING)
	operator.SetOperatorStatusReachTime(op1, operator.STARTED, time.Now().Add(-10*time.Minute))
	res2 = ApplyOperatorStep(res2, op2)
	tc.PutResource(res2)
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
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, tc.ID, tc, false /* no need to run */)
	oc := NewOperatorController(s.ctx, tc, stream)
	tc.AddLeaderContainer(1, 2)
	tc.AddLeaderContainer(2, 0)
	tc.AddLeaderContainer(3, 0)
	tc.AddLeaderResource(1, 1, 2)
	steps := []operator.OpStep{
		operator.RemovePeer{FromContainer: 2},
		operator.AddPeer{ToContainer: 3, PeerID: 4},
	}
	op := operator.NewOperator("test", "test", 1, metapb.ResourceEpoch{}, operator.OpResource, steps...)
	res := tc.GetResource(1)
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
	op = operator.NewOperator("test", "test", 1, metapb.ResourceEpoch{}, operator.OpResource, operator.TransferLeader{ToContainer: 5})
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
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, tc.ID, tc, false /* no need to run */)
	oc := NewOperatorController(s.ctx, tc, stream)
	tc.AddLeaderContainer(1, 0)
	tc.AddLeaderContainer(2, 1)
	tc.AddLeaderResource(1, 2, 1)
	tc.AddLeaderResource(2, 2, 1)
	res1 := tc.GetResource(1)
	steps := []operator.OpStep{
		operator.RemovePeer{FromContainer: 1},
		operator.AddPeer{ToContainer: 1, PeerID: 4},
	}
	{
		// finished op
		op := operator.NewOperator("test", "test", 1, metapb.ResourceEpoch{}, operator.OpResource, operator.TransferLeader{ToContainer: 2})
		assert.True(t, oc.checkAddOperator(op))
		op.Start()
		assert.False(t, oc.checkAddOperator(op)) // started
		assert.Nil(t, op.Check(res1))
		assert.Equal(t, operator.SUCCESS, op.Status())
		assert.False(t, oc.checkAddOperator(op)) // success
	}
	{
		// finished op canceled
		op := operator.NewOperator("test", "test", 1, metapb.ResourceEpoch{}, operator.OpResource, operator.TransferLeader{ToContainer: 2})
		assert.True(t, oc.checkAddOperator(op))
		assert.True(t, op.Cancel())
		assert.False(t, oc.checkAddOperator(op))
	}
	{
		// finished op replaced
		op := operator.NewOperator("test", "test", 1, metapb.ResourceEpoch{}, operator.OpResource, operator.TransferLeader{ToContainer: 2})
		assert.True(t, oc.checkAddOperator(op))
		assert.True(t, op.Start())
		assert.True(t, op.Replace())
		assert.False(t, oc.checkAddOperator(op))
	}
	{
		// finished op expired
		op1 := operator.NewOperator("test", "test", 1, metapb.ResourceEpoch{}, operator.OpResource, operator.TransferLeader{ToContainer: 2})
		op2 := operator.NewOperator("test", "test", 2, metapb.ResourceEpoch{}, operator.OpResource, operator.TransferLeader{ToContainer: 1})
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
		op := operator.NewOperator("test", "test", 1, metapb.ResourceEpoch{}, operator.OpResource, steps...)
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
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, tc.ID, tc, false /* no need to run */)
	oc := NewOperatorController(s.ctx, tc, stream)
	tc.AddLeaderContainer(1, 0)
	tc.AddLeaderContainer(2, 1)
	tc.AddLeaderResource(1, 2, 1)
	res1 := tc.GetResource(1)
	steps := []operator.OpStep{
		operator.RemovePeer{FromContainer: 1},
		operator.AddPeer{ToContainer: 1, PeerID: 4},
	}
	// finished op with normal priority
	op1 := operator.NewOperator("test", "test", 1, metapb.ResourceEpoch{}, operator.OpResource, operator.TransferLeader{ToContainer: 2})
	// unfinished op with high priority
	op2 := operator.NewOperator("test", "test", 1, metapb.ResourceEpoch{}, operator.OpResource|operator.OpAdmin, steps...)

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

func TestPollDispatchResource(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, tc.ID, tc, false /* no need to run */)
	oc := NewOperatorController(s.ctx, tc, stream)
	tc.AddLeaderContainer(1, 2)
	tc.AddLeaderContainer(2, 1)
	tc.AddLeaderResource(1, 1, 2)
	tc.AddLeaderResource(2, 1, 2)
	tc.AddLeaderResource(4, 2, 1)
	steps := []operator.OpStep{
		operator.RemovePeer{FromContainer: 2},
		operator.AddPeer{ToContainer: 2, PeerID: 4},
	}
	op1 := operator.NewOperator("test", "test", 1, metapb.ResourceEpoch{}, operator.OpResource, operator.TransferLeader{ToContainer: 2})
	op2 := operator.NewOperator("test", "test", 2, metapb.ResourceEpoch{}, operator.OpResource, steps...)
	op3 := operator.NewOperator("test", "test", 3, metapb.ResourceEpoch{}, operator.OpResource, steps...)
	op4 := operator.NewOperator("test", "test", 4, metapb.ResourceEpoch{}, operator.OpResource, operator.TransferLeader{ToContainer: 2})
	res1 := tc.GetResource(1)
	res2 := tc.GetResource(2)
	res4 := tc.GetResource(4)
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
	r, next := oc.pollNeedDispatchResource()
	assert.Nil(t, r)
	assert.False(t, next)

	// after wait 100 millisecond, the resource1 need to dispatch, but not resource2.
	time.Sleep(100 * time.Millisecond)
	r, next = oc.pollNeedDispatchResource()
	assert.NotNil(t, r)
	assert.True(t, next)
	assert.Equal(t, res1.Meta.ID(), r.Meta.ID())

	// find op3 with nil resource, remove it
	assert.NotNil(t, oc.GetOperator(3))
	r, next = oc.pollNeedDispatchResource()
	assert.Nil(t, r)
	assert.True(t, next)
	assert.Nil(t, oc.GetOperator(3))

	// find op4 finished
	r, next = oc.pollNeedDispatchResource()
	assert.NotNil(t, r)
	assert.True(t, next)
	assert.Equal(t, res4.Meta.ID(), r.Meta.ID())

	// after waiting 500 milliseconds, the resource2 need to dispatch
	time.Sleep(400 * time.Millisecond)
	r, next = oc.pollNeedDispatchResource()
	assert.NotNil(t, r)
	assert.True(t, next)
	assert.Equal(t, res2.Meta.ID(), r.Meta.ID())
	r, next = oc.pollNeedDispatchResource()
	assert.Nil(t, r)
	assert.False(t, next)
}

func TestContainerLimit(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	opt := config.NewTestOptions()
	tc := mockcluster.NewCluster(opt)
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, tc.ID, tc, false /* no need to run */)
	oc := NewOperatorController(s.ctx, tc, stream)
	tc.AddLeaderContainer(1, 0)
	tc.UpdateLeaderCount(1, 1000)
	tc.AddLeaderContainer(2, 0)
	for i := uint64(1); i <= 1000; i++ {
		tc.AddLeaderResource(i, i)
		// make it small resource
		tc.PutResource(tc.GetResource(i).Clone(core.SetApproximateSize(10)))
	}

	tc.SetContainerLimit(2, limit.AddPeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op := operator.NewOperator("test", "test", 1, metapb.ResourceEpoch{}, operator.OpResource, operator.AddPeer{ToContainer: 2, PeerID: i})
		assert.True(t, oc.AddOperator(op))
		checkRemoveOperatorSuccess(t, oc, op)
	}
	op := operator.NewOperator("test", "test", 1, metapb.ResourceEpoch{}, operator.OpResource, operator.AddPeer{ToContainer: 2, PeerID: 1})
	assert.False(t, oc.AddOperator(op))
	assert.False(t, oc.RemoveOperator(op, ""))

	tc.SetContainerLimit(2, limit.AddPeer, 120)
	for i := uint64(1); i <= 10; i++ {
		op = operator.NewOperator("test", "test", i, metapb.ResourceEpoch{}, operator.OpResource, operator.AddPeer{ToContainer: 2, PeerID: i})
		assert.True(t, oc.AddOperator(op))
		checkRemoveOperatorSuccess(t, oc, op)
	}
	tc.SetAllContainersLimit(limit.AddPeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op = operator.NewOperator("test", "test", i, metapb.ResourceEpoch{}, operator.OpResource, operator.AddPeer{ToContainer: 2, PeerID: i})
		assert.True(t, oc.AddOperator(op))
		checkRemoveOperatorSuccess(t, oc, op)
	}
	op = operator.NewOperator("test", "test", 1, metapb.ResourceEpoch{}, operator.OpResource, operator.AddPeer{ToContainer: 2, PeerID: 1})
	assert.False(t, oc.AddOperator(op))
	assert.False(t, oc.RemoveOperator(op, ""))

	tc.SetContainerLimit(2, limit.RemovePeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op := operator.NewOperator("test", "test", 1, metapb.ResourceEpoch{}, operator.OpResource, operator.RemovePeer{FromContainer: 2})
		assert.True(t, oc.AddOperator(op))
		checkRemoveOperatorSuccess(t, oc, op)
	}
	op = operator.NewOperator("test", "test", 1, metapb.ResourceEpoch{}, operator.OpResource, operator.RemovePeer{FromContainer: 2})
	assert.False(t, oc.AddOperator(op))
	assert.False(t, oc.RemoveOperator(op, ""))

	tc.SetContainerLimit(2, limit.RemovePeer, 120)
	for i := uint64(1); i <= 10; i++ {
		op = operator.NewOperator("test", "test", i, metapb.ResourceEpoch{}, operator.OpResource, operator.RemovePeer{FromContainer: 2})
		assert.True(t, oc.AddOperator(op))
		checkRemoveOperatorSuccess(t, oc, op)
	}
	tc.SetAllContainersLimit(limit.RemovePeer, 60)
	for i := uint64(1); i <= 5; i++ {
		op = operator.NewOperator("test", "test", i, metapb.ResourceEpoch{}, operator.OpResource, operator.RemovePeer{FromContainer: 2})
		assert.True(t, oc.AddOperator(op))
		checkRemoveOperatorSuccess(t, oc, op)
	}
	op = operator.NewOperator("test", "test", 1, metapb.ResourceEpoch{}, operator.OpResource, operator.RemovePeer{FromContainer: 2})
	assert.False(t, oc.AddOperator(op))
	assert.False(t, oc.RemoveOperator(op, ""))
}

// #1652
func TestDispatchOutdatedresource(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	cluster := mockcluster.NewCluster(config.NewTestOptions())
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, cluster.ID, cluster, false /* no need to run */)
	controller := NewOperatorController(s.ctx, cluster, stream)

	cluster.AddLeaderContainer(1, 2)
	cluster.AddLeaderContainer(2, 0)
	cluster.SetAllContainersLimit(limit.RemovePeer, 600)
	cluster.AddLeaderResource(1, 1, 2)
	steps := []operator.OpStep{
		operator.TransferLeader{FromContainer: 1, ToContainer: 2},
		operator.RemovePeer{FromContainer: 1},
	}

	op := operator.NewOperator("test", "test", 1,
		metapb.ResourceEpoch{ConfVer: 0, Version: 0},
		operator.OpResource, steps...)
	assert.True(t, controller.AddOperator(op))
	assert.Equal(t, 1, stream.MsgLength())

	// report the result of transferring leader
	resource := cluster.MockCachedResource(1, 2, []uint64{1, 2}, []uint64{},
		metapb.ResourceEpoch{ConfVer: 0, Version: 0})

	controller.Dispatch(resource, DispatchFromHeartBeat)
	assert.Equal(t, uint64(0), op.ConfVerChanged(resource))
	assert.Equal(t, 2, stream.MsgLength())

	// report the result of removing peer
	resource = cluster.MockCachedResource(1, 2, []uint64{2}, []uint64{},
		metapb.ResourceEpoch{ConfVer: 0, Version: 0})

	controller.Dispatch(resource, DispatchFromHeartBeat)
	assert.Equal(t, uint64(1), op.ConfVerChanged(resource))
	assert.Equal(t, 2, stream.MsgLength())

	// add and dispatch op again, the op should be stale
	op = operator.NewOperator("test", "test", 1,
		metapb.ResourceEpoch{ConfVer: 0, Version: 0},
		operator.OpResource, steps...)
	assert.True(t, controller.AddOperator(op))
	assert.Equal(t, uint64(0), op.ConfVerChanged(resource))
	assert.Equal(t, 3, stream.MsgLength())

	// report resource with an abnormal confver
	resource = cluster.MockCachedResource(1, 1, []uint64{1, 2}, []uint64{},
		metapb.ResourceEpoch{ConfVer: 1, Version: 0})
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
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, cluster.ID, cluster, false /* no need to run */)
	controller := NewOperatorController(s.ctx, cluster, stream)

	// Create a new resource with epoch(0, 0)
	// the resource has two peers with its peer id allocated incrementally.
	// so the two peers are {peerID: 1, ContainerID:  1}, {peerID: 2, ContainerID:  2}
	// The peer on container 1 is the leader
	epoch := metapb.ResourceEpoch{ConfVer: 0, Version: 0}
	resource := cluster.MockCachedResource(1, 1, []uint64{2}, []uint64{}, epoch)
	// Put resource into cluster, otherwise, AddOperator will fail because of
	// missing resource
	cluster.PutResource(resource)

	// The next allocated peer should have peerid 3, so we add this peer
	// to container 3
	testSteps := [][]operator.OpStep{
		{
			operator.AddLearner{ToContainer: 3, PeerID: 3},
			operator.PromoteLearner{ToContainer: 3, PeerID: 3},
			operator.TransferLeader{ToContainer: 3},
			operator.RemovePeer{FromContainer: 1},
		},
		{
			operator.AddLightLearner{ToContainer: 3, PeerID: 3},
			operator.PromoteLearner{ToContainer: 3, PeerID: 3},
			operator.TransferLeader{ToContainer: 3},
			operator.RemovePeer{FromContainer: 1},
		},
	}

	for _, steps := range testSteps {
		// Create an operator
		op := operator.NewOperator("test", "test", 1, epoch,
			operator.OpResource, steps...)
		assert.True(t, controller.AddOperator(op))
		assert.Equal(t, 1, stream.MsgLength())

		// Create resource2 which is cloned from the original resource.
		// resource2 has peer 2 in pending state, so the AddPeer step
		// is left unfinished
		resource2 := resource.Clone(
			core.WithAddPeer(metapb.Peer{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Learner}),
			core.WithPendingPeers([]metapb.Peer{
				{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Learner},
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
			core.WithAddPeer(metapb.Peer{ID: 3, ContainerID: 3, Role: metapb.PeerRole_Learner}),
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
		p, _ := resource4.GetContainerPeer(3)
		resource5 := resource4.Clone(
			core.WithLeader(&p),
		)
		assert.True(t, steps[2].IsFinish(resource5))
		controller.Dispatch(resource5, DispatchFromHeartBeat)
		assert.Equal(t, uint64(2), op.ConfVerChanged(resource5))
		assert.Equal(t, 4, stream.MsgLength())

		// Remove peer
		resource6 := resource5.Clone(
			core.WithRemoveContainerPeer(1),
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

func TestContainerLimitWithMerge(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	cfg := config.NewTestOptions()
	tc := mockcluster.NewCluster(cfg)
	tc.SetMaxMergeResourceSize(2)
	tc.SetMaxMergeResourceKeys(2)
	tc.SetSplitMergeInterval(0)
	resources := []*core.CachedResource{
		newresourceInfo(1, "", "a", 1, 1, []uint64{101, 1}, []uint64{101, 1}, []uint64{102, 2}),
		newresourceInfo(2, "a", "t", 200, 200, []uint64{104, 4}, []uint64{103, 1}, []uint64{104, 4}, []uint64{105, 5}),
		newresourceInfo(3, "t", "x", 1, 1, []uint64{108, 6}, []uint64{106, 2}, []uint64{107, 5}, []uint64{108, 6}),
		newresourceInfo(4, "x", "", 10, 10, []uint64{109, 4}, []uint64{109, 4}),
	}

	for i := uint64(1); i <= 6; i++ {
		tc.AddLeaderContainer(i, 10)
	}

	for _, resource := range resources {
		tc.PutResource(resource)
	}

	mc := checker.NewMergeChecker(s.ctx, tc)
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, tc.ID, tc, false /* no need to run */)
	oc := NewOperatorController(s.ctx, tc, stream)

	resources[2] = resources[2].Clone(
		core.SetPeers([]metapb.Peer{
			{ID: 109, ContainerID: 2},
			{ID: 110, ContainerID: 3},
			{ID: 111, ContainerID: 6},
		}),
		core.WithLeader(&metapb.Peer{ID: 109, ContainerID: 2}),
	)
	tc.PutResource(resources[2])
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
	tc.PutResource(resources[2])
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
	stream := hbstream.NewTestHeartbeatStreams(s.ctx, cluster.ID, cluster, false /* no need to run */)
	controller := NewOperatorController(s.ctx, cluster, stream)
	cluster.AddLabelsContainer(1, 1, map[string]string{"host": "host1"})
	cluster.AddLabelsContainer(2, 1, map[string]string{"host": "host2"})
	cluster.AddLabelsContainer(3, 1, map[string]string{"host": "host3"})
	addPeerOp := func(i uint64) *operator.Operator {
		start := fmt.Sprintf("%da", i)
		end := fmt.Sprintf("%db", i)
		resource := newresourceInfo(i, start, end, 1, 1, []uint64{101, 1}, []uint64{101, 1})
		cluster.PutResource(resource)
		peer := metapb.Peer{
			ContainerID: 2,
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
	ops, err := operator.CreateMergeResourceOperator("merge-resource", cluster, source, target, operator.OpMerge)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(ops))
	assert.Equal(t, 2, controller.AddWaitingOperator(ops...))

	// no space left, new operator can not be added.
	assert.Equal(t, 0, controller.AddWaitingOperator(addPeerOp(0)))
}

func checkRemoveOperatorSuccess(t *testing.T, oc *OperatorController, op *operator.Operator) {
	assert.True(t, oc.RemoveOperator(op, ""))
	assert.True(t, op.IsEnd())
	assert.True(t, reflect.DeepEqual(op, oc.GetOperatorStatus(op.ResourceID()).Op))
}

func newresourceInfo(id uint64, startKey, endKey string, size, keys int64, leader []uint64, peers ...[]uint64) *core.CachedResource {
	prs := make([]metapb.Peer, 0, len(peers))
	for _, peer := range peers {
		prs = append(prs, metapb.Peer{ID: peer[0], ContainerID: peer[1]})
	}
	return core.NewCachedResource(
		&metadata.TestResource{
			ResID:    id,
			Start:    []byte(startKey),
			End:      []byte(endKey),
			ResPeers: prs,
		},
		&metapb.Peer{ID: leader[0], ContainerID: leader[1]},
		core.SetApproximateSize(size),
		core.SetApproximateKeys(keys),
	)
}
