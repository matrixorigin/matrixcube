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

package cluster

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockhbstream"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/hbstream"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/schedulers"
	"github.com/matrixorigin/matrixcube/components/prophet/statistics"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/testutil"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/stretchr/testify/assert"
)

func newTestOperator(shardID uint64, shardEpoch metapb.ShardEpoch, kind operator.OpKind, steps ...operator.OpStep) *operator.Operator {
	return operator.NewOperator("test", "test", shardID, shardEpoch, kind, steps...)
}

func (c *testCluster) AllocPeer(storeID uint64) (metapb.Replica, error) {
	id, err := c.AllocID()
	if err != nil {
		return metapb.Replica{}, err
	}
	return metapb.Replica{ID: id, StoreID: storeID}, nil
}

func (c *testCluster) addShardStore(storeID uint64, shardCount int, shardSizes ...uint64) error {
	var shardSize uint64
	if len(shardSizes) == 0 {
		shardSize = uint64(shardCount) * 10
	} else {
		shardSize = shardSizes[0]
	}

	stats := &metapb.StoreStats{}
	stats.Capacity = 100 * (1 << 30)
	stats.UsedSize = shardSize * (1 << 20)
	stats.Available = stats.Capacity - stats.UsedSize
	newStore := core.NewCachedStore(metapb.Store{ID: storeID},
		core.SetStoreStats(stats),
		core.SetShardCount("", shardCount),
		core.SetShardSize("", int64(shardSize)),
		core.SetLastHeartbeatTS(time.Now()),
	)

	c.SetStoreLimit(storeID, limit.AddPeer, 60)
	c.SetStoreLimit(storeID, limit.RemovePeer, 60)
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) addLeaderShard(shardID uint64, leaderStoreID uint64, followerStoreIDs ...uint64) error {
	shard := *newTestShardMeta(shardID)
	leader, _ := c.AllocPeer(leaderStoreID)
	shard.SetReplicas([]metapb.Replica{leader})
	for _, followerStoreID := range followerStoreIDs {
		peer, _ := c.AllocPeer(followerStoreID)
		shard.SetReplicas(append(shard.GetReplicas(), peer))
	}
	shardInfo := core.NewCachedShard(shard, &leader, core.SetApproximateSize(10), core.SetApproximateKeys(10))
	c.core.PutShard(shardInfo)
	return nil
}

func (c *testCluster) updateLeaderCount(storeID uint64, leaderCount int) error {
	store := c.GetStore(storeID)
	newStore := store.Clone(
		core.SetLeaderCount("", leaderCount),
		core.SetLeaderSize("", int64(leaderCount)*10),
	)
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) addLeaderStore(storeID uint64, leaderCount int) error {
	stats := &metapb.StoreStats{}
	newStore := core.NewCachedStore(metapb.Store{ID: storeID},
		core.SetStoreStats(stats),
		core.SetLeaderCount("", leaderCount),
		core.SetLeaderSize("", int64(leaderCount)*10),
		core.SetLastHeartbeatTS(time.Now()),
	)

	c.SetStoreLimit(storeID, limit.AddPeer, 60)
	c.SetStoreLimit(storeID, limit.RemovePeer, 60)
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) setStoreDown(storeID uint64) error {
	store := c.GetStore(storeID)
	newStore := store.Clone(
		core.UpStore(),
		core.SetLastHeartbeatTS(time.Time{}),
	)
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) setStoreOffline(storeID uint64) error {
	store := c.GetStore(storeID)
	newStore := store.Clone(core.OfflineStore(false))
	c.Lock()
	defer c.Unlock()
	return c.putStoreLocked(newStore)
}

func (c *testCluster) LoadShard(shardID uint64, followerStoreIDs ...uint64) error {
	//  shards load from etcd will have no leader
	shard := *newTestShardMeta(shardID)
	shard.SetReplicas([]metapb.Replica{})
	for _, id := range followerStoreIDs {
		peer, _ := c.AllocPeer(id)
		shard.SetReplicas(append(shard.GetReplicas(), peer))
	}
	c.core.PutShard(core.NewCachedShard(shard, nil))
	return nil
}

func TestBasic(t *testing.T) {
	tc, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()
	oc := co.opController

	assert.Nil(t, tc.addLeaderShard(1, 1))

	op1 := newTestOperator(1, tc.GetShard(1).Meta.GetEpoch(), operator.OpLeader)
	oc.AddWaitingOperator(op1)
	assert.Equal(t, uint64(1), oc.OperatorCount(op1.Kind()))
	assert.Equal(t, op1.ShardID(), oc.GetOperator(1).ShardID())

	// shard 1 already has an operator, cannot add another one.
	op2 := newTestOperator(1, tc.GetShard(1).Meta.GetEpoch(), operator.OpShard)
	oc.AddWaitingOperator(op2)
	assert.Equal(t, uint64(0), oc.OperatorCount(op2.Kind()))

	// Remove the operator manually, then we can add a new operator.
	assert.True(t, oc.RemoveOperator(op1, ""))
	op3 := newTestOperator(1, tc.GetShard(1).Meta.GetEpoch(), operator.OpShard)
	oc.AddWaitingOperator(op3)
	assert.Equal(t, uint64(1), oc.OperatorCount(op3.Kind()))
	assert.Equal(t, op3.ShardID(), oc.GetOperator(1).ShardID())
}

func TestDispatch(t *testing.T) {
	tc, co, cleanup := prepare(t, nil, func(tc *testCluster) { tc.prepareChecker.isPrepared = true }, nil)
	defer cleanup()
	tc.DisableJointConsensus()
	// Transfer peer from store 4 to store 1.
	assert.Nil(t, tc.addShardStore(4, 40))
	assert.Nil(t, tc.addShardStore(3, 30))
	assert.Nil(t, tc.addShardStore(2, 20))
	assert.Nil(t, tc.addShardStore(1, 10))
	assert.Nil(t, tc.addLeaderShard(1, 2, 3, 4))

	// Transfer leader from store 4 to store 2.
	assert.Nil(t, tc.updateLeaderCount(4, 50))
	assert.Nil(t, tc.updateLeaderCount(3, 50))
	assert.Nil(t, tc.updateLeaderCount(2, 20))
	assert.Nil(t, tc.updateLeaderCount(1, 10))
	assert.Nil(t, tc.addLeaderShard(2, 4, 3, 2))

	co.run()

	// Wait for schedule and turn off balance.
	waitOperator(t, co, 1)
	testutil.CheckTransferPeer(t, co.opController.GetOperator(1), operator.OpKind(0), 4, 1)
	assert.Nil(t, co.removeScheduler(schedulers.BalanceShardName))
	waitOperator(t, co, 2)
	testutil.CheckTransferLeader(t, co.opController.GetOperator(2), operator.OpKind(0), 4, 2)
	assert.Nil(t, co.removeScheduler(schedulers.BalanceLeaderName))

	stream := mockhbstream.NewHeartbeatStream()

	// Transfer peer.
	shard := tc.GetShard(1).Clone()
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	shard = waitAddLearner(t, stream, shard, 1)
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	shard = waitPromoteLearner(t, stream, shard, 1)
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	shard = waitRemovePeer(t, stream, shard, 4)
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	waitNoResponse(t, stream)

	// Transfer leader.
	shard = tc.GetShard(2).Clone()
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	waitTransferLeader(t, stream, shard, 2)
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	waitNoResponse(t, stream)
}

func TestCollectMetrics(t *testing.T) {
	tc, co, cleanup := prepare(t, nil, func(tc *testCluster) {
		tc.shardStats = statistics.NewShardStatistics(tc.GetOpts(), nil)
	}, func(co *coordinator) { co.run() })
	defer cleanup()

	// Make sure there are no problem when concurrent write and read
	var wg sync.WaitGroup
	count := 10
	wg.Add(count + 1)
	for i := 0; i <= count; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				assert.Nil(t, tc.addShardStore(uint64(i%5), rand.Intn(200)))
			}
		}(i)
	}
	for i := 0; i < 1000; i++ {
		co.collectSchedulerMetrics()
		co.cluster.collectClusterMetrics()
	}
	co.resetSchedulerMetrics()
	co.cluster.resetClusterMetrics()
	wg.Wait()
}

func TestCheckShard(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc, co, cleanup := prepare(t, nil, nil, func(co *coordinator) { co.run() })
	hbStreams, opt := co.hbStreams, tc.opt
	defer cleanup()

	assert.Nil(t, tc.addShardStore(4, 4))
	assert.Nil(t, tc.addShardStore(3, 3))
	assert.Nil(t, tc.addShardStore(2, 2))
	assert.Nil(t, tc.addShardStore(1, 1))
	assert.Nil(t, tc.addLeaderShard(1, 2, 3))
	checkCOShard(t, tc, co, 1, false, 1)
	waitOperator(t, co, 1)
	testutil.CheckAddPeer(t, co.opController.GetOperator(1), operator.OpReplica, 1)
	checkCOShard(t, tc, co, 1, false, 0)

	r := tc.GetShard(1)
	p := metapb.Replica{ID: 1, StoreID: 1, Role: metapb.ReplicaRole_Learner}
	r = r.Clone(
		core.WithAddPeer(p),
		core.WithPendingPeers(append(r.GetPendingPeers(), p)),
	)
	tc.core.PutShard(r)
	checkCOShard(t, tc, co, 1, false, 0)
	co.stop()
	co.wg.Wait()

	tc = newTestCluster(opt)
	co = newCoordinator(ctx, tc.RaftCluster, hbStreams)
	co.run()

	assert.Nil(t, tc.addShardStore(4, 4))
	assert.Nil(t, tc.addShardStore(3, 3))
	assert.Nil(t, tc.addShardStore(2, 2))
	assert.Nil(t, tc.addShardStore(1, 1))
	tc.core.PutShard(r)
	checkCOShard(t, tc, co, 1, false, 0)
	r = r.Clone(core.WithPendingPeers(nil))
	tc.core.PutShard(r)
	checkCOShard(t, tc, co, 1, false, 1)
	waitOperator(t, co, 1)
	op := co.opController.GetOperator(1)
	assert.Equal(t, 1, op.Len())
	assert.Equal(t, uint64(1), op.Step(0).(operator.PromoteLearner).ToStore)
	checkCOShard(t, tc, co, 1, false, 0)
}

func TestCheckerIsBusy(t *testing.T) {
	tc, co, cleanup := prepare(t, func(cfg *config.ScheduleConfig) {
		cfg.ReplicaScheduleLimit = 0 // ensure replica checker is busy
		cfg.MergeScheduleLimit = 10
	}, nil, func(co *coordinator) { co.run() })
	defer cleanup()

	assert.Nil(t, tc.addShardStore(1, 0))
	num := 1 + typeutil.MaxUint64(tc.opt.GetReplicaScheduleLimit(), tc.opt.GetMergeScheduleLimit())
	var operatorKinds = []operator.OpKind{
		operator.OpReplica, operator.OpShard | operator.OpMerge,
	}
	for i, operatorKind := range operatorKinds {
		for j := uint64(0); j < num; j++ {
			shardID := j + uint64(i+1)*num
			assert.Nil(t, tc.addLeaderShard(shardID, 1))
			switch operatorKind {
			case operator.OpReplica:
				op := newTestOperator(shardID, tc.GetShard(shardID).Meta.GetEpoch(), operatorKind)
				assert.Equal(t, 1, co.opController.AddWaitingOperator(op))
			case operator.OpShard | operator.OpMerge:
				if shardID%2 == 1 {
					ops, err := operator.CreateMergeShardOperator("merge-shard", co.cluster, tc.GetShard(shardID), tc.GetShard(shardID-1), operator.OpMerge)
					assert.NoError(t, err)
					assert.Equal(t, len(ops), co.opController.AddWaitingOperator(ops...))
				}
			}

		}
	}
	checkCOShard(t, tc, co, num, true, 0)
}

func TestReplica(t *testing.T) {
	tc, co, cleanup := prepare(t, func(cfg *config.ScheduleConfig) {
		// Turn off balance.
		cfg.LeaderScheduleLimit = 0
		cfg.ShardScheduleLimit = 0
	}, nil, func(co *coordinator) { co.run() })
	defer cleanup()

	assert.Nil(t, tc.addShardStore(1, 1))
	assert.Nil(t, tc.addShardStore(2, 2))
	assert.Nil(t, tc.addShardStore(3, 3))
	assert.Nil(t, tc.addShardStore(4, 4))

	stream := mockhbstream.NewHeartbeatStream()

	// Add peer to store 1.
	assert.Nil(t, tc.addLeaderShard(1, 2, 3))
	shard := tc.GetShard(1)
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	shard = waitAddLearner(t, stream, shard, 1)
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	shard = waitPromoteLearner(t, stream, shard, 1)
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	waitNoResponse(t, stream)

	// Peer in store 3 is down, remove peer in store 3 and add peer to store 4.
	assert.Nil(t, tc.setStoreDown(3))
	p, _ := shard.GetStorePeer(3)
	downPeer := metapb.ReplicaStats{
		Replica:     p,
		DownSeconds: 24 * 60 * 60,
	}
	shard = shard.Clone(
		core.WithDownPeers(append(shard.GetDownPeers(), downPeer)),
	)
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	shard = waitAddLearner(t, stream, shard, 4)
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	shard = waitPromoteLearner(t, stream, shard, 4)
	shard = shard.Clone(core.WithDownPeers(nil))
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	waitNoResponse(t, stream)

	// Remove peer from store 4.
	assert.Nil(t, tc.addLeaderShard(2, 1, 2, 3, 4))
	shard = tc.GetShard(2)
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	shard = waitRemovePeer(t, stream, shard, 4)
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	waitNoResponse(t, stream)

	// Remove offline peer directly when it's pending.
	assert.Nil(t, tc.addLeaderShard(3, 1, 2, 3))
	assert.Nil(t, tc.setStoreOffline(3))
	shard = tc.GetShard(3)
	p, _ = shard.GetStorePeer(3)
	shard = shard.Clone(core.WithPendingPeers([]metapb.Replica{p}))
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	waitNoResponse(t, stream)
}

func TestPeerState(t *testing.T) {
	tc, co, cleanup := prepare(t, nil, nil, func(co *coordinator) { co.run() })
	defer cleanup()

	// Transfer peer from store 4 to store 1.
	assert.Nil(t, tc.addShardStore(1, 10))
	assert.Nil(t, tc.addShardStore(2, 10))
	assert.Nil(t, tc.addShardStore(3, 10))
	assert.Nil(t, tc.addShardStore(4, 40))
	assert.Nil(t, tc.addLeaderShard(1, 2, 3, 4))

	stream := mockhbstream.NewHeartbeatStream()

	// Wait for schedule.
	waitOperator(t, co, 1)
	testutil.CheckTransferPeer(t, co.opController.GetOperator(1), operator.OpKind(0), 4, 1)

	shard := tc.GetShard(1).Clone()

	// Add new peer.
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	shard = waitAddLearner(t, stream, shard, 1)
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	shard = waitPromoteLearner(t, stream, shard, 1)

	// If the new peer is pending, the operator will not finish.
	p, _ := shard.GetStorePeer(1)
	shard = shard.Clone(core.WithPendingPeers(append(shard.GetPendingPeers(), p)))
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	waitNoResponse(t, stream)
	assert.NotNil(t, co.opController.GetOperator(shard.Meta.GetID()))

	// The new peer is not pending now, the operator will finish.
	// And we will proceed to remove peer in store 4.
	shard = shard.Clone(core.WithPendingPeers(nil))
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	waitRemovePeer(t, stream, shard, 4)
	assert.Nil(t, tc.addLeaderShard(1, 1, 2, 3))
	shard = tc.GetShard(1).Clone()
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	waitNoResponse(t, stream)
}

func TestShouldRun(t *testing.T) {
	tc, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()

	assert.Nil(t, tc.addLeaderStore(1, 5))
	assert.Nil(t, tc.addLeaderStore(2, 2))
	assert.Nil(t, tc.addLeaderStore(3, 0))
	assert.Nil(t, tc.addLeaderStore(4, 0))
	assert.Nil(t, tc.LoadShard(1, 1, 2, 3))
	assert.Nil(t, tc.LoadShard(2, 1, 2, 3))
	assert.Nil(t, tc.LoadShard(3, 1, 2, 3))
	assert.Nil(t, tc.LoadShard(4, 1, 2, 3))
	assert.Nil(t, tc.LoadShard(5, 1, 2, 3))
	assert.Nil(t, tc.LoadShard(6, 2, 1, 4))
	assert.Nil(t, tc.LoadShard(7, 2, 1, 4))
	assert.False(t, co.shouldRun())
	assert.Equal(t, 2, tc.core.Shards.GetStoreShardCount("", 4))

	tbl := []struct {
		shardID   uint64
		shouldRun bool
	}{
		{1, false},
		{2, false},
		{3, false},
		{4, false},
		{5, false},
		// store4 needs collect two shard
		{6, false},
		{7, true},
	}

	for _, tb := range tbl {
		r := tc.GetShard(tb.shardID)
		nr := r.Clone(core.WithLeader(&r.Meta.GetReplicas()[0]))
		assert.Nil(t, tc.processShardHeartbeat(nr))
		assert.Equal(t, tb.shouldRun, co.shouldRun())
	}
	nr := metapb.Shard{ID: 6, Replicas: []metapb.Replica{}}
	newShard := core.NewCachedShard(nr, nil)
	assert.NotNil(t, tc.processShardHeartbeat(newShard))
	assert.Equal(t, 7, co.cluster.prepareChecker.sum)
}

func TestShouldRunWithNonLeaderShards(t *testing.T) {
	tc, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()

	assert.Nil(t, tc.addLeaderStore(1, 10))
	assert.Nil(t, tc.addLeaderStore(2, 0))
	assert.Nil(t, tc.addLeaderStore(3, 0))
	for i := 0; i < 10; i++ {
		assert.Nil(t, tc.LoadShard(uint64(i+1), 1, 2, 3))
	}
	assert.False(t, co.shouldRun())
	assert.Equal(t, 10, tc.core.Shards.GetStoreShardCount("", 1))

	tbl := []struct {
		shardID   uint64
		shouldRun bool
	}{
		{1, false},
		{2, false},
		{3, false},
		{4, false},
		{5, false},
		{6, false},
		{7, false},
		{8, true},
	}

	for _, tb := range tbl {
		r := tc.GetShard(tb.shardID)
		nr := r.Clone(core.WithLeader(&r.Meta.GetReplicas()[0]))
		assert.Nil(t, tc.processShardHeartbeat(nr))
		assert.Equal(t, tb.shouldRun, co.shouldRun())
	}
	nr := metapb.Shard{ID: 8, Replicas: []metapb.Replica{}}
	newShard := core.NewCachedShard(nr, nil)
	assert.NotNil(t, tc.processShardHeartbeat(newShard))
	assert.Equal(t, 8, co.cluster.prepareChecker.sum)

	// Now, after server is prepared, there exist some shards with no leader.
	assert.Equal(t, uint64(0), tc.GetShard(9).GetLeader().GetStoreID())
	assert.Equal(t, uint64(0), tc.GetShard(10).GetLeader().GetStoreID())
}

func TestAddScheduler(t *testing.T) {
	tc, co, cleanup := prepare(t, nil, nil, func(co *coordinator) { co.run() })
	defer cleanup()

	assert.Equal(t, 2, len(co.schedulers))
	assert.Nil(t, co.removeScheduler(schedulers.BalanceLeaderName))
	assert.Nil(t, co.removeScheduler(schedulers.BalanceShardName))
	assert.Empty(t, co.schedulers)

	stream := mockhbstream.NewHeartbeatStream()

	// Add stores 1,2,3
	assert.Nil(t, tc.addLeaderStore(1, 1))
	assert.Nil(t, tc.addLeaderStore(2, 1))
	assert.Nil(t, tc.addLeaderStore(3, 1))
	// Add shards 1 with leader in store 1 and followers in stores 2,3
	assert.Nil(t, tc.addLeaderShard(1, 1, 2, 3))
	// Add shards 2 with leader in store 2 and followers in stores 1,3
	assert.Nil(t, tc.addLeaderShard(2, 2, 1, 3))
	// Add shards 3 with leader in store 3 and followers in stores 1,2
	assert.Nil(t, tc.addLeaderShard(3, 3, 1, 2))

	oc := co.opController
	gls, err := schedule.CreateScheduler(schedulers.GrantLeaderType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(schedulers.GrantLeaderType, []string{"0"}))
	assert.Nil(t, err)
	assert.NotNil(t, co.addScheduler(gls))
	assert.NotNil(t, co.removeScheduler(gls.GetName()))

	gls, err = schedule.CreateScheduler(schedulers.GrantLeaderType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(schedulers.GrantLeaderType, []string{"1"}))
	assert.Nil(t, err)
	assert.Nil(t, co.addScheduler(gls))

	// Transfer all leaders to store 1.
	waitOperator(t, co, 2)
	shard2 := tc.GetShard(2)
	assert.Nil(t, dispatchHeartbeat(co, shard2, stream))
	shard2 = waitTransferLeader(t, stream, shard2, 1)
	assert.Nil(t, dispatchHeartbeat(co, shard2, stream))
	waitNoResponse(t, stream)

	waitOperator(t, co, 3)
	shard3 := tc.GetShard(3)
	assert.Nil(t, dispatchHeartbeat(co, shard3, stream))
	shard3 = waitTransferLeader(t, stream, shard3, 1)
	assert.Nil(t, dispatchHeartbeat(co, shard3, stream))
	waitNoResponse(t, stream)
}

func TestRemoveScheduler(t *testing.T) {
	tc, co, cleanup := prepare(t, func(cfg *config.ScheduleConfig) {
		cfg.ReplicaScheduleLimit = 0
	}, nil, func(co *coordinator) { co.run() })
	defer cleanup()

	// Add stores 1,2
	assert.Nil(t, tc.addLeaderStore(1, 1))
	assert.Nil(t, tc.addLeaderStore(2, 1))

	assert.Equal(t, 2, len(co.schedulers))
	oc := co.opController
	storage := tc.RaftCluster.storage

	gls1, err := schedule.CreateScheduler(schedulers.GrantLeaderType, oc, storage, schedule.ConfigSliceDecoder(schedulers.GrantLeaderType, []string{"1"}))
	assert.Nil(t, err)
	assert.Nil(t, co.addScheduler(gls1, "1"))
	assert.Equal(t, 3, len(co.schedulers))
	sches, _, err := storage.LoadAllScheduleConfig()
	assert.Nil(t, err)
	assert.Equal(t, 3, len(sches))

	// remove all schedulers
	assert.Nil(t, co.removeScheduler(schedulers.BalanceLeaderName))
	assert.Nil(t, co.removeScheduler(schedulers.BalanceShardName))
	assert.Nil(t, co.removeScheduler(schedulers.GrantLeaderName))
	// all removed
	sches, _, err = storage.LoadAllScheduleConfig()
	assert.Nil(t, err)
	assert.Empty(t, sches)
	assert.Empty(t, co.schedulers)
	assert.Nil(t, co.cluster.opt.Persist(co.cluster.storage))
	co.stop()
	co.wg.Wait()
}

func TestRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tc, co, cleanup := prepare(t, func(cfg *config.ScheduleConfig) {
		// Turn off balance, we test add replica only.
		cfg.LeaderScheduleLimit = 0
		cfg.ShardScheduleLimit = 0
	}, nil, func(co *coordinator) { co.run() })
	hbStreams := co.hbStreams
	defer cleanup()

	// Add 3 stores (1, 2, 3) and a shard with 1 replica on store 1.
	assert.Nil(t, tc.addShardStore(1, 1))
	assert.Nil(t, tc.addShardStore(2, 2))
	assert.Nil(t, tc.addShardStore(3, 3))
	assert.Nil(t, tc.addLeaderShard(1, 1))
	shard := tc.GetShard(1)
	tc.prepareChecker.collect(shard)

	// Add 1 replica on store 2.
	co = newCoordinator(ctx, tc.RaftCluster, hbStreams)
	co.run()
	stream := mockhbstream.NewHeartbeatStream()
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	shard = waitAddLearner(t, stream, shard, 2)
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	shard = waitPromoteLearner(t, stream, shard, 2)
	co.stop()
	co.wg.Wait()

	// Recreate coordinator then add another replica on store 3.
	co = newCoordinator(ctx, tc.RaftCluster, hbStreams)
	co.run()
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	shard = waitAddLearner(t, stream, shard, 3)
	assert.Nil(t, dispatchHeartbeat(co, shard, stream))
	waitPromoteLearner(t, stream, shard, 3)
}

func dispatchHeartbeat(co *coordinator, shard *core.CachedShard, stream opt.HeartbeatStream) error {
	co.hbStreams.BindStream(shard.GetLeader().GetStoreID(), stream)
	co.cluster.core.PutShard(shard.Clone())
	co.opController.Dispatch(shard, schedule.DispatchFromHeartBeat)
	return nil
}

func prepare(t *testing.T, setCfg func(*config.ScheduleConfig), setTc func(*testCluster), run func(*coordinator)) (*testCluster, *coordinator, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg, opt, err := newTestScheduleConfig()
	assert.NoError(t, err)
	if setCfg != nil {
		setCfg(cfg)
	}
	tc := newTestCluster(opt)
	hbStreams := hbstream.NewTestHeartbeatStreams(ctx, 0, tc, true /* need to run */, nil)
	if setTc != nil {
		setTc(tc)
	}
	co := newCoordinator(ctx, tc.RaftCluster, hbStreams)
	if run != nil {
		run(co)
	}
	return tc, co, func() {
		co.stop()
		co.wg.Wait()
		hbStreams.Close()
		cancel()
	}
}

func checkCOShard(t *testing.T, tc *testCluster, co *coordinator, shardID uint64, expectCheckerIsBusy bool, expectAddOperator int) {
	ops := co.checkers.CheckShard(tc.GetShard(shardID))
	if ops == nil {
		assert.Equal(t, 0, expectAddOperator)
	} else {
		assert.Equal(t, expectAddOperator, co.opController.AddWaitingOperator(ops...))
	}
}

func waitOperator(t *testing.T, co *coordinator, shardID uint64) {
	testutil.WaitUntil(t, func(t *testing.T) bool {
		return co.opController.GetOperator(shardID) != nil
	})
}

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

func TestOperatorCount(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	tc, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()
	oc := co.opController
	assert.Equal(t, uint64(0), oc.OperatorCount(operator.OpLeader))
	assert.Equal(t, uint64(0), oc.OperatorCount(operator.OpShard))

	assert.Nil(t, tc.addLeaderShard(1, 1))
	assert.Nil(t, tc.addLeaderShard(2, 2))
	{
		op1 := newTestOperator(1, tc.GetShard(1).Meta.GetEpoch(), operator.OpLeader)
		oc.AddWaitingOperator(op1)
		assert.Equal(t, oc.OperatorCount(operator.OpLeader), uint64(1)) // 1:leader
		op2 := newTestOperator(2, tc.GetShard(2).Meta.GetEpoch(), operator.OpLeader)
		oc.AddWaitingOperator(op2)
		assert.Equal(t, oc.OperatorCount(operator.OpLeader), uint64(2)) // 1:leader, 2:leader
		assert.True(t, oc.RemoveOperator(op1, ""))
		assert.Equal(t, oc.OperatorCount(operator.OpLeader), uint64(1)) // 2:leader
	}

	{
		op1 := newTestOperator(1, tc.GetShard(1).Meta.GetEpoch(), operator.OpShard)
		oc.AddWaitingOperator(op1)
		assert.Equal(t, oc.OperatorCount(operator.OpShard), uint64(1)) // 1:shard 2:leader
		assert.Equal(t, oc.OperatorCount(operator.OpLeader), uint64(1))
		op2 := newTestOperator(2, tc.GetShard(2).Meta.GetEpoch(), operator.OpShard)
		op2.SetPriorityLevel(core.HighPriority)
		oc.AddWaitingOperator(op2)
		assert.Equal(t, oc.OperatorCount(operator.OpShard), uint64(2)) // 1:shard 2:shard
		assert.Equal(t, oc.OperatorCount(operator.OpLeader), uint64(0))
	}
}

func TestStoreOverloaded(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	tc, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()
	oc := co.opController
	lb, err := schedule.CreateScheduler(schedulers.BalanceShardType, oc, tc.storage, schedule.ConfigSliceDecoder(schedulers.BalanceShardType, []string{"0", "", ""}))
	assert.NoError(t, err)
	opt := tc.GetOpts()
	assert.Nil(t, tc.addShardStore(4, 100))
	assert.Nil(t, tc.addShardStore(3, 100))
	assert.Nil(t, tc.addShardStore(2, 100))
	assert.Nil(t, tc.addShardStore(1, 10))
	assert.Nil(t, tc.addLeaderShard(1, 2, 3, 4))
	shard := tc.GetShard(1).Clone(core.SetApproximateSize(60))
	tc.core.PutShard(shard)
	start := time.Now()
	{
		op1 := lb.Schedule(tc)[0]
		assert.NotNil(t, op1)
		assert.True(t, oc.AddOperator(op1))
		assert.True(t, oc.RemoveOperator(op1, ""))
	}
	for {
		time.Sleep(time.Millisecond * 10)
		ops := lb.Schedule(tc)
		if time.Since(start) > time.Second {
			break
		}
		assert.Nil(t, ops)
	}

	// reset all stores' limit
	// scheduling one time needs 1/10 seconds
	opt.SetAllStoresLimit(limit.AddPeer, 600)
	opt.SetAllStoresLimit(limit.RemovePeer, 600)
	for i := 0; i < 10; i++ {
		op1 := lb.Schedule(tc)[0]
		assert.NotNil(t, op1)
		assert.True(t, oc.AddOperator(op1))
		assert.True(t, oc.RemoveOperator(op1, ""))
	}
	// sleep 1 seconds to make sure that the token is filled up
	time.Sleep(1 * time.Second)
	for i := 0; i < 100; i++ {
		assert.NotNil(t, lb.Schedule(tc))
	}
}

func TestStoreOverloadedWithReplace(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	tc, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()
	oc := co.opController
	lb, err := schedule.CreateScheduler(schedulers.BalanceShardType, oc, tc.storage, schedule.ConfigSliceDecoder(schedulers.BalanceShardType, []string{"0", "", ""}))
	assert.NoError(t, err)

	assert.Nil(t, tc.addShardStore(4, 100))
	assert.Nil(t, tc.addShardStore(3, 100))
	assert.Nil(t, tc.addShardStore(2, 100))
	assert.Nil(t, tc.addShardStore(1, 10))
	assert.Nil(t, tc.addLeaderShard(1, 2, 3, 4))
	assert.Nil(t, tc.addLeaderShard(2, 1, 3, 4))
	shard := tc.GetShard(1).Clone(core.SetApproximateSize(60))
	tc.core.PutShard(shard)
	shard = tc.GetShard(2).Clone(core.SetApproximateSize(60))
	tc.core.PutShard(shard)
	op1 := newTestOperator(1, tc.GetShard(1).Meta.GetEpoch(), operator.OpShard, operator.AddPeer{ToStore: 1, PeerID: 1})
	assert.True(t, oc.AddOperator(op1))
	op2 := newTestOperator(1, tc.GetShard(1).Meta.GetEpoch(), operator.OpShard, operator.AddPeer{ToStore: 2, PeerID: 2})
	op2.SetPriorityLevel(core.HighPriority)
	assert.True(t, oc.AddOperator(op2))
	op3 := newTestOperator(1, tc.GetShard(2).Meta.GetEpoch(), operator.OpShard, operator.AddPeer{ToStore: 1, PeerID: 3})
	assert.False(t, oc.AddOperator(op3))
	assert.Nil(t, lb.Schedule(tc))
	// sleep 2 seconds to make sure that token is filled up
	time.Sleep(2 * time.Second)
	assert.NotNil(t, lb.Schedule(tc))
}

// FIXME: remove after move into schedulers package
type mockLimitScheduler struct {
	schedule.Scheduler
	limit   uint64
	counter *schedule.OperatorController
	kind    operator.OpKind
}

func (s *mockLimitScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.counter.OperatorCount(s.kind) < s.limit
}

func TestController(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	tc, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()
	oc := co.opController

	assert.Nil(t, tc.addLeaderShard(1, 1))
	assert.Nil(t, tc.addLeaderShard(2, 2))
	scheduler, err := schedule.CreateScheduler(schedulers.BalanceLeaderType, oc, storage.NewTestStorage(), schedule.ConfigSliceDecoder(schedulers.BalanceLeaderType, []string{"0", "", ""}))
	assert.NoError(t, err)
	lb := &mockLimitScheduler{
		Scheduler: scheduler,
		counter:   oc,
		kind:      operator.OpLeader,
	}

	sc := newScheduleController(co, lb)

	for i := schedulers.MinScheduleInterval; sc.GetInterval() != schedulers.MaxScheduleInterval; i = sc.GetNextInterval(i) {
		assert.Equal(t, i, sc.GetInterval())
		assert.Nil(t, sc.Schedule())
	}
	// limit = 2
	lb.limit = 2
	// count = 0
	{
		assert.True(t, sc.AllowSchedule())
		op1 := newTestOperator(1, tc.GetShard(1).Meta.GetEpoch(), operator.OpLeader)
		assert.Equal(t, 1, oc.AddWaitingOperator(op1))
		// count = 1
		assert.True(t, sc.AllowSchedule())
		op2 := newTestOperator(2, tc.GetShard(2).Meta.GetEpoch(), operator.OpLeader)
		assert.Equal(t, 1, oc.AddWaitingOperator(op2))
		// count = 2
		assert.False(t, sc.AllowSchedule())
		assert.True(t, oc.RemoveOperator(op1, ""))
		// count = 1
		assert.True(t, sc.AllowSchedule())
	}

	op11 := newTestOperator(1, tc.GetShard(1).Meta.GetEpoch(), operator.OpLeader)
	// add a PriorityKind operator will remove old operator
	{
		op3 := newTestOperator(2, tc.GetShard(2).Meta.GetEpoch(), operator.OpHotShard)
		op3.SetPriorityLevel(core.HighPriority)
		assert.Equal(t, 1, oc.AddWaitingOperator(op11))
		assert.False(t, sc.AllowSchedule())
		assert.Equal(t, 1, oc.AddWaitingOperator(op3))
		assert.True(t, sc.AllowSchedule())
		assert.True(t, oc.RemoveOperator(op3, ""))
	}

	// add a admin operator will remove old operator
	{
		op2 := newTestOperator(2, tc.GetShard(2).Meta.GetEpoch(), operator.OpLeader)
		assert.Equal(t, 1, oc.AddWaitingOperator(op2))
		assert.False(t, sc.AllowSchedule())
		op4 := newTestOperator(2, tc.GetShard(2).Meta.GetEpoch(), operator.OpAdmin)
		op4.SetPriorityLevel(core.HighPriority)
		assert.Equal(t, 1, oc.AddWaitingOperator(op4))
		assert.True(t, sc.AllowSchedule())
		assert.True(t, oc.RemoveOperator(op4, ""))
	}

	// test wrong shard id.
	{
		op5 := newTestOperator(3, metapb.ShardEpoch{}, operator.OpHotShard)
		assert.Equal(t, 0, oc.AddWaitingOperator(op5))
	}

	// test wrong shard epoch.
	assert.True(t, oc.RemoveOperator(op11, ""))
	epoch := metapb.ShardEpoch{
		Generation: tc.GetShard(1).Meta.GetEpoch().Generation + 1,
		ConfigVer:  tc.GetShard(1).Meta.GetEpoch().ConfigVer,
	}
	{
		op6 := newTestOperator(1, epoch, operator.OpLeader)
		assert.Equal(t, 0, oc.AddWaitingOperator(op6))
	}
	epoch.Generation--
	{
		op6 := newTestOperator(1, epoch, operator.OpLeader)
		assert.Equal(t, 1, oc.AddWaitingOperator(op6))
		assert.True(t, oc.RemoveOperator(op6, ""))
	}
}

func TestInterval(t *testing.T) {
	s := &testOperatorController{}
	s.setup(t)
	defer s.tearDown()

	_, co, cleanup := prepare(t, nil, nil, nil)
	defer cleanup()

	lb, err := schedule.CreateScheduler(schedulers.BalanceLeaderType, co.opController, storage.NewTestStorage(), schedule.ConfigSliceDecoder(schedulers.BalanceLeaderType, []string{"0", "", ""}))
	assert.NoError(t, err)
	sc := newScheduleController(co, lb)

	// If no operator for x seconds, the next check should be in x/2 seconds.
	idleSeconds := []int{5, 10, 20, 30, 60}
	for _, n := range idleSeconds {
		sc.nextInterval = schedulers.MinScheduleInterval
		for totalSleep := time.Duration(0); totalSleep <= time.Second*time.Duration(n); totalSleep += sc.GetInterval() {
			assert.Nil(t, sc.Schedule())
		}
		assert.True(t, sc.GetInterval() < time.Second*time.Duration(n/2))
	}
}

func waitAddLearner(t *testing.T, stream mockhbstream.HeartbeatStream, shard *core.CachedShard, storeID uint64) *core.CachedShard {
	var res *rpcpb.ShardHeartbeatRsp
	testutil.WaitUntil(t, func(t *testing.T) bool {
		if res = stream.Recv(); res != nil {
			return res.GetShardID() == shard.Meta.GetID() &&
				res.GetConfigChange().GetChangeType() == metapb.ConfigChangeType_AddLearnerNode &&
				res.GetConfigChange().GetReplica().StoreID == storeID
		}
		return false
	})
	return shard.Clone(
		core.WithAddPeer(res.GetConfigChange().GetReplica()),
		core.WithIncConfVer(),
	)
}

func waitPromoteLearner(t *testing.T, stream mockhbstream.HeartbeatStream, shard *core.CachedShard, storeID uint64) *core.CachedShard {
	var res *rpcpb.ShardHeartbeatRsp
	testutil.WaitUntil(t, func(t *testing.T) bool {
		if res = stream.Recv(); res != nil {
			return res.GetShardID() == shard.Meta.GetID() &&
				res.GetConfigChange().GetChangeType() == metapb.ConfigChangeType_AddNode &&
				res.GetConfigChange().GetReplica().StoreID == storeID
		}
		return false
	})
	// Remove learner than add voter.
	return shard.Clone(
		core.WithRemoveStorePeer(storeID),
		core.WithAddPeer(res.GetConfigChange().GetReplica()),
	)
}

func waitRemovePeer(t *testing.T, stream mockhbstream.HeartbeatStream, shard *core.CachedShard, storeID uint64) *core.CachedShard {
	var res *rpcpb.ShardHeartbeatRsp
	testutil.WaitUntil(t, func(t *testing.T) bool {
		if res = stream.Recv(); res != nil {
			return res.GetShardID() == shard.Meta.GetID() &&
				res.GetConfigChange().GetChangeType() == metapb.ConfigChangeType_RemoveNode &&
				res.GetConfigChange().GetReplica().StoreID == storeID
		}
		return false
	})
	return shard.Clone(
		core.WithRemoveStorePeer(storeID),
		core.WithIncConfVer(),
	)
}

func waitTransferLeader(t *testing.T, stream mockhbstream.HeartbeatStream, shard *core.CachedShard, storeID uint64) *core.CachedShard {
	var res *rpcpb.ShardHeartbeatRsp
	testutil.WaitUntil(t, func(t *testing.T) bool {
		if res = stream.Recv(); res != nil {
			return res.GetShardID() == shard.Meta.GetID() && res.GetTransferLeader().GetReplica().StoreID == storeID
		}
		return false
	})

	p := res.GetTransferLeader().GetReplica()
	return shard.Clone(
		core.WithLeader(&p),
	)
}

func waitNoResponse(t *testing.T, stream mockhbstream.HeartbeatStream) {
	testutil.WaitUntil(t, func(t *testing.T) bool {
		res := stream.Recv()
		return res == nil
	})
}
