// Copyright 2021 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless assertd by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package raftstore

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/transport"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

var (
	testWaitTimeout = time.Minute
)

func TestSplitWithSingleCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()
	c := NewSingleTestClusterStore(t,
		DiskTestCluster,
		OldTestCluster,
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(4)
			cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(2)
		}))

	c.Start()
	defer c.Stop()

	sid := prepareSplit(t, c, []int{0}, []int{2})
	c.Restart()
	checkSplitWithStore(t, c, 0, sid, 2, true)
	checkSplitWithProphet(t, c, sid, 1)
}

func TestSplitWithMultiNodesCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()
	c := NewTestClusterStore(t,
		DiskTestCluster,
		OldTestCluster,
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(4)
			cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(2)
		}))

	c.Start()
	defer c.Stop()

	sid := prepareSplit(t, c, []int{0, 1, 2}, []int{2, 2, 2})
	c.Restart()
	c.EveryStore(func(index int, store Store) {
		checkSplitWithStore(t, c, index, sid, 2, true)
	})
	checkSplitWithProphet(t, c, sid, 3)
}

func TestSplitWithCase1(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	// A -> B+C
	// A has 3 replcias A1, A2, A3
	// restart after split request applied
	// check re-run destroy shard task

	defer leaktest.AfterTest(t)()
	c := NewTestClusterStore(t,
		DiskTestCluster,
		OldTestCluster,
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(4)
			cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(2)
		}))

	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	sid := c.GetShardByIndex(0, 0).ID
	c.EveryStore(func(i int, s Store) {
		pr := s.(*store).getReplica(sid, false)
		pr.destoryTaskFactory = newTestDestroyReplicaTaskFactory(true) // skip destory task
	})

	prepareSplit(t, c, nil, []int{3, 3, 3})

	// sync
	c.EveryStore(func(i int, s Store) {
		assert.NoError(t, s.(*store).DataStorageByGroup(0).Sync(nil))
	})

	// restart
	c.Restart()
	c.WaitRemovedByShardIDAt(sid, []int{0, 1, 2}, testWaitTimeout)
	c.EveryStore(func(index int, store Store) {
		checkSplitWithStore(t, c, index, sid, 2, true)
	})
	checkSplitWithProphet(t, c, sid, 3)
}

func TestSplitWithCase2(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	// A -> B+C
	// A has 3 replcias A1, A2, A3
	// A3 cannot send and received raft message
	// pd will remove A3
	// split completed
	// A3 back and removed by pd check, cannot split

	defer leaktest.AfterTest(t)()

	skipStore := uint64(0)
	filter := func(msg meta.RaftMessage) bool {
		return msg.To.ContainerID == atomic.LoadUint64(&skipStore) ||
			msg.From.ContainerID == atomic.LoadUint64(&skipStore)
	}

	var c TestRaftCluster
	c = NewTestClusterStore(t,
		DiskTestCluster,
		OldTestCluster,
		WithTestClusterNodeCount(3),
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(4)
			cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(2)
			cfg.Prophet.Schedule.EnableRemoveDownReplica = true
			if node == 2 {
				cfg.Replication.StoreHeartbeatDuration = typeutil.NewDuration(time.Second * 2)
			}
			cfg.Replication.MaxPeerDownTime = typeutil.NewDuration(time.Second)
			cfg.Prophet.Schedule.MaxContainerDownTime = typeutil.NewDuration(time.Second)
			cfg.Customize.CustomTransportFactory = func() transport.Trans {
				return newTestTransport(c, filter)
			}
			cfg.Replication.ShardStateCheckDuration.Duration = time.Second
		}))

	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	// now shard 1 has 3 replicas, skip send raft msg to the last store
	atomic.StoreUint64(&skipStore, c.GetStore(2).Meta().ID)

	// only check node0, node1's shard count.
	sid := prepareSplit(t, c, []int{0, 1}, []int{2, 2})
	c.EveryStore(func(index int, store Store) {
		v := 2
		if index == 2 {
			v = 1
		}
		checkSplitWithStore(t, c, index, sid, v, index != 2)
	})
	checkSplitWithProphet(t, c, sid, 2)

	// node3 back and removed by state check
	atomic.StoreUint64(&skipStore, 0)
	c.WaitRemovedByShardIDAt(sid, []int{2}, testWaitTimeout)
}

func TestSplitWithCase3(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	// A -> B+C
	// A has 3 replcias A1, A2, A3
	// A3 cannot send and received raft message
	// pd will remove A3
	// split completed
	// restart A1, A2, A3
	// A3 back and removed by pd check, cannot split

	defer leaktest.AfterTest(t)()

	skipStore := uint64(0)
	filter := func(msg meta.RaftMessage) bool {
		return msg.To.ContainerID == atomic.LoadUint64(&skipStore) ||
			msg.From.ContainerID == atomic.LoadUint64(&skipStore)
	}

	var c TestRaftCluster
	c = NewTestClusterStore(t,
		DiskTestCluster,
		OldTestCluster,
		WithTestClusterNodeCount(3),
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(4)
			cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(2)
			cfg.Prophet.Schedule.EnableRemoveDownReplica = true
			if node == 2 {
				cfg.Replication.StoreHeartbeatDuration = typeutil.NewDuration(time.Second * 2)
			}
			cfg.Replication.MaxPeerDownTime = typeutil.NewDuration(time.Second)
			cfg.Prophet.Schedule.MaxContainerDownTime = typeutil.NewDuration(time.Second)
			cfg.Customize.CustomTransportFactory = func() transport.Trans {
				return newTestTransport(c, filter)
			}
			cfg.Replication.ShardStateCheckDuration.Duration = time.Second
		}))

	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	// now shard 1 has 3 replicas, skip send raft msg to the last store
	atomic.StoreUint64(&skipStore, c.GetStore(2).Meta().ID)

	sid := prepareSplit(t, c, []int{0, 1}, []int{2, 2, 1})
	c.EveryStore(func(index int, store Store) {
		v := 2
		if index == 2 {
			v = 1
		}
		checkSplitWithStore(t, c, index, sid, v, index != 2)
	})
	checkSplitWithProphet(t, c, sid, 2)

	// restart
	c.RestartWithFunc(func() {
		c.EveryStore(func(i int, s Store) {
			s.GetConfig().Customize.CustomTransportFactory = nil
		})
	})
	assert.Nil(t, c.GetStore(2).(*store).getReplica(sid, false))
}

func prepareSplit(t *testing.T, c TestRaftCluster, removedNodes, counts []int) uint64 {
	c.WaitShardByCountPerNode(1, testWaitTimeout)

	kv := c.CreateTestKVClient(0)
	defer kv.Close()

	sid := c.GetShardByIndex(0, 0).ID
	kv.Set("k1", "v1", testWaitTimeout)
	kv.Set("k2", "v2", testWaitTimeout)

	c.WaitRemovedByShardIDAt(sid, removedNodes, testWaitTimeout)
	c.WaitShardByCounts(counts, testWaitTimeout)
	return sid
}

func checkSplitWithStore(t *testing.T, c TestRaftCluster, index int, parentShardID uint64, shardsCount int, checkRange bool) {
	c.WaitShardByCountOnNode(index, shardsCount, testWaitTimeout)

	kv := c.CreateTestKVClient(index)
	defer kv.Close()

	if checkRange {
		// check new shards range
		assert.Empty(t, c.GetShardByIndex(index, 0).Start, "index %d", index)
		assert.Equal(t, []byte("k2"), c.GetShardByIndex(index, 0).End, "index %d", index)
		assert.Empty(t, c.GetShardByIndex(index, 1).End, "index %d", index)
		assert.Equal(t, []byte("k2"), c.GetShardByIndex(index, 1).Start, "index %d", index)
	}

	// read
	v, err := kv.Get("k1", testWaitTimeout)
	assert.NoError(t, err, "index %d", index)
	assert.Equal(t, "v1", string(v), "index %d", index)

	v, err = kv.Get("k2", testWaitTimeout)
	assert.NoError(t, err, "index %d", index)
	assert.Equal(t, "v2", string(v), "index %d", index)

	// check router
	shard, _ := c.GetStore(index).GetRouter().SelectShard(0, []byte("k1"))
	assert.NotEqual(t, parentShardID, shard.ID)
	shard, _ = c.GetStore(index).GetRouter().SelectShard(0, []byte("k2"))
	assert.NotEqual(t, parentShardID, shard.ID)

}

func checkSplitWithProphet(t *testing.T, c TestRaftCluster, sid uint64, replicaCount int) {
	pd := c.GetStore(0).Prophet()
	if pd.GetMember().IsLeader() {
		v, err := pd.GetStorage().GetResource(sid)
		assert.NoError(t, err)
		assert.Equal(t, metapb.ResourceState_Destroyed, v.State())
		assert.Equal(t, replicaCount, len(v.Peers()))

		res := pd.GetBasicCluster().Resources.GetResource(sid)
		assert.Nil(t, res)

		res = pd.GetBasicCluster().SearchResource(0, []byte("k1"))
		assert.NotNil(t, res)
		assert.NotEqual(t, sid, res.Meta.ID())

		res = pd.GetBasicCluster().SearchResource(0, []byte("k3"))
		assert.NotNil(t, res)
		assert.NotEqual(t, sid, res.Meta.ID())
	}
}
