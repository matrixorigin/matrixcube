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
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3"
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
			cfg.Replication.ShardStateCheckDuration.Duration = time.Second
			cfg.Prophet.Schedule.EnableRemoveDownReplica = true
			cfg.Replication.MaxPeerDownTime = typeutil.NewDuration(time.Second)

		}))

	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	sid := c.GetShardByIndex(0, 0).ID
	c.EveryStore(func(i int, s Store) {
		pr := s.(*store).getReplica(sid, false)
		pr.destroyTaskFactory = newTestDestroyReplicaTaskFactory(true) // skip destroy task
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
	c := NewTestClusterStore(t,
		DiskTestCluster,
		OldTestCluster,
		WithTestClusterNodeCount(3),
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(4)
			cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(2)
			cfg.Prophet.Schedule.EnableRemoveDownReplica = true
			cfg.Replication.MaxPeerDownTime = typeutil.NewDuration(time.Second)
			cfg.Replication.ShardStateCheckDuration.Duration = time.Second
		}))

	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.WaitAllReplicasChangeToVoter(c.GetShardByIndex(0, 0).ID, testWaitTimeout)

	// start network partition between (node0, node1) and (node2)
	c.StartNetworkPartition([][]int{{0, 1}, {2}})

	// only check node0, node1's shard count.
	sid := prepareSplit(t, c, []int{0, 1, 2}, []int{2, 2, 0})
	c.EveryStore(func(index int, store Store) {
		v := 2
		if index == 2 {
			v = 0
		}
		checkSplitWithStore(t, c, index, sid, v, index != 2)
	})
	checkSplitWithProphet(t, c, sid, 2)

	// node2 back and removed by state check
	c.StopNetworkPartition()
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
	c := NewTestClusterStore(t,
		DiskTestCluster,
		OldTestCluster,
		WithTestClusterNodeCount(3),
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(4)
			cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(2)
			cfg.Prophet.Schedule.EnableRemoveDownReplica = true
			cfg.Replication.MaxPeerDownTime = typeutil.NewDuration(time.Second)
			cfg.Replication.ShardStateCheckDuration.Duration = time.Second
		}))

	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.WaitAllReplicasChangeToVoter(c.GetShardByIndex(0, 0).ID, testWaitTimeout)

	// start network partition between (node0, node1) and (node2)
	c.StartNetworkPartition([][]int{{0, 1}, {2}})

	sid := prepareSplit(t, c, []int{0, 1, 2}, []int{2, 2, 0})
	c.EveryStore(func(index int, store Store) {
		v := 2
		if index == 2 {
			v = 0
		}
		checkSplitWithStore(t, c, index, sid, v, index != 2)
	})
	checkSplitWithProphet(t, c, sid, 2)

	// restart
	c.Restart()
	c.StopNetworkPartition()
	assert.Nil(t, c.GetStore(2).(*store).getReplica(sid, false))
}

func TestSplitWithCase4(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	// A -> B+C
	// A has 3 replcias A1, A2, A3
	// network partition A3
	// A1, A2 split
	// restart
	// A1, A2 is destroying, A3 is running(A3 will not create and start, because A is destroying in pd)
	// A1, A2's destroy shard task is blocking at A3's committed index is too small.
	// A3 will removed by pd's conf change remove node
	// A1, A2 completed destroy task

	defer leaktest.AfterTest(t)()
	c := NewTestClusterStore(t,
		DiskTestCluster,
		OldTestCluster,
		WithTestClusterNodeCount(3),
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(4)
			cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(2)
			cfg.Replication.ShardStateCheckDuration.Duration = time.Second
		}))
	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.WaitAllReplicasChangeToVoter(c.GetShardByIndex(0, 0).ID, testWaitTimeout)

	sid := c.GetShardByIndex(0, 0).ID

	// start network partition between (node0, node1) and (node2)
	c.StartNetworkPartition([][]int{{0, 1}, {2}})

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.WaitLeadersByCount(1, testWaitTimeout)
	kv := c.CreateTestKVClient(0)
	defer kv.Close()

	assert.NoError(t, kv.Set("k1", "v1", testWaitTimeout))
	assert.NoError(t, kv.Set("k2", "v2", testWaitTimeout))
	c.WaitShardByCounts([]int{3, 3, 1}, testWaitTimeout)

	c.WaitShardStateChangedTo(sid, metapb.ShardState_Destroying, testWaitTimeout)

	c.RestartWithFunc(func() {
		c.StopNetworkPartition()
		c.EveryStore(func(i int, s Store) {
			s.(*store).cfg.Prophet.Schedule.EnableRemoveDownReplica = true
			s.(*store).cfg.Replication.MaxPeerDownTime = typeutil.NewDuration(time.Second)
		})
	})
	c.WaitRemovedByShardIDAt(sid, []int{0, 1, 2}, testWaitTimeout)
	c.WaitShardByCount(2, testWaitTimeout)
}

func TestSplitWithApplySnapshotAndStartDestroyByStateCheck(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()
	c := NewTestClusterStore(t,
		DiskTestCluster,
		OldTestCluster,
		WithTestClusterNodeCount(3),
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(4)
			cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(2)
			cfg.Replication.ShardStateCheckDuration.Duration = time.Second
		}))
	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.WaitAllReplicasChangeToVoter(c.GetShardByIndex(0, 0).ID, testWaitTimeout)

	sid := c.GetShardByIndex(0, 0).ID

	// start network partition between (node0, node1) and (node2)
	c.StartNetworkPartition([][]int{{0, 1}, {2}})

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.WaitLeadersByCount(1, testWaitTimeout)
	kv := c.CreateTestKVClient(0)
	defer kv.Close()

	assert.NoError(t, kv.Set("k1", "v1", testWaitTimeout))

	// force log compact
	c.EveryStore(func(i int, s Store) {
		pr := s.(*store).getReplica(sid, true)
		if pr != nil {
			idx, _ := pr.sm.getAppliedIndexTerm()
			pr.sm.dataStorage.Sync([]uint64{sid})
			pr.addAdminRequest(rpcpb.AdminCompactLog, &rpcpb.CompactLogRequest{
				CompactIndex: idx,
			})

			hasLog := func(index uint64) bool {
				lr := pr.lr
				_, err := lr.Entries(index, index+1, math.MaxUint64)
				if err == nil {
					return true
				}
				if err == raft.ErrCompacted {
					return false
				}
				panic(err)
			}

			// wait for compaction to complete
			for i := 0; i < 10; i++ {
				if hasLog(idx) {
					time.Sleep(time.Second)
				} else {
					break
				}
				if i == 9 {
					t.Fatalf("failed to remove log entries from logdb")
				}
			}
		}
	})

	// continue write data to ensure split
	assert.NoError(t, kv.Set("k2", "v2", testWaitTimeout))

	// resume node2
	c.StopNetworkPartition()

	// wait shard sid removed at all nodes
	c.WaitRemovedByShardIDAt(sid, []int{0, 1, 2}, testWaitTimeout)

	c.EveryStore(func(index int, store Store) {
		checkSplitWithStore(t, c, index, sid, 2, true)
	})
	checkSplitWithProphet(t, c, sid, 3)
}

func prepareSplit(t *testing.T, c TestRaftCluster, removedNodes, counts []int) uint64 {
	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.WaitLeadersByCount(1, testWaitTimeout)
	sid := c.GetShardByIndex(0, 0).ID

	kv := c.CreateTestKVClient(0)
	defer kv.Close()

	assert.NoError(t, kv.Set("k1", "v1", testWaitTimeout))
	assert.NoError(t, kv.Set("k2", "v2", testWaitTimeout))

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
		s := c.GetShardByIndex(index, 0)
		assert.Empty(t, s.Start, "index %d", index)
		assert.Equal(t, []byte("k2"), s.End, "index %d, %+v", index, s)

		s = c.GetShardByIndex(index, 1)
		assert.Empty(t, s.End, "index %d", index)
		assert.Equal(t, []byte("k2"), s.Start, "index %d, %+v", index, s)
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
		v, err := pd.GetStorage().GetShard(sid)
		assert.NoError(t, err)
		if v != nil {
			assert.Equal(t, metapb.ShardState_Destroyed, v.GetState())
			assert.True(t, len(v.GetReplicas()) <= replicaCount) // maybe some replica removed by conf change
		}

		bc := pd.GetBasicCluster()
		bc.RLock()
		res := bc.Shards.GetShard(sid)
		assert.Nil(t, res)

		res = bc.Shards.SearchShard(0, []byte("k1"))
		assert.NotNil(t, res)
		assert.NotEqual(t, sid, res.Meta.GetID())

		res = bc.Shards.SearchShard(0, []byte("k3"))
		assert.NotNil(t, res)
		assert.NotEqual(t, sid, res.Meta.GetID())
		bc.RUnlock()
	}
}
