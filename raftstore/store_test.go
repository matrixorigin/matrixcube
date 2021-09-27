// Copyright 2020 MatrixOrigin.
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

package raftstore

import (
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

func TestSingleTestClusterStartAndStop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewSingleTestClusterStore(t,
		DiskTestCluster)
	defer c.Stop()

	c.Start()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.CheckShardCount(1)
}

func TestClusterStartAndStop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewTestClusterStore(t)
	defer c.Stop()

	c.Start()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.CheckShardCount(1)
}

func TestClusterStartWithMoreNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewTestClusterStore(t, WithTestClusterNodeCount(5))
	defer c.Stop()

	c.Start()

	c.WaitLeadersByCount(1, testWaitTimeout)
}

func TestClusterStartConcurrent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewTestClusterStore(t, DiskTestCluster)
	defer c.Stop()

	c.StartWithConcurrent(true)

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.CheckShardCount(1)

	c.Restart()
	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.CheckShardCount(1)
}

func TestAdjustRaftTickerInterval(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewSingleTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
		cfg.Raft.TickInterval.Duration = time.Millisecond
	}))
	defer c.Stop()

	c.Start()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.CheckShardCount(1)

	c.EveryStore(func(i int, store Store) {
		assert.False(t, store.GetConfig().Raft.TickInterval.Duration == time.Millisecond)
	})
}

func TestIssue123(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewSingleTestClusterStore(t,
		WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
			cfg.Customize.CustomInitShardsFactory = func() []Shard { return []Shard{{Start: []byte("a"), End: []byte("b")}} }
		}))
	defer c.Stop()

	c.Start()
	c.WaitShardByCountPerNode(1, testWaitTimeout)

	p, err := c.GetStore(0).CreateResourcePool(metapb.ResourcePool{
		RangePrefix: []byte("b"),
		Capacity:    20,
	})
	assert.NoError(t, err)
	assert.NotNil(t, p)

	c.WaitShardByCountPerNode(21, testWaitTimeout)

	kv := c.CreateTestKVClient(0)
	defer kv.Close()

	for i := 0; i < 20; i++ {
		s, err := p.Alloc(0, []byte(fmt.Sprintf("%d", i)))
		assert.NoError(t, err)
		assert.NoError(t, kv.Set(string(c.GetShardByID(0, s.ShardID).Start), "OK", time.Second))
	}
}

func TestAddShardWithMultiGroups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewSingleTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
		cfg.ShardGroups = 2
		cfg.Prophet.Replication.Groups = []uint64{0, 1}
		cfg.Customize.CustomInitShardsFactory = func() []Shard {
			return []Shard{{Start: []byte("a"), End: []byte("b")}, {Group: 1, Start: []byte("a"), End: []byte("b")}}
		}
	}))
	defer c.Stop()

	c.Start()
	c.WaitShardByCountPerNode(2, testWaitTimeout)

	err := c.GetProphet().GetClient().AsyncAddResources(NewResourceAdapterWithShard(Shard{Start: []byte("b"), End: []byte("c"), Unique: "abc", Group: 1}))
	assert.NoError(t, err)
	c.WaitShardByCountPerNode(3, testWaitTimeout)
}

func TestAppliedRules(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
		cfg.Customize.CustomInitShardsFactory = func() []Shard { return []Shard{{Start: []byte("a"), End: []byte("b")}} }
	}))
	defer c.Stop()

	c.Start()
	c.WaitShardByCountPerNode(1, testWaitTimeout)

	assert.NoError(t, c.GetProphet().GetClient().PutPlacementRule(rpcpb.PlacementRule{
		GroupID: "g1",
		ID:      "id1",
		Count:   3,
		LabelConstraints: []rpcpb.LabelConstraint{
			{
				Key:    "c",
				Op:     rpcpb.In,
				Values: []string{"0", "1"},
			},
		},
	}))
	res := NewResourceAdapterWithShard(Shard{Start: []byte("b"), End: []byte("c"), Unique: "abc", RuleGroups: []string{"g1"}})
	err := c.GetProphet().GetClient().AsyncAddResourcesWithLeastPeers([]metadata.Resource{res}, []int{2})
	assert.NoError(t, err)

	c.WaitShardByCounts([]int{2, 2, 1}, testWaitTimeout)
}

// TODO(TODO): resume
// func TestSplit(t *testing.T) {
// 	defer leaktest.AfterTest(t)()
// 	c := NewSingleTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
// 		cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(20)
// 		cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(10)
// 	}))
// 	defer c.Stop()

// 	c.Start()
// 	c.WaitShardByCountPerNode(1, testWaitTimeout)

// 	c.Set(0, keys.EncodeDataKey(0, []byte("key1")), []byte("value11"))
// 	c.Set(0, keys.EncodeDataKey(0, []byte("key2")), []byte("value22"))
// 	c.Set(0, keys.EncodeDataKey(0, []byte("key3")), []byte("value33"))

// 	c.WaitShardByCountPerNode(3, testWaitTimeout)
// 	c.WaitShardSplitByCount(c.GetShardByIndex(0, 0).ID, 1, testWaitTimeout)
// 	c.CheckShardRange(0, nil, []byte("key2"))
// 	c.CheckShardRange(1, []byte("key2"), []byte("key3"))
// 	c.CheckShardRange(2, []byte("key3"), nil)
// }

// TODO(fagongzi): resume
// func TestCustomSplit(t *testing.T) {
// 	defer leaktest.AfterTest(t)()
// 	target := keys.EncodeDataKey(0, []byte("key2"))
// 	c := NewSingleTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
// 		cfg.Customize.CustomSplitCheckFuncFactory = func(group uint64) func(shard Shard) (uint64, uint64, [][]byte, error) {
// 			return func(shard Shard) (uint64, uint64, [][]byte, error) {
// 				store := cfg.Storage.DataStorageFactory(shard.Group).(storage.KVStorage)
// 				endGroup := shard.Group
// 				if len(shard.End) == 0 {
// 					endGroup++
// 				}
// 				size := uint64(0)
// 				totalKeys := uint64(0)
// 				hasTarget := false
// 				store.Scan(keys.EncodeDataKey(shard.Group, shard.Start), keys.EncodeDataKey(endGroup, shard.End), func(key, value []byte) (bool, error) {
// 					size += uint64(len(key) + len(value))
// 					totalKeys++
// 					if bytes.Equal(key, target) {
// 						hasTarget = true
// 					}
// 					return true, nil
// 				}, false)

// 				if len(shard.End) == 0 && len(shard.Start) == 0 && hasTarget {
// 					return size, totalKeys, [][]byte{target}, nil
// 				}

// 				return size, totalKeys, nil, nil
// 			}
// 		}
// 	}))
// 	defer c.Stop()

// 	c.Start()
// 	c.WaitShardByCountPerNode(1, testWaitTimeout)

// 	c.Set(0, keys.EncodeDataKey(0, []byte("key1")), []byte("value11"))
// 	c.Set(0, keys.EncodeDataKey(0, []byte("key2")), []byte("value22"))
// 	c.Set(0, keys.EncodeDataKey(0, []byte("key3")), []byte("value33"))

// 	c.WaitShardByCountPerNode(2, testWaitTimeout)
// 	c.WaitShardSplitByCount(c.GetShardByIndex(0, 0).ID, 1, testWaitTimeout)
// 	c.CheckShardRange(0, nil, []byte("key2"))
// 	c.CheckShardRange(1, []byte("key2"), nil)
// }

func TestSpeedupAddShard(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
		cfg.Raft.TickInterval = typeutil.NewDuration(time.Second * 2)
		cfg.Customize.CustomInitShardsFactory = func() []Shard { return []Shard{{Start: []byte("a"), End: []byte("b")}} }
	}))
	defer c.Stop()

	c.Start()
	c.WaitShardByCountPerNode(1, testWaitTimeout)

	err := c.GetProphet().GetClient().AsyncAddResources(NewResourceAdapterWithShard(Shard{Start: []byte("b"), End: []byte("c"), Unique: "abc"}))
	assert.NoError(t, err)

	c.WaitShardByCountPerNode(2, testWaitTimeout)
	c.CheckShardCount(2)

	id := c.GetShardByIndex(0, 1).ID
	c.WaitShardStateChangedTo(id, metapb.ResourceState_Running, testWaitTimeout)
}

func TestIssue166(t *testing.T) {
	c := NewSingleTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
		cfg.Test.SaveDynamicallyShardInitStateWait = time.Second
		cfg.Customize.CustomInitShardsFactory = func() []Shard { return []Shard{{Start: []byte("a"), End: []byte("b")}} }
	}))
	defer c.Stop()

	c.Start()
	c.WaitLeadersByCount(1, testWaitTimeout)

	_, err := c.GetStore(0).CreateResourcePool(metapb.ResourcePool{
		Capacity:    1,
		Group:       0,
		RangePrefix: []byte("b"),
	})
	assert.NoError(t, err)
	c.WaitLeadersByCount(2, testWaitTimeout)
	// make sure slow point
	time.Sleep(time.Second)

	// TODO(fagongzi): check commit
	// v, err := c.GetStore(0).MetadataStorage().Get(keys.GetRaftLocalStateKey(c.GetShardByIndex(0, 1).ID))
	// assert.NoError(t, err)
	// state := &bhraftpb.RaftLocalState{}
	// protoc.MustUnmarshal(state, v)
	// assert.Equal(t, uint64(6), state.HardState.Commit)
}

func TestInitialMember(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
		cfg.Customize.CustomInitShardsFactory = func() []Shard { return []Shard{{Start: []byte("a"), End: []byte("b")}} }
	}))
	defer c.Stop()
	c.Start()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	initialMembers := 0
	for _, p := range c.GetShardByIndex(0, 0).Replicas {
		if p.InitialMember {
			initialMembers++
		}
	}
	assert.Equal(t, 1, initialMembers)

	p, err := c.GetStore(0).CreateResourcePool(metapb.ResourcePool{Group: 0, Capacity: 1, RangePrefix: []byte("b")})
	assert.NoError(t, err)
	assert.NotNil(t, p)

	c.WaitShardByCountPerNode(2, testWaitTimeout)
	for i := 0; i < 3; i++ {
		initialMembers = 0
		for _, p := range c.GetShardByIndex(i, 1).Replicas {
			if p.InitialMember {
				initialMembers++
			}
		}
		assert.Equal(t, 3, initialMembers)
	}
}

func TestReadAndWriteAndRestart(t *testing.T) {
	c := NewSingleTestClusterStore(t,
		WithTestClusterLogLevel(zapcore.DebugLevel),
		DiskTestCluster)
	defer c.Stop()

	c.Start()
	c.WaitLeadersByCount(1, testWaitTimeout)

	kv := c.CreateTestKVClient(0)
	defer kv.Close()

	for i := 0; i < 100; i++ {
		assert.NoError(t, kv.Set(fmt.Sprintf("k-%d", i), fmt.Sprintf("v-%d", i), testWaitTimeout))
	}

	c.Restart()
	c.WaitLeadersByCount(1, testWaitTimeout)

	kv2 := c.CreateTestKVClient(0)
	defer kv2.Close()

	for i := 0; i < 100; i++ {
		v, err := kv2.Get(fmt.Sprintf("k-%d", i), testWaitTimeout)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("v-%d", i), v)
	}
}

func TestReadAndWriteAndRestartWithNodes(t *testing.T) {
	c := NewTestClusterStore(t,
		DiskTestCluster)
	defer c.Stop()

	c.Start()
	c.WaitLeadersByCount(1, testWaitTimeout)

	kv := c.CreateTestKVClient(0)
	defer kv.Close()

	for i := 0; i < 100; i++ {
		assert.NoError(t, kv.Set(fmt.Sprintf("k-%d", i), fmt.Sprintf("v-%d", i), testWaitTimeout))
	}

	c.Restart()
	c.WaitLeadersByCount(1, testWaitTimeout)

	kv2 := c.CreateTestKVClient(0)
	defer kv2.Close()

	for i := 0; i < 100; i++ {
		v, err := kv2.Get(fmt.Sprintf("k-%d", i), testWaitTimeout)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("v-%d", i), v)
	}
}
