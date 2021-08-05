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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/aware"
	"github.com/matrixorigin/matrixcube/components/prophet"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/mem"
	"github.com/stretchr/testify/assert"
)

func recreateTestTempDir(tmpDir string) {
	os.RemoveAll(tmpDir)
	os.MkdirAll(tmpDir, 0755)
}

// NewTestClusterStore create test cluster with 3 nodes
func NewTestClusterStore(t *testing.T, tmpDir string, adjustFunc func(cfg *config.Config), initAttrsFunc func(int, *config.Config, Store, map[string]interface{}), startNodeFunc func(map[string]interface{})) *TestCluster {
	if tmpDir == "" {
		tmpDir = "/tmp/cube"
	}

	recreateTestTempDir(tmpDir)
	c := &TestCluster{t: t, startNodeFunc: startNodeFunc}
	for i := 0; i < 3; i++ {
		dataStorage := mem.NewStorage()
		cfg := &config.Config{}
		cfg.DataPath = fmt.Sprintf("%s/node-%d", tmpDir, i)
		cfg.RaftAddr = fmt.Sprintf("127.0.0.1:1000%d", i)
		cfg.ClientAddr = fmt.Sprintf("127.0.0.1:2000%d", i)
		cfg.Labels = append(cfg.Labels, []string{"c", fmt.Sprintf("%d", i)})

		cfg.Replication.ShardHeartbeatDuration = typeutil.NewDuration(time.Millisecond * 100)
		cfg.Replication.StoreHeartbeatDuration = typeutil.NewDuration(time.Second)
		cfg.Replication.ShardSplitCheckDuration = typeutil.NewDuration(time.Millisecond * 100)
		cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(20)
		cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(10)
		cfg.Raft.TickInterval = typeutil.NewDuration(time.Millisecond * 100)

		cfg.Prophet.Name = fmt.Sprintf("node-%d", i)
		cfg.Prophet.StorageNode = true
		cfg.Prophet.RPCAddr = fmt.Sprintf("127.0.0.1:3000%d", i)
		if i != 0 {
			cfg.Prophet.EmbedEtcd.Join = "http://127.0.0.1:40000"
		}
		cfg.Prophet.EmbedEtcd.TickInterval = typeutil.NewDuration(time.Millisecond * 100)
		cfg.Prophet.EmbedEtcd.ClientUrls = fmt.Sprintf("http://127.0.0.1:4000%d", i)
		cfg.Prophet.EmbedEtcd.PeerUrls = fmt.Sprintf("http://127.0.0.1:5000%d", i)
		cfg.Prophet.Schedule.EnableJointConsensus = true

		cfg.Storage.MetaStorage = mem.NewStorage()
		cfg.Storage.DataStorageFactory = func(group, shardID uint64) storage.DataStorage {
			return dataStorage
		}
		cfg.Storage.ForeachDataStorageFunc = func(cb func(storage.DataStorage)) {
			cb(dataStorage)
		}

		if adjustFunc != nil {
			adjustFunc(cfg)
		}

		ts := newTestShardAware()
		cfg.Customize.CustomShardStateAwareFactory = func() aware.ShardStateAware {
			return ts
		}

		c.stores = append(c.stores, NewStore(cfg).(*store))
		c.awares = append(c.awares, ts)
		c.storages = append(c.storages, dataStorage)
		c.attrs = append(c.attrs, make(map[string]interface{}))

		if initAttrsFunc != nil {
			initAttrsFunc(i, cfg, c.stores[i], c.attrs[i])
		}
	}

	return c
}

type testShardAware struct {
	sync.RWMutex

	shards  []bhmetapb.Shard
	leaders map[uint64]bool
	applied map[uint64]int

	removed map[uint64]bhmetapb.Shard
}

func newTestShardAware() *testShardAware {
	return &testShardAware{
		leaders: make(map[uint64]bool),
		applied: make(map[uint64]int),
		removed: make(map[uint64]bhmetapb.Shard),
	}
}

func (ts *testShardAware) waitRemovedByShardID(t *testing.T, id uint64, timeout time.Duration) {
	timeoutC := time.After(timeout)
	for {
		select {
		case <-timeoutC:
			assert.FailNowf(t, "", "wait remove shard %d timeout", id)
		default:
			if !ts.hasShard(id) {
				assert.Equal(t, metapb.ResourceState_Removed, ts.removed[id].State)
				return
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (ts *testShardAware) waitByShardCount(t *testing.T, count int, timeout time.Duration) {
	timeoutC := time.After(timeout)
	for {
		select {
		case <-timeoutC:
			assert.FailNowf(t, "", "wait shard count %d timeout", count)
		default:
			if ts.shardCount() >= count {
				return
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (ts *testShardAware) hasShard(id uint64) bool {
	ts.RLock()
	defer ts.RUnlock()

	for _, shard := range ts.shards {
		if shard.ID == id {
			return true
		}
	}
	return false
}

func (ts *testShardAware) getShardByIndex(index int) bhmetapb.Shard {
	ts.RLock()
	defer ts.RUnlock()

	return ts.shards[index]
}

func (ts *testShardAware) shardCount() int {
	ts.RLock()
	defer ts.RUnlock()

	return len(ts.shards)
}

func (ts *testShardAware) Created(shard bhmetapb.Shard) {
	ts.Lock()
	defer ts.Unlock()

	ts.shards = append(ts.shards, shard)
	ts.leaders[shard.ID] = false
}

func (ts *testShardAware) Splited(shard bhmetapb.Shard) {
	ts.Lock()
	defer ts.Unlock()

	for idx := range ts.shards {
		if ts.shards[idx].ID == shard.ID {
			ts.shards[idx] = shard
		}
	}
}

func (ts *testShardAware) Destory(shard bhmetapb.Shard) {
	ts.Lock()
	defer ts.Unlock()

	var newShards []bhmetapb.Shard
	for _, s := range ts.shards {
		if s.ID != shard.ID {
			newShards = append(newShards, s)
		}
	}
	ts.shards = newShards
	delete(ts.leaders, shard.ID)
	ts.removed[shard.ID] = shard
}

func (ts *testShardAware) BecomeLeader(shard bhmetapb.Shard) {
	ts.Lock()
	defer ts.Unlock()

	ts.leaders[shard.ID] = true
}

func (ts *testShardAware) BecomeFollower(shard bhmetapb.Shard) {
	ts.Lock()
	defer ts.Unlock()

	ts.leaders[shard.ID] = false
}

func (ts *testShardAware) SnapshotApplied(shard bhmetapb.Shard) {
	ts.Lock()
	defer ts.Unlock()

	ts.applied[shard.ID]++
}

// TestCluster test cluster
type TestCluster struct {
	sync.RWMutex

	t             *testing.T
	stores        []*store
	awares        []*testShardAware
	storages      []*mem.Storage
	attrs         []map[string]interface{}
	startNodeFunc func(map[string]interface{})
}

// Start start the test cluster
func (c *TestCluster) Start() {
	for i, s := range c.stores {
		if c.startNodeFunc == nil {
			s.Start()
		} else {
			c.startNodeFunc(c.attrs[i])
		}
	}
}

// Stop stop the test cluster
func (c *TestCluster) Stop() {
	for _, s := range c.stores {
		s.Stop()
	}
}

// GetPRCount returns peer replica count of node
func (c *TestCluster) GetPRCount(nodeIndex int) int {
	cnt := 0
	c.stores[nodeIndex].replicas.Range(func(k, v interface{}) bool {
		cnt++
		return true
	})
	return cnt
}

// Set set key, values to storage for all nodes
func (c *TestCluster) Set(key, value []byte) {
	shard := c.GetShardByIndex(0)
	for idx, s := range c.stores {
		c.storages[idx].Set(key, value)
		pr := s.getPR(shard.ID, false)
		pr.sizeDiffHint += uint64(len(key) + len(value))
	}
}

// GetShardByIndex returns the shard by index
func (c *TestCluster) GetShardByIndex(index int) bhmetapb.Shard {
	return c.awares[0].getShardByIndex(index)
}

// CheckShardCount check shard count
func (c *TestCluster) CheckShardCount(t *testing.T, count int) {
	for idx := range c.stores {
		assert.Equal(t, count, c.GetPRCount(idx))
	}
}

// CheckShardRange check shard range
func (c *TestCluster) CheckShardRange(t *testing.T, index int, start, end []byte) {
	for idx := range c.stores {
		shard := c.awares[idx].getShardByIndex(index)
		assert.Equal(t, start, shard.Start)
		assert.Equal(t, end, shard.End)
	}
}

// WaitRemovedByShardID check whether the specific shard removed until timeout
func (c *TestCluster) WaitRemovedByShardID(t *testing.T, id uint64, timeout time.Duration) {
	for idx := range c.stores {
		c.awares[idx].waitRemovedByShardID(t, id, timeout)
	}
}

// WaitShardByCount check whether the number of shards reaches a specific value until timeout
func (c *TestCluster) WaitShardByCount(t *testing.T, count int, timeout time.Duration) {
	for idx := range c.stores {
		c.awares[idx].waitByShardCount(t, count, timeout)
	}
}

// WaitShardByCounts check whether the number of shards reaches a specific value until timeout
func (c *TestCluster) WaitShardByCounts(t *testing.T, counts [3]int, timeout time.Duration) {
	for idx := range c.stores {
		c.awares[idx].waitByShardCount(t, counts[idx], timeout)
	}
}

// WaitShardStateChangedTo check whether the state of shard changes to the specific value until timeout
func (c *TestCluster) WaitShardStateChangedTo(t *testing.T, id uint64, to metapb.ResourceState, timeout time.Duration) {
	timeoutC := time.After(timeout)
	for {
		select {
		case <-timeoutC:
			assert.FailNowf(t, "", "wait shard state changed to %+v timeout", to)
		default:
			res, err := c.GetProphet().GetStorage().GetResource(id)
			assert.NoError(t, err)
			if res.State() == to {
				return
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

// GetProphet returns prophet
func (c *TestCluster) GetProphet() prophet.Prophet {
	return c.stores[0].pd
}

// GetAttr get node attr
func (c *TestCluster) GetAttr(node int, attr string) interface{} {
	c.RLock()
	defer c.RUnlock()

	return c.attrs[node][attr]
}

// SetAttr set node attr
func (c *TestCluster) SetAttr(node int, attr string, value interface{}) {
	c.Lock()
	defer c.Unlock()

	c.attrs[node][attr] = value
}
