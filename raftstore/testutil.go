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

	"github.com/fagongzi/log"
	"github.com/matrixorigin/matrixcube/aware"
	"github.com/matrixorigin/matrixcube/components/prophet"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	putil "github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/mem"
	"github.com/stretchr/testify/assert"
)

// TestClusterOption is the option for create TestCluster
type TestClusterOption func(*testClusterOptions)

type testClusterOptions struct {
	tmpDir            string
	nodes             int
	recreate          bool
	adjustConfigFuncs []func(node int, cfg *config.Config)
	storeFactory      func(node int, cfg *config.Config) Store
	nodeStartFunc     func(node int, store Store)
	logLevel          string
}

func (opts *testClusterOptions) adjust() {
	if opts.tmpDir == "" {
		opts.tmpDir = "/tmp/cube"
	}
	if opts.nodes == 0 {
		opts.nodes = 3
	}
	if opts.logLevel == "" {
		opts.logLevel = "error"
	}
}

// WithTestClusterLogLevel set raftstore log level
func WithTestClusterLogLevel(level string) TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.logLevel = level
	}
}

// WithTestClusterStoreFactory custom create raftstore factory
func WithTestClusterStoreFactory(value func(node int, cfg *config.Config) Store) TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.storeFactory = value
	}
}

// WithTestClusterNodeStartFunc custom node start func
func WithTestClusterNodeStartFunc(value func(node int, store Store)) TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.nodeStartFunc = value
	}
}

// WithTestClusterRecreate if true, the test cluster will clean and recreate the data dir
func WithTestClusterRecreate(value bool) TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.recreate = value
	}
}

// WithAppendTestClusterAdjustConfigFunc adjust config
func WithAppendTestClusterAdjustConfigFunc(value func(node int, cfg *config.Config)) TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.adjustConfigFuncs = append(opts.adjustConfigFuncs, value)
	}
}

func recreateTestTempDir(tmpDir string) {
	os.RemoveAll(tmpDir)
	os.MkdirAll(tmpDir, 0755)
}

// NewTestClusterStore create test cluster with 3 nodes, and the data dir is not empty
func NewTestClusterStore(t *testing.T, opts ...TestClusterOption) *TestRaftCluster {
	c := &TestRaftCluster{t: t, opts: &testClusterOptions{recreate: true}}
	for _, opt := range opts {
		opt(c.opts)
	}
	c.opts.adjust()

	log.SetHighlighting(false)
	log.SetLevelByString(c.opts.logLevel)
	putil.SetLogger(log.NewLoggerWithPrefix("prophet"))

	if c.opts.recreate {
		recreateTestTempDir(c.opts.tmpDir)
	}

	for i := 0; i < c.opts.nodes; i++ {
		cfg := &config.Config{}
		cfg.DataPath = fmt.Sprintf("%s/node-%d", c.opts.tmpDir, i)
		cfg.RaftAddr = fmt.Sprintf("127.0.0.1:1000%d", i)
		cfg.ClientAddr = fmt.Sprintf("127.0.0.1:2000%d", i)
		cfg.Labels = append(cfg.Labels, []string{"c", fmt.Sprintf("%d", i)})

		cfg.Replication.ShardHeartbeatDuration = typeutil.NewDuration(time.Millisecond * 100)
		cfg.Replication.StoreHeartbeatDuration = typeutil.NewDuration(time.Second)
		cfg.Replication.ShardSplitCheckDuration = typeutil.NewDuration(time.Millisecond * 100)
		cfg.Raft.TickInterval = typeutil.NewDuration(time.Millisecond * 300)

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

		for _, fn := range c.opts.adjustConfigFuncs {
			fn(i, cfg)
		}

		if cfg.Storage.MetaStorage == nil {
			cfg.Storage.MetaStorage = mem.NewStorage()
		}
		if cfg.Storage.DataStorageFactory == nil {
			dataStorage := mem.NewStorage()
			cfg.Storage.DataStorageFactory = func(group, shardID uint64) storage.DataStorage {
				return dataStorage
			}
			cfg.Storage.ForeachDataStorageFunc = func(cb func(storage.DataStorage)) {
				cb(dataStorage)
			}
			c.storages = append(c.storages, dataStorage)
		}

		ts := newTestShardAware()
		cfg.Customize.TestShardStateAware = ts

		if c.opts.storeFactory != nil {
			c.stores = append(c.stores, c.opts.storeFactory(i, cfg).(*store))
		} else {
			c.stores = append(c.stores, NewStore(cfg).(*store))
		}

		c.awares = append(c.awares, ts)
	}

	return c
}

type testShardAware struct {
	sync.RWMutex

	wrapper aware.ShardStateAware
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

func (ts *testShardAware) SetWrapper(wrapper aware.ShardStateAware) {
	ts.wrapper = wrapper
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

	if ts.wrapper != nil {
		ts.wrapper.Created(shard)
	}
}

func (ts *testShardAware) Splited(shard bhmetapb.Shard) {
	ts.Lock()
	defer ts.Unlock()

	for idx := range ts.shards {
		if ts.shards[idx].ID == shard.ID {
			ts.shards[idx] = shard
		}
	}

	if ts.wrapper != nil {
		ts.wrapper.Splited(shard)
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

	if ts.wrapper != nil {
		ts.wrapper.Destory(shard)
	}
}

func (ts *testShardAware) BecomeLeader(shard bhmetapb.Shard) {
	ts.Lock()
	defer ts.Unlock()

	ts.leaders[shard.ID] = true

	if ts.wrapper != nil {
		ts.wrapper.BecomeLeader(shard)
	}
}

func (ts *testShardAware) BecomeFollower(shard bhmetapb.Shard) {
	ts.Lock()
	defer ts.Unlock()

	ts.leaders[shard.ID] = false

	if ts.wrapper != nil {
		ts.wrapper.BecomeFollower(shard)
	}
}

func (ts *testShardAware) SnapshotApplied(shard bhmetapb.Shard) {
	ts.Lock()
	defer ts.Unlock()

	ts.applied[shard.ID]++

	if ts.wrapper != nil {
		ts.wrapper.SnapshotApplied(shard)
	}
}

// TestRaftCluster test cluster
type TestRaftCluster struct {
	sync.RWMutex

	opts     *testClusterOptions
	t        *testing.T
	stores   []*store
	awares   []*testShardAware
	storages []*mem.Storage
}

// EveryStore do every store, it can be used to init some store register
func (c *TestRaftCluster) EveryStore(fn func(i int, store Store)) {
	for idx, store := range c.stores {
		fn(idx, store)
	}
}

// GetStore returns the node store
func (c *TestRaftCluster) GetStore(node int) Store {
	return c.stores[node]
}

// GetWatcher returns the node store router watcher
func (c *TestRaftCluster) GetWatcher(node int) prophet.Watcher {
	return c.stores[node].router.(*defaultRouter).watcher
}

// Start start the test raft cluster
func (c *TestRaftCluster) Start() {
	var wg sync.WaitGroup
	for i, s := range c.stores {
		wg.Add(1)
		fn := func(i int) {
			defer wg.Done()

			if c.opts.nodeStartFunc != nil {
				c.opts.nodeStartFunc(i, c.stores[i])
			} else {
				s.Start()
			}
		}

		if c.opts.recreate {
			fn(i)
		} else {
			go fn(i)
		}
	}

	wg.Wait()
}

// Stop stop the test cluster
func (c *TestRaftCluster) Stop() {
	for _, s := range c.stores {
		s.Stop()
	}
}

// GetPRCount returns peer replica count of node
func (c *TestRaftCluster) GetPRCount(nodeIndex int) int {
	cnt := 0
	c.stores[nodeIndex].replicas.Range(func(k, v interface{}) bool {
		cnt++
		return true
	})
	return cnt
}

func (c *TestRaftCluster) set(key, value []byte) {
	shard := c.GetShardByIndex(0)
	for idx, s := range c.stores {
		c.storages[idx].Set(key, value)
		pr := s.getPR(shard.ID, false)
		pr.sizeDiffHint += uint64(len(key) + len(value))
	}
}

// GetShardByIndex returns the shard by index
func (c *TestRaftCluster) GetShardByIndex(index int) bhmetapb.Shard {
	return c.awares[0].getShardByIndex(index)
}

// CheckShardCount check shard count
func (c *TestRaftCluster) CheckShardCount(t *testing.T, count int) {
	for idx := range c.stores {
		assert.Equal(t, count, c.GetPRCount(idx))
	}
}

// CheckShardRange check shard range
func (c *TestRaftCluster) CheckShardRange(t *testing.T, index int, start, end []byte) {
	for idx := range c.stores {
		shard := c.awares[idx].getShardByIndex(index)
		assert.Equal(t, start, shard.Start)
		assert.Equal(t, end, shard.End)
	}
}

// WaitRemovedByShardID check whether the specific shard removed until timeout
func (c *TestRaftCluster) WaitRemovedByShardID(t *testing.T, id uint64, timeout time.Duration) {
	for idx := range c.stores {
		c.awares[idx].waitRemovedByShardID(t, id, timeout)
	}
}

// WaitShardByCount check whether the number of shards reaches a specific value until timeout
func (c *TestRaftCluster) WaitShardByCount(t *testing.T, count int, timeout time.Duration) {
	for idx := range c.stores {
		c.awares[idx].waitByShardCount(t, count, timeout)
	}
}

// WaitShardByCounts check whether the number of shards reaches a specific value until timeout
func (c *TestRaftCluster) WaitShardByCounts(t *testing.T, counts [3]int, timeout time.Duration) {
	for idx := range c.stores {
		c.awares[idx].waitByShardCount(t, counts[idx], timeout)
	}
}

// WaitShardStateChangedTo check whether the state of shard changes to the specific value until timeout
func (c *TestRaftCluster) WaitShardStateChangedTo(t *testing.T, id uint64, to metapb.ResourceState, timeout time.Duration) {
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
func (c *TestRaftCluster) GetProphet() prophet.Prophet {
	return c.stores[0].pd
}
