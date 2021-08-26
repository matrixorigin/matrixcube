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
	"sort"
	"sync"
	"testing"
	"time"

	cpebble "github.com/cockroachdb/pebble"
	"github.com/fagongzi/log"
	"github.com/matrixorigin/matrixcube/aware"
	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/components/prophet"
	pconfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	putil "github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/mem"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
)

var (
	// SingleTestCluster single test raft cluster
	SingleTestCluster = WithTestClusterNodeCount(1)
	// DiskTestCluster using pebble storage test raft store
	DiskTestCluster = WithTestClusterUseDisk()
	// DisableScheduleTestCluster disable prophet schedulers  test raft store
	DisableScheduleTestCluster = WithTestClusterDisableSchedule()
	// NewTestCluster clean data before test cluster start
	NewTestCluster = WithTestClusterRecreate(true)
	// NoCleanTestCluster using exists data before test cluster start
	OldTestCluster = WithTestClusterRecreate(false)
	// SetCMDTestClusterHandler set cmd handler
	SetCMDTestClusterHandler = WithTestClusterWriteHandler(1, func(s bhmetapb.Shard, r *raftcmdpb.Request, c command.Context) (uint64, int64, *raftcmdpb.Response) {
		resp := pb.AcquireResponse()
		c.WriteBatch().Set(r.Key, r.Cmd)
		resp.Value = []byte("OK")
		changed := uint64(len(r.Key)) + uint64(len(r.Cmd))
		return changed, int64(changed), resp
	})
	// GetCMDTestClusterHandler get cmd handler
	GetCMDTestClusterHandler = WithTestClusterReadHandler(2, func(s bhmetapb.Shard, r *raftcmdpb.Request, c command.Context) (*raftcmdpb.Response, uint64) {
		resp := pb.AcquireResponse()
		value, err := c.DataStorage().(storage.KVStorage).Get(r.Key)
		if err != nil {
			panic("BUG: can not error")
		}
		resp.Value = value
		return resp, uint64(len(value))
	})

	testWaitTimeout = time.Minute
)

// TestClusterOption is the option for create TestCluster
type TestClusterOption func(*testClusterOptions)

type testClusterOptions struct {
	tmpDir             string
	nodes              int
	recreate           bool
	adjustConfigFuncs  []func(node int, cfg *config.Config)
	storeFactory       func(node int, cfg *config.Config) Store
	nodeStartFunc      func(node int, store Store)
	logLevel           string
	useDisk            bool
	dataOpts, metaOpts *cpebble.Options

	writeHandlers map[uint64]command.WriteCommandFunc
	readHandlers  map[uint64]command.ReadCommandFunc

	disableSchedule bool
}

func newTestClusterOptions() *testClusterOptions {
	return &testClusterOptions{
		recreate:      true,
		dataOpts:      &cpebble.Options{},
		metaOpts:      &cpebble.Options{},
		writeHandlers: make(map[uint64]command.WriteCommandFunc),
		readHandlers:  make(map[uint64]command.ReadCommandFunc),
	}
}

func (opts *testClusterOptions) adjust() {
	if opts.tmpDir == "" {
		opts.tmpDir = util.GetTestDir()
	}
	if opts.nodes == 0 {
		opts.nodes = 3
	}
	if opts.logLevel == "" {
		opts.logLevel = "info"
	}
}

// WithTestClusterDataPath set data data storage directory
func WithTestClusterDataPath(path string) TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.tmpDir = path
	}
}

// WithTestClusterNodeCount set node count of test cluster
func WithTestClusterNodeCount(n int) TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.nodes = n
	}
}

// WithTestClusterUseDisk use disk storage for testing
func WithTestClusterUseDisk() TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.useDisk = true
	}
}

// WithTestClusterDisableSchedule disable pd schedule
func WithTestClusterDisableSchedule() TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.disableSchedule = true
	}
}

// WithTestClusterWriteHandler write handlers
func WithTestClusterWriteHandler(cmd uint64, value command.WriteCommandFunc) TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.writeHandlers[cmd] = value
	}
}

// WithTestClusterReadHandler read handlers
func WithTestClusterReadHandler(cmd uint64, value command.ReadCommandFunc) TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.readHandlers[cmd] = value
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

// WithDataStorageOption set options to create data storage
func WithDataStorageOption(dataOpts *cpebble.Options) TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.dataOpts = dataOpts
	}
}

// WithMetadataStorageOption set options to create metadata storage
func WithMetadataStorageOption(metaOpts *cpebble.Options) TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.metaOpts = metaOpts
	}
}

// WithAppendTestClusterAdjustConfigFunc adjust config
func WithAppendTestClusterAdjustConfigFunc(value func(node int, cfg *config.Config)) TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.adjustConfigFuncs = append(opts.adjustConfigFuncs, value)
	}
}

func recreateTestTempDir(fs vfs.FS, tmpDir string) {
	fs.RemoveAll(tmpDir)
	fs.MkdirAll(tmpDir, 0755)
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

func (ts *testShardAware) getShardByID(id uint64) bhmetapb.Shard {
	ts.RLock()
	defer ts.RUnlock()

	for _, s := range ts.shards {
		if s.ID == id {
			return s
		}
	}

	return bhmetapb.Shard{}
}

func (ts *testShardAware) shardCount() int {
	ts.RLock()
	defer ts.RUnlock()

	return len(ts.shards)
}

func (ts *testShardAware) leaderCount() int {
	ts.RLock()
	defer ts.RUnlock()

	c := 0
	for _, ok := range ts.leaders {
		if ok {
			c++
		}
	}
	return c
}

func (ts *testShardAware) isLeader(id uint64) bool {
	ts.RLock()
	defer ts.RUnlock()

	return ts.leaders[id]
}

func (ts *testShardAware) Created(shard bhmetapb.Shard) {
	ts.Lock()
	defer ts.Unlock()

	ts.shards = append(ts.shards, shard)
	ts.leaders[shard.ID] = false

	sort.Slice(ts.shards, func(i, j int) bool {
		return ts.shards[i].ID < ts.shards[j].ID
	})

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

	// init fields
	t        *testing.T
	initOpts []TestClusterOption

	// reset fields
	opts             *testClusterOptions
	stores           []*store
	awares           []*testShardAware
	dataStorages     []storage.DataStorage
	metadataStorages []storage.MetadataStorage
}

// NewSingleTestClusterStore create test cluster with 1 node
func NewSingleTestClusterStore(t *testing.T, opts ...TestClusterOption) *TestRaftCluster {
	return NewTestClusterStore(t, append(opts, WithTestClusterNodeCount(1), WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
		cfg.Prophet.Replication.MaxReplicas = 1
	}))...)
}

// NewTestClusterStore create test cluster using options
func NewTestClusterStore(t *testing.T, opts ...TestClusterOption) *TestRaftCluster {
	c := &TestRaftCluster{t: t, initOpts: opts}
	c.reset(opts...)
	return c
}

func (c *TestRaftCluster) reset(opts ...TestClusterOption) {
	c.opts = newTestClusterOptions()
	c.stores = nil
	c.awares = nil
	c.dataStorages = nil
	c.metadataStorages = nil

	for _, opt := range opts {
		opt(c.opts)
	}
	c.opts.adjust()

	log.SetHighlighting(false)
	log.SetLevelByString(c.opts.logLevel)
	putil.SetLogger(log.NewLoggerWithPrefix("prophet"))

	if c.opts.disableSchedule {
		pconfig.DefaultSchedulers = nil
	}

	for i := 0; i < c.opts.nodes; i++ {
		cfg := &config.Config{}
		cfg.FS = vfs.GetTestFS()
		cfg.DataPath = fmt.Sprintf("%s/node-%d", c.opts.tmpDir, i)
		if c.opts.recreate {
			recreateTestTempDir(cfg.FS, cfg.DataPath)
		}

		cfg.RaftAddr = fmt.Sprintf("127.0.0.1:1000%d", i)
		cfg.ClientAddr = fmt.Sprintf("127.0.0.1:2000%d", i)
		cfg.Labels = append(cfg.Labels, []string{"c", fmt.Sprintf("%d", i)})

		if c.opts.nodes < 3 {
			cfg.Prophet.Replication.MaxReplicas = uint64(c.opts.nodes)
		}

		cfg.Replication.ShardHeartbeatDuration = typeutil.NewDuration(time.Millisecond * 100)
		cfg.Replication.StoreHeartbeatDuration = typeutil.NewDuration(time.Second)
		cfg.Replication.ShardSplitCheckDuration = typeutil.NewDuration(time.Millisecond * 100)
		cfg.Raft.TickInterval = typeutil.NewDuration(time.Millisecond * 100)

		cfg.Worker.RaftEventWorkers = 1
		cfg.Worker.ApplyWorkerCount = 1
		cfg.Worker.SendRaftMsgWorkerCount = 1

		// TODO: duplicated field
		cfg.Prophet.FS = cfg.FS
		cfg.Prophet.Name = fmt.Sprintf("node-%d", i)
		cfg.Prophet.StorageNode = true
		cfg.Prophet.RPCAddr = fmt.Sprintf("127.0.0.1:3000%d", i)
		if i != 0 {
			cfg.Prophet.EmbedEtcd.Join = "http://127.0.0.1:40000"
		}
		cfg.Prophet.EmbedEtcd.TickInterval.Duration = time.Millisecond * 30
		cfg.Prophet.EmbedEtcd.ElectionInterval.Duration = time.Millisecond * 150
		cfg.Prophet.EmbedEtcd.ClientUrls = fmt.Sprintf("http://127.0.0.1:4000%d", i)
		cfg.Prophet.EmbedEtcd.PeerUrls = fmt.Sprintf("http://127.0.0.1:5000%d", i)
		cfg.Prophet.Schedule.EnableJointConsensus = true

		for _, fn := range c.opts.adjustConfigFuncs {
			fn(i, cfg)
		}

		// check whether the raft tickinterval is set properly.
		// If the time that the raft log persists to disk is longer
		// than the election timeout time, then the entire cluster
		// cannot work normally
		electionDuration := cfg.Raft.GetElectionTimeoutDuration()
		testFsyncDuration := getRTTMillisecond(cfg.FS, cfg.DataPath)
		if !(electionDuration >= 10*testFsyncDuration) {
			old := cfg.Raft.TickInterval.Duration
			cfg.Raft.TickInterval.Duration = 10 * testFsyncDuration
			cfg.Prophet.EmbedEtcd.TickInterval.Duration = 10 * testFsyncDuration
			cfg.Prophet.EmbedEtcd.ElectionInterval.Duration = 5 * cfg.Prophet.EmbedEtcd.TickInterval.Duration
			logger.Warningf("########## adjust Raft.TickInterval from %s to %s, because current fsync on current fs is %s",
				old,
				cfg.Raft.TickInterval.Duration,
				testFsyncDuration)
		}

		if cfg.Storage.MetaStorage == nil {
			var metaStorage storage.MetadataStorage
			metaStorage = mem.NewStorage(cfg.FS)
			if c.opts.useDisk {
				c.opts.metaOpts.FS = vfs.NewPebbleFS(cfg.FS)
				s, err := pebble.NewStorage(cfg.FS.PathJoin(cfg.DataPath, "meta"), c.opts.metaOpts)
				assert.NoError(c.t, err)
				metaStorage = s
			}
			cfg.Storage.MetaStorage = metaStorage
			c.metadataStorages = append(c.metadataStorages, metaStorage)
		}
		if cfg.Storage.DataStorageFactory == nil {
			var dataStorage storage.DataStorage
			dataStorage = mem.NewStorage(cfg.FS)
			if c.opts.useDisk {
				c.opts.metaOpts.FS = vfs.NewPebbleFS(cfg.FS)
				s, err := pebble.NewStorage(cfg.FS.PathJoin(cfg.DataPath, "data"), c.opts.metaOpts)
				assert.NoError(c.t, err)
				dataStorage = s
			}

			cfg.Storage.DataStorageFactory = func(group, shardID uint64) storage.DataStorage {
				return dataStorage
			}
			cfg.Storage.ForeachDataStorageFunc = func(cb func(storage.DataStorage)) {
				cb(dataStorage)
			}
			c.dataStorages = append(c.dataStorages, dataStorage)
		}

		ts := newTestShardAware()
		cfg.Test.ShardStateAware = ts

		var s *store
		if c.opts.storeFactory != nil {
			s = c.opts.storeFactory(i, cfg).(*store)
		} else {
			s = NewStore(cfg).(*store)
		}

		for k, h := range c.opts.writeHandlers {
			s.RegisterWriteFunc(k, h)
		}

		for k, h := range c.opts.readHandlers {
			s.RegisterReadFunc(k, h)
		}

		c.stores = append(c.stores, s)
		c.awares = append(c.awares, ts)
	}
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

// StartNode start node
func (c *TestRaftCluster) StartNode(node int) {
	c.stores[node].Start()
}

// StopNode start node
func (c *TestRaftCluster) StopNode(node int) {
	c.stores[node].Stop()
}

// Start start the test raft cluster
func (c *TestRaftCluster) Start() {
	c.StartWithConcurrent(false)
}

// StartWithConcurrent start the test raft cluster, if concurrent set to true, the nodes will concurrent start exclude first
func (c *TestRaftCluster) StartWithConcurrent(concurrent bool) {
	var wg sync.WaitGroup
	for i := range c.stores {
		wg.Add(1)
		fn := func(i int) {
			defer wg.Done()

			if c.opts.nodeStartFunc != nil {
				c.opts.nodeStartFunc(i, c.stores[i])
			} else {
				c.stores[i].Start()
			}
		}

		if c.opts.recreate {
			if concurrent && i != 0 {
				go fn(i)
			} else {
				fn(i)
			}
		} else {
			go fn(i)
		}
	}

	wg.Wait()
}

// Restart restart the test cluster
func (c *TestRaftCluster) Restart() {
	c.RestartWithFunc(nil)
}

// RestartWithFunc restart the test cluster
func (c *TestRaftCluster) RestartWithFunc(beforeStartFunc func()) {
	c.Stop()
	opts := append(c.initOpts, WithTestClusterRecreate(false))
	c.reset(opts...)
	if beforeStartFunc != nil {
		beforeStartFunc()
	}
	c.Start()
}

// Stop stop the test cluster
func (c *TestRaftCluster) Stop() {
	for _, s := range c.stores {
		s.Stop()
	}

	for _, s := range c.dataStorages {
		s.Close()
	}

	for _, s := range c.metadataStorages {
		s.Close()
	}

	for _, s := range c.stores {
		fs := s.cfg.FS
		if fs == nil {
			panic("fs not set")
		}
		vfs.ReportLeakedFD(fs, c.t)
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
		c.dataStorages[idx].(storage.KVStorage).Set(key, value)
		pr := s.getPR(shard.ID, false)
		pr.sizeDiffHint += uint64(len(key) + len(value))
	}
}

// GetShardByIndex returns the shard by index
func (c *TestRaftCluster) GetShardByIndex(index int) bhmetapb.Shard {
	return c.awares[0].getShardByIndex(index)
}

// GetShardByID returns the shard by id
func (c *TestRaftCluster) GetShardByID(id uint64) bhmetapb.Shard {
	return c.awares[0].getShardByID(id)
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

// GetShardLeaderStore returns the shard leader store
func (c *TestRaftCluster) GetShardLeaderStore(id uint64) Store {
	for idx, s := range c.stores {
		if c.awares[idx].isLeader(id) {
			return s
		}
	}
	return nil
}

// WaitLeadersByCount check whether the number of shards leaders reaches a specific value until timeout
func (c *TestRaftCluster) WaitLeadersByCount(t *testing.T, count int, timeout time.Duration) {
	timeoutC := time.After(timeout)
	for {
		select {
		case <-timeoutC:
			assert.FailNowf(t, "", "wait leader count %d timeout", count)
		default:
			leaders := 0
			for idx := range c.stores {
				leaders += c.awares[idx].leaderCount()
			}
			if leaders >= count {
				return
			}
			time.Sleep(time.Millisecond * 100)
		}
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
			if res != nil && res.State() == to {
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

var rttMillisecond uint64
var mu sync.Mutex
var rttValues = []uint64{10, 20, 30, 50, 100, 200, 500}

func getRTTMillisecond(fs vfs.FS, dir string) time.Duration {
	mu.Lock()
	defer mu.Unlock()

	rttMillisecond = calcRTTMillisecond(fs, dir)
	return time.Duration(rttMillisecond) * time.Millisecond
}

func calcRTTMillisecond(fs vfs.FS, dir string) uint64 {
	testFile := fs.PathJoin(dir, ".cube_test_file_safe_to_delete")
	defer func() {
		_ = fs.RemoveAll(testFile)
	}()
	_ = fs.MkdirAll(dir, 0755)
	f, err := fs.Create(testFile)
	if err != nil {
		panic(err)
	}
	defer func() {
		f.Close()
	}()
	data := make([]byte, 512)
	total := uint64(0)
	repeat := 5
	for i := 0; i < repeat; i++ {
		if _, err := f.Write(data); err != nil {
			panic(err)
		}
		start := time.Now()
		if err := f.Sync(); err != nil {
			panic(err)
		}
		total += uint64(time.Since(start).Milliseconds())
	}
	rtt := total / uint64(repeat)
	for i := range rttValues {
		if rttValues[i] > rtt {
			if i == 0 {
				return rttValues[0]
			}
			return rttValues[i-1]
		}
	}
	return rttValues[len(rttValues)-1]
}
