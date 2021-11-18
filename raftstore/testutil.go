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
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cpebble "github.com/cockroachdb/pebble"
	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixcube/aware"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet"
	pconfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor/simple"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/storage/kv/pebble"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/util/task"
	"github.com/matrixorigin/matrixcube/util/testutil"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
	logLevel          zapcore.Level
	useDisk           bool
	dataOpts          *cpebble.Options

	disableSchedule    bool
	enableParallelTest bool
}

func newTestClusterOptions() *testClusterOptions {
	return &testClusterOptions{
		recreate: true,
		dataOpts: &cpebble.Options{},
	}
}

func (opts *testClusterOptions) adjust() {
	if opts.tmpDir == "" {
		opts.tmpDir = util.GetTestDir()
	}
	if opts.nodes == 0 {
		opts.nodes = 3
	}
	if opts.logLevel == 0 {
		opts.logLevel = zap.DebugLevel
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

// WithEnableTestParallel enable parallel testing
func WithEnableTestParallel() TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.enableParallelTest = true
	}
}

// WithTestClusterLogLevel set raftstore log level
func WithTestClusterLogLevel(level zapcore.Level) TestClusterOption {
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

	wrapper      aware.ShardStateAware
	shards       []Shard
	leaders      map[uint64]bool
	applied      map[uint64]int
	splitedCount map[uint64]int

	removed map[uint64]Shard
}

func newTestShardAware() *testShardAware {
	return &testShardAware{
		leaders:      make(map[uint64]bool),
		applied:      make(map[uint64]int),
		removed:      make(map[uint64]Shard),
		splitedCount: make(map[uint64]int),
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

func (ts *testShardAware) waitByShardSplitCount(t *testing.T, id uint64, count int, timeout time.Duration) {
	timeoutC := time.After(timeout)
	for {
		select {
		case <-timeoutC:
			assert.FailNowf(t, "", "wait shard %d split count %d timeout", id, count)
		default:
			if ts.shardSplitedCount(id) >= count {
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

func (ts *testShardAware) getShardByIndex(index int) Shard {
	ts.RLock()
	defer ts.RUnlock()

	return ts.shards[index]
}

func (ts *testShardAware) getShardByID(id uint64) Shard {
	ts.RLock()
	defer ts.RUnlock()

	for _, s := range ts.shards {
		if s.ID == id {
			return s
		}
	}

	return Shard{}
}

func (ts *testShardAware) shardCount() int {
	ts.RLock()
	defer ts.RUnlock()

	return len(ts.shards)
}

func (ts *testShardAware) shardSplitedCount(id uint64) int {
	ts.RLock()
	defer ts.RUnlock()

	return ts.splitedCount[id]
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

func (ts *testShardAware) Created(shard Shard) {
	ts.Lock()
	defer ts.Unlock()

	ts.shards = append(ts.shards, shard)
	ts.leaders[shard.ID] = false
	ts.splitedCount[shard.ID] = 0

	sort.Slice(ts.shards, func(i, j int) bool {
		return ts.shards[i].ID < ts.shards[j].ID
	})

	if ts.wrapper != nil {
		ts.wrapper.Created(shard)
	}
}

func (ts *testShardAware) Splited(shard Shard) {
	ts.Lock()
	defer ts.Unlock()

	for idx := range ts.shards {
		if ts.shards[idx].ID == shard.ID {
			ts.shards[idx] = shard
			ts.splitedCount[shard.ID]++
		}
	}

	if ts.wrapper != nil {
		ts.wrapper.Splited(shard)
	}
}

func (ts *testShardAware) Destroyed(shard Shard) {
	ts.Lock()
	defer ts.Unlock()

	var newShards []Shard
	for _, s := range ts.shards {
		if s.ID != shard.ID {
			newShards = append(newShards, s)
		}
	}
	ts.shards = newShards
	delete(ts.leaders, shard.ID)
	ts.removed[shard.ID] = shard

	if ts.wrapper != nil {
		ts.wrapper.Destroyed(shard)
	}
}

func (ts *testShardAware) BecomeLeader(shard Shard) {
	ts.Lock()
	defer ts.Unlock()

	ts.leaders[shard.ID] = true

	if ts.wrapper != nil {
		ts.wrapper.BecomeLeader(shard)
	}
}

func (ts *testShardAware) BecomeFollower(shard Shard) {
	ts.Lock()
	defer ts.Unlock()

	ts.leaders[shard.ID] = false

	if ts.wrapper != nil {
		ts.wrapper.BecomeFollower(shard)
	}
}

func (ts *testShardAware) SnapshotApplied(shard Shard) {
	ts.Lock()
	defer ts.Unlock()

	ts.applied[shard.ID]++
	for idx, s := range ts.shards {
		if s.ID == shard.ID {
			ts.shards[idx] = shard
		}
	}

	if ts.wrapper != nil {
		ts.wrapper.SnapshotApplied(shard)
	}
}

// TestRaftCluster is the test cluster is used to test starting N nodes in a process, and to provide
// the start and stop capabilities of a single node, which is used to test `raftstore` more easily.
type TestRaftCluster interface {
	// EveryStore do fn at every store, it can be used to init some store register
	EveryStore(fn func(i int, store Store))
	// GetStore returns the node store
	GetStore(node int) Store
	// // GetWatcher returns event watcher of the node
	// GetWatcher(node int) prophet.Watcher
	// Start start each node sequentially
	Start()
	// Stop stop each node sequentially
	Stop()
	// StartWithConcurrent after starting the first node, other nodes start concurrently
	StartWithConcurrent(bool)
	// Restart restart the cluster
	Restart()
	// RestartWithFunc restart the cluster, `beforeStartFunc` is called before starting
	RestartWithFunc(beforeStartFunc func())
	// GetPRCount returns the number of replicas on the node
	GetPRCount(node int) int
	// GetShardByIndex returns the shard by `shardIndex`, `shardIndex` is the order in which
	// the shard is created on the node
	GetShardByIndex(node int, shardIndex int) Shard
	// GetShardByID returns the shard from the node by shard id
	GetShardByID(node int, shardID uint64) Shard
	// CheckShardCount check whether the number of shards on each node is correct
	CheckShardCount(countPerNode int)
	// CheckShardRange check whether the range field of the shard on each node is correct,
	// `shardIndex` is the order in which the shard is created on the node
	CheckShardRange(shardIndex int, start, end []byte)
	// WaitRemovedByShardID check whether the specific shard removed from every node until timeout
	WaitRemovedByShardID(shardID uint64, timeout time.Duration)
	// WaitLeadersByCount check that the number of leaders of the cluster reaches at least the specified value
	// until the timeout
	WaitLeadersByCount(count int, timeout time.Duration)
	// WaitShardByCount check that the number of shard of the cluster reaches at least the specified value
	// until the timeout
	WaitShardByCount(count int, timeout time.Duration)
	// WaitShardByCountPerNode check that the number of shard of each node reaches at least the specified value
	// until the timeout
	WaitShardByCountPerNode(count int, timeout time.Duration)
	// WaitShardSplitByCount check whether the count of shard split reaches a specific value until timeout
	WaitShardSplitByCount(id uint64, count int, timeout time.Duration)
	// WaitShardByCounts check whether the number of shards reaches a specific value until timeout
	WaitShardByCounts(counts []int, timeout time.Duration)
	// WaitShardStateChangedTo check whether the state of shard changes to the specific value until timeout
	WaitShardStateChangedTo(shardID uint64, to metapb.ResourceState, timeout time.Duration)
	// GetShardLeaderStore return the leader node of the shard
	GetShardLeaderStore(shardID uint64) Store
	// GetProphet returns the prophet instance
	GetProphet() prophet.Prophet
	// CreateTestKVClient create and returns a kv client
	CreateTestKVClient(node int) TestKVClient
}

// TestKVClient is a kv client that uses `TestRaftCluster` as Backend's KV storage engine
type TestKVClient interface {
	// Set set key-value to the backend kv storage
	Set(key, value string, timeout time.Duration) error
	// Get returns the value of the specific key from backend kv storage
	Get(key string, timeout time.Duration) (string, error)
	// Close close the test client
	Close()
}

func newTestKVClient(t *testing.T, store Store) TestKVClient {
	kv := &testKVClient{
		errCtx:  make(map[string]chan error),
		doneCtx: make(map[string]chan string),
		runner:  task.NewRunner(),
	}
	kv.proxy = store.GetShardsProxy()
	kv.proxy.SetCallback(kv.done, kv.errorDone)
	return kv
}

type testKVClient struct {
	sync.RWMutex

	id      uint64
	runner  *task.Runner
	proxy   ShardsProxy
	doneCtx map[string]chan string
	errCtx  map[string]chan error
}

func (kv *testKVClient) Set(key, value string, timeout time.Duration) error {
	doneC := make(chan string, 1)
	defer close(doneC)

	errorC := make(chan error, 1)
	defer close(errorC)

	id := kv.nextID()
	kv.addContext(id, doneC, errorC)
	defer kv.clearContext(id)

	req := createTestWriteReq(id, key, value)
	req.StopAt = time.Now().Add(timeout).Unix()
	err := kv.proxy.Dispatch(req)
	if err != nil {
		return err
	}

	select {
	case <-doneC:
		return nil
	case err := <-errorC:
		return err
	case <-time.After(timeout):
		return ErrTimeout
	}
}

func (kv *testKVClient) Get(key string, timeout time.Duration) (string, error) {
	doneC := make(chan string, 1)
	defer close(doneC)

	errorC := make(chan error, 1)
	defer close(errorC)

	id := kv.nextID()
	kv.addContext(id, doneC, errorC)
	defer kv.clearContext(id)

	req := createTestReadReq(id, key)
	req.StopAt = time.Now().Add(timeout).Unix()
	err := kv.proxy.Dispatch(req)
	if err != nil {
		return "", err
	}

	select {
	case v := <-doneC:
		return v, nil
	case err := <-errorC:
		return "", err
	case <-time.After(timeout):
		return "", ErrTimeout
	}
}

func (kv *testKVClient) Close() {
	kv.runner.Stop()
}

func (kv *testKVClient) addContext(id string, c chan string, ec chan error) {
	kv.Lock()
	defer kv.Unlock()

	kv.errCtx[id] = ec
	kv.doneCtx[id] = c
}

func (kv *testKVClient) clearContext(id string) {
	kv.Lock()
	defer kv.Unlock()

	delete(kv.errCtx, id)
	delete(kv.doneCtx, id)
}

func (kv *testKVClient) done(resp rpc.Response) {
	kv.Lock()
	defer kv.Unlock()

	if c, ok := kv.doneCtx[string(resp.ID)]; ok {
		c <- string(resp.Value)
	}
}

func (kv *testKVClient) errorDone(req *rpc.Request, err error) {
	kv.Lock()
	defer kv.Unlock()

	if c, ok := kv.errCtx[string(req.ID)]; ok {
		c <- err
	}
}

func (kv *testKVClient) nextID() string {
	return fmt.Sprintf("%d", atomic.AddUint64(&kv.id, 1))
}

func createTestWriteReq(id, k, v string) rpc.Request {
	wr := simple.NewWriteRequest([]byte(k), []byte(v))

	req := rpc.Request{}
	req.ID = []byte(id)
	req.CustomType = wr.CmdType
	req.Type = rpc.CmdType_Write
	req.Key = wr.Key
	req.Cmd = wr.Cmd
	return req
}

func createTestReadReq(id, k string) rpc.Request {
	rr := simple.NewReadRequest([]byte(k))

	req := rpc.Request{}
	req.ID = []byte(id)
	req.CustomType = rr.CmdType
	req.Type = rpc.CmdType_Read
	req.Key = rr.Key
	return req
}

type testRaftCluster struct {
	sync.RWMutex

	// init fields
	t               *testing.T
	fs              vfs.FS
	initOpts        []TestClusterOption
	baseDataDir     string
	portsRaftAddr   []int
	portsClientAddr []int
	portsRPCAddr    []int
	portsEtcdClient []int
	portsEtcdPeer   []int

	// reset fields
	opts         *testClusterOptions
	stores       []*store
	awares       []*testShardAware
	dataStorages []storage.DataStorage
}

// NewSingleTestClusterStore create test cluster with 1 node
func NewSingleTestClusterStore(t *testing.T, opts ...TestClusterOption) TestRaftCluster {
	return NewTestClusterStore(t, append(opts, WithTestClusterNodeCount(1), WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
		cfg.Prophet.Replication.MaxReplicas = 1
	}))...)
}

// NewTestClusterStore create test cluster using options
func NewTestClusterStore(t *testing.T, opts ...TestClusterOption) TestRaftCluster {
	c := &testRaftCluster{t: t, initOpts: opts}
	c.reset(true, opts...)
	return c
}

func (c *testRaftCluster) reset(init bool, opts ...TestClusterOption) {
	c.opts = newTestClusterOptions()
	c.stores = nil
	c.awares = nil
	c.dataStorages = nil

	for _, opt := range opts {
		opt(c.opts)
	}
	c.opts.adjust()

	if init {
		if c.opts.enableParallelTest {
			c.t.Parallel()
		}

		c.fs = vfs.GetTestFS()
		if c.opts.useDisk {
			c.fs = vfs.Default
		}
		c.baseDataDir = filepath.Join(c.opts.tmpDir, c.t.Name(), fmt.Sprintf("%d", time.Now().Nanosecond()))
		c.portsRaftAddr = testutil.GenTestPorts(c.opts.nodes)
		c.portsClientAddr = testutil.GenTestPorts(c.opts.nodes)
		c.portsRPCAddr = testutil.GenTestPorts(c.opts.nodes)
		c.portsEtcdClient = testutil.GenTestPorts(c.opts.nodes)
		c.portsEtcdPeer = testutil.GenTestPorts(c.opts.nodes)
	}

	if c.opts.disableSchedule {
		pconfig.DefaultSchedulers = nil
	}

	for i := 0; i < c.opts.nodes; i++ {
		cfg := &config.Config{}
		cfg.Logger = log.GetDefaultZapLoggerWithLevel(c.opts.logLevel).WithOptions(zap.OnFatal(zapcore.WriteThenPanic)).With(zap.String("case", c.t.Name()))
		cfg.UseMemoryAsStorage = true
		cfg.FS = c.fs
		cfg.DataPath = fmt.Sprintf("%s/node-%d", c.baseDataDir, i)
		if c.opts.recreate {
			recreateTestTempDir(cfg.FS, cfg.DataPath)
		}

		cfg.RaftAddr = fmt.Sprintf("127.0.0.1:%d", c.portsRaftAddr[i])
		cfg.ClientAddr = fmt.Sprintf("127.0.0.1:%d", c.portsClientAddr[i])
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

		cfg.Prophet.Name = fmt.Sprintf("node-%d", i)
		cfg.Prophet.RPCAddr = fmt.Sprintf("127.0.0.1:%d", c.portsRPCAddr[i])
		cfg.Prophet.Schedule.EnableJointConsensus = true
		if i < 3 {
			cfg.Prophet.StorageNode = true
			if i != 0 {
				cfg.Prophet.EmbedEtcd.Join = fmt.Sprintf("http://127.0.0.1:%d", c.portsEtcdClient[0])
			}
			cfg.Prophet.EmbedEtcd.TickInterval.Duration = time.Millisecond * 30
			cfg.Prophet.EmbedEtcd.ElectionInterval.Duration = time.Millisecond * 150
			cfg.Prophet.EmbedEtcd.ClientUrls = fmt.Sprintf("http://127.0.0.1:%d", c.portsEtcdClient[i])
			cfg.Prophet.EmbedEtcd.PeerUrls = fmt.Sprintf("http://127.0.0.1:%d", c.portsEtcdPeer[i])
		} else {
			cfg.Prophet.StorageNode = false
			cfg.Prophet.ExternalEtcd = []string{
				fmt.Sprintf("http://127.0.0.1:%d", c.portsEtcdClient[0]),
				fmt.Sprintf("http://127.0.0.1:%d", c.portsEtcdClient[1]),
				fmt.Sprintf("http://127.0.0.1:%d", c.portsEtcdClient[2]),
			}
		}

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
			cfg.Raft.TickInterval.Duration = 10 * testFsyncDuration
			cfg.Prophet.EmbedEtcd.TickInterval.Duration = 10 * testFsyncDuration
			cfg.Prophet.EmbedEtcd.ElectionInterval.Duration = 5 * cfg.Prophet.EmbedEtcd.TickInterval.Duration
		}

		if cfg.Storage.DataStorageFactory == nil {
			var dataStorage storage.DataStorage
			var kvs storage.KVStorage
			if c.opts.useDisk {
				c.opts.dataOpts.FS = vfs.NewPebbleFS(cfg.FS)
				s, err := pebble.NewStorage(cfg.FS.PathJoin(cfg.DataPath, "data"), cfg.Logger, c.opts.dataOpts)
				assert.NoError(c.t, err)
				kvs = s
			} else {
				kvs = mem.NewStorage()
			}
			base := kv.NewBaseStorage(kvs, cfg.FS)
			dataStorage = kv.NewKVDataStorage(base, simple.NewSimpleKVExecutor(kvs))

			cfg.Storage.DataStorageFactory = func(group uint64) storage.DataStorage {
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

		c.stores = append(c.stores, s)
		c.awares = append(c.awares, ts)
	}
}

func (c *testRaftCluster) EveryStore(fn func(i int, store Store)) {
	for idx, store := range c.stores {
		fn(idx, store)
	}
}

func (c *testRaftCluster) GetStore(node int) Store {
	return c.stores[node]
}

func (c *testRaftCluster) Start() {
	c.StartWithConcurrent(false)
}

func (c *testRaftCluster) StartWithConcurrent(concurrent bool) {
	var notProphetNodes []int
	var wg sync.WaitGroup
	fn := func(i int) {
		defer wg.Done()

		s := c.stores[i]
		if c.opts.nodeStartFunc != nil {
			c.opts.nodeStartFunc(i, s)
		} else {
			s.Start()
		}
	}

	for i := range c.stores {
		if i > 2 {
			notProphetNodes = append(notProphetNodes, i)
			continue
		}

		wg.Add(1)
		if concurrent {
			go fn(i)
		} else {
			fn(i)
		}
	}
	wg.Wait()

	if len(notProphetNodes) > 0 {
		for _, idx := range notProphetNodes {
			wg.Add(1)
			go fn(idx)
		}

		wg.Wait()
	}
}

func (c *testRaftCluster) Restart() {
	c.RestartWithFunc(nil)
}

func (c *testRaftCluster) RestartWithFunc(beforeStartFunc func()) {
	c.Stop()
	opts := append(c.initOpts, WithTestClusterRecreate(false))
	c.reset(false, opts...)
	if beforeStartFunc != nil {
		beforeStartFunc()
	}
	c.StartWithConcurrent(true)
}

func (c *testRaftCluster) Stop() {
	for _, s := range c.stores {
		s.Stop()
	}

	c.closeStorage()

	for _, s := range c.stores {
		fs := s.cfg.FS
		if fs == nil {
			panic("fs not set")
		}
		vfs.ReportLeakedFD(fs, c.t)
	}
}

func (c *testRaftCluster) closeStorage() {
	for _, s := range c.dataStorages {
		s.Close()
	}
}

func (c *testRaftCluster) GetPRCount(node int) int {
	cnt := 0
	c.stores[node].replicas.Range(func(k, v interface{}) bool {
		cnt++
		return true
	})
	return cnt
}

func (c *testRaftCluster) GetShardByIndex(node int, shardIndex int) Shard {
	return c.awares[node].getShardByIndex(shardIndex)
}

func (c *testRaftCluster) GetShardByID(node int, shardID uint64) Shard {
	return c.awares[node].getShardByID(shardID)
}

func (c *testRaftCluster) CheckShardCount(countPerNode int) {
	for idx := range c.stores {
		assert.Equal(c.t, countPerNode, c.GetPRCount(idx))
	}
}

func (c *testRaftCluster) CheckShardRange(shardIndex int, start, end []byte) {
	for idx := range c.stores {
		shard := c.awares[idx].getShardByIndex(shardIndex)
		assert.Equal(c.t, start, shard.Start)
		assert.Equal(c.t, end, shard.End)
	}
}

func (c *testRaftCluster) WaitRemovedByShardID(shardID uint64, timeout time.Duration) {
	for idx := range c.stores {
		c.awares[idx].waitRemovedByShardID(c.t, shardID, timeout)
	}
}

func (c *testRaftCluster) WaitLeadersByCount(count int, timeout time.Duration) {
	timeoutC := time.After(timeout)
	for {
		select {
		case <-timeoutC:
			assert.FailNowf(c.t, "", "wait leader count %d timeout", count)
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

func (c *testRaftCluster) WaitShardByCount(count int, timeout time.Duration) {
	timeoutC := time.After(timeout)
	for {
		select {
		case <-timeoutC:
			assert.FailNowf(c.t, "", "wait shards count %d of cluster timeout", count)
		default:
			shards := 0
			for idx := range c.stores {
				shards += c.awares[idx].shardCount()
			}
			if shards >= count {
				return
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (c *testRaftCluster) WaitShardByCountPerNode(count int, timeout time.Duration) {
	for idx := range c.stores {
		c.awares[idx].waitByShardCount(c.t, count, timeout)
	}
}

func (c *testRaftCluster) WaitShardSplitByCount(id uint64, count int, timeout time.Duration) {
	for idx := range c.stores {
		c.awares[idx].waitByShardSplitCount(c.t, id, count, timeout)
	}
}

func (c *testRaftCluster) WaitShardByCounts(counts []int, timeout time.Duration) {
	for idx := range c.stores {
		c.awares[idx].waitByShardCount(c.t, counts[idx], timeout)
	}
}

func (c *testRaftCluster) WaitShardStateChangedTo(shardID uint64, to metapb.ResourceState, timeout time.Duration) {
	timeoutC := time.After(timeout)
	for {
		select {
		case <-timeoutC:
			assert.FailNowf(c.t, "", "wait shard state changed to %+v timeout", to)
		default:
			res, err := c.GetProphet().GetStorage().GetResource(shardID)
			assert.NoError(c.t, err)
			if res != nil && res.State() == to {
				return
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (c *testRaftCluster) GetShardLeaderStore(shardID uint64) Store {
	for idx, s := range c.stores {
		if c.awares[idx].isLeader(shardID) {
			return s
		}
	}
	return nil
}

func (c *testRaftCluster) GetProphet() prophet.Prophet {
	return c.stores[0].pd
}

func (c *testRaftCluster) CreateTestKVClient(node int) TestKVClient {
	return newTestKVClient(c.t, c.GetStore(node))
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

type testTransport struct {
	sync.RWMutex
	sendingSnapshotCount uint64
}

func (trans *testTransport) Start() error               { return nil }
func (trans *testTransport) Close() error               { return nil }
func (trans *testTransport) Send(meta.RaftMessage) bool { return true }
func (trans *testTransport) SendingSnapshotCount() uint64 {
	trans.RLock()
	defer trans.RUnlock()
	return trans.sendingSnapshotCount
}

// TestDataBuilder build test data
type TestDataBuilder struct {
}

// NewTestDataBuilder create and return TestDataBuilder
func NewTestDataBuilder() *TestDataBuilder {
	return &TestDataBuilder{}
}

// CreateShard create shard for testing. format:
// id: id
// range: [id, id+1)
// replicasFormat: Voter Format: pid/cid
//                 Learner Format: pid/cid/[l|v], default v
//                 Initial Member: pid/cid/l/[t|f], default f
//                 Use ',' to split multi-replica.
//                 First is current replica
func (b *TestDataBuilder) CreateShard(id uint64, replicasFormater string) Shard {
	var replicas []metapb.Replica
	if replicasFormater != "" {
		values := strings.Split(replicasFormater, ",")
		for _, value := range values {
			fields := strings.Split(value, "/")
			r := metapb.Replica{Role: metapb.ReplicaRole_Voter}
			for idx, field := range fields {
				switch idx {
				case 0:
					r.ID = format.MustParseStringUint64(field)
				case 1:
					r.ContainerID = format.MustParseStringUint64(field)
				case 2:
					if field == "l" {
						r.Role = metapb.ReplicaRole_Learner
					}
				case 3:
					if field == "t" {
						r.InitialMember = true
					}
				default:
					panic(fmt.Sprintf("invalid replcias foramt: %s", replicasFormater))
				}
			}
			replicas = append(replicas, r)
		}
	}

	return Shard{
		ID:       id,
		Start:    format.Uint64ToBytes(id),
		End:      format.Uint64ToBytes(id + 1),
		Replicas: replicas,
	}
}

func newTestStore(t *testing.T) (*store, func()) {
	c := NewSingleTestClusterStore(t).(*testRaftCluster)
	for _, ds := range c.dataStorages {
		_, err := ds.GetInitialStates()
		assert.NoError(t, err)
	}
	return c.GetStore(0).(*store), func() { c.closeStorage() }
}
