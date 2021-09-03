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
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cpebble "github.com/cockroachdb/pebble"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
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

	wrapper      aware.ShardStateAware
	shards       []bhmetapb.Shard
	leaders      map[uint64]bool
	applied      map[uint64]int
	splitedCount map[uint64]int

	removed map[uint64]bhmetapb.Shard
}

func newTestShardAware() *testShardAware {
	return &testShardAware{
		leaders:      make(map[uint64]bool),
		applied:      make(map[uint64]int),
		removed:      make(map[uint64]bhmetapb.Shard),
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

func (ts *testShardAware) Created(shard bhmetapb.Shard) {
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

func (ts *testShardAware) Splited(shard bhmetapb.Shard) {
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

// TestRaftCluster is the test cluster is used to test starting N nodes in a process, and to provide
// the start and stop capabilities of a single node, which is used to test `raftstore` more easily.
type TestRaftCluster interface {
	// EveryStore do fn at every store, it can be used to init some store register
	EveryStore(fn func(i int, store Store))
	// GetStore returns the node store
	GetStore(node int) Store
	// GetWatcher returns event watcher of the node
	GetWatcher(node int) prophet.Watcher
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
	GetShardByIndex(node int, shardIndex int) bhmetapb.Shard
	// GetShardByID returns the shard from the node by shard id
	GetShardByID(node int, shardID uint64) bhmetapb.Shard
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
	// Set write key-value pairs to the `DataStorage` of the node
	Set(node int, key, value []byte)

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
	proxy, err := NewShardsProxyWithStore(store, kv.done, kv.errorDone)
	if err != nil {
		assert.FailNowf(t, "", "createtest kv client failed with %+v", err)
	}
	kv.proxy = proxy
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

func (kv *testKVClient) done(resp *raftcmdpb.Response) {
	kv.Lock()
	defer kv.Unlock()

	if c, ok := kv.doneCtx[string(resp.ID)]; ok {
		c <- string(resp.Value)
	}
}

func (kv *testKVClient) errorDone(req *raftcmdpb.Request, err error) {
	kv.Lock()
	defer kv.Unlock()

	if c, ok := kv.errCtx[string(req.ID)]; ok {
		c <- err
	}
}

func (kv *testKVClient) nextID() string {
	return fmt.Sprintf("%d", atomic.AddUint64(&kv.id, 1))
}

func createTestWriteReq(id, k, v string) *raftcmdpb.Request {
	req := pb.AcquireRequest()
	req.ID = []byte(id)
	req.CustemType = 1
	req.Type = raftcmdpb.CMDType_Write
	req.Key = []byte(k)
	req.Cmd = []byte(v)
	return req
}

func createTestReadReq(id, k string) *raftcmdpb.Request {
	req := pb.AcquireRequest()
	req.ID = []byte(id)
	req.CustemType = 2
	req.Type = raftcmdpb.CMDType_Read
	req.Key = []byte(k)
	return req
}

func sendTestReqs(s Store, timeout time.Duration, waiterC chan string, waiters map[string]string, reqs ...*raftcmdpb.Request) (map[string]*raftcmdpb.RaftCMDResponse, error) {
	if waiters == nil {
		waiters = make(map[string]string)
	}

	resps := make(map[string]*raftcmdpb.RaftCMDResponse)
	c := make(chan *raftcmdpb.RaftCMDResponse, len(reqs))
	defer close(c)

	cb := func(resp *raftcmdpb.RaftCMDResponse) {
		c <- resp
	}

	for _, req := range reqs {
		if v, ok := waiters[string(req.ID)]; ok {
		OUTER:
			for {
				select {
				case <-time.After(timeout):
					return nil, errors.New("timeout error")
				case c := <-waiterC:
					if c == v {
						break OUTER
					}
				}
			}
		}
		err := s.(*store).onRequestWithCB(req, cb)
		if err != nil {
			return nil, err
		}
	}

	for {
		select {
		case <-time.After(timeout):
			return nil, errors.New("timeout error")
		case resp := <-c:
			if len(resp.Responses) <= 1 {
				resps[string(resp.Responses[0].ID)] = resp
			} else {
				for i := range resp.Responses {
					r := &raftcmdpb.RaftCMDResponse{}
					protoc.MustUnmarshal(r, protoc.MustMarshal(resp))
					r.Responses = resp.Responses[i : i+1]
					resps[string(r.Responses[0].ID)] = r
				}
			}

			if len(resps) == len(reqs) {
				return resps, nil
			}
		}
	}
}

type testRaftCluster struct {
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
func NewSingleTestClusterStore(t *testing.T, opts ...TestClusterOption) TestRaftCluster {
	return NewTestClusterStore(t, append(opts, WithTestClusterNodeCount(1), WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
		cfg.Prophet.Replication.MaxReplicas = 1
	}))...)
}

// NewTestClusterStore create test cluster using options
func NewTestClusterStore(t *testing.T, opts ...TestClusterOption) TestRaftCluster {
	c := &testRaftCluster{t: t, initOpts: opts}
	c.reset(opts...)
	return c
}

func (c *testRaftCluster) reset(opts ...TestClusterOption) {
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
		cfg.Prophet.RPCAddr = fmt.Sprintf("127.0.0.1:3000%d", i)
		cfg.Prophet.Schedule.EnableJointConsensus = true
		if i < 3 {
			cfg.Prophet.StorageNode = true
			if i != 0 {
				cfg.Prophet.EmbedEtcd.Join = "http://127.0.0.1:40000"
			}
			cfg.Prophet.EmbedEtcd.TickInterval.Duration = time.Millisecond * 30
			cfg.Prophet.EmbedEtcd.ElectionInterval.Duration = time.Millisecond * 150
			cfg.Prophet.EmbedEtcd.ClientUrls = fmt.Sprintf("http://127.0.0.1:4000%d", i)
			cfg.Prophet.EmbedEtcd.PeerUrls = fmt.Sprintf("http://127.0.0.1:5000%d", i)
		} else {
			cfg.Prophet.StorageNode = false
			cfg.Prophet.ExternalEtcd = []string{"http://127.0.0.1:40000", "http://127.0.0.1:40001", "http://127.0.0.1:40002"}
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

func (c *testRaftCluster) EveryStore(fn func(i int, store Store)) {
	for idx, store := range c.stores {
		fn(idx, store)
	}
}

func (c *testRaftCluster) GetStore(node int) Store {
	return c.stores[node]
}

func (c *testRaftCluster) GetWatcher(node int) prophet.Watcher {
	return c.stores[node].router.(*defaultRouter).watcher
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
	c.reset(opts...)
	if beforeStartFunc != nil {
		beforeStartFunc()
	}
	c.Start()
}

func (c *testRaftCluster) Stop() {
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

func (c *testRaftCluster) GetPRCount(node int) int {
	cnt := 0
	c.stores[node].replicas.Range(func(k, v interface{}) bool {
		cnt++
		return true
	})
	return cnt
}

func (c *testRaftCluster) GetShardByIndex(node int, shardIndex int) bhmetapb.Shard {
	return c.awares[node].getShardByIndex(shardIndex)
}

func (c *testRaftCluster) GetShardByID(node int, shardID uint64) bhmetapb.Shard {
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

func (c *testRaftCluster) Set(node int, key, value []byte) {
	shard := c.GetShardByIndex(node, 0)
	for idx, s := range c.stores {
		c.dataStorages[idx].(storage.KVStorage).Set(key, value)
		pr := s.getPR(shard.ID, false)
		pr.sizeDiffHint += uint64(len(key) + len(value))
	}
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
