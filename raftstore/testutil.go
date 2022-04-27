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
	"testing"
	"time"

	cpebble "github.com/cockroachdb/pebble"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/aware"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet"
	pconfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	_ "github.com/matrixorigin/matrixcube/components/prophet/schedulers"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/storage/kv/pebble"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/util/stop"
	"github.com/matrixorigin/matrixcube/util/testutil"
	"github.com/matrixorigin/matrixcube/util/uuid"
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
	tmpDir                string
	nodes                 int
	recreate              bool
	adjustConfigFuncs     []func(node int, cfg *config.Config)
	storeFactory          func(node int, cfg *config.Config) Store
	nodeStartFunc         func(node int, store Store)
	logLevel              zapcore.Level
	useDisk               bool
	enableAdvertiseAddr   bool
	dataOpts              *cpebble.Options
	shardCapacityBytes    uint64
	shardSplitCheckBytes  uint64
	disableSchedule       bool
	enableParallelTest    bool
	useProphetInitCluster bool

	storageStatsReaderFunc func(*store) storageStatsReader
}

func newTestClusterOptions() *testClusterOptions {
	return &testClusterOptions{
		logLevel: zap.DebugLevel,
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
	if opts.storageStatsReaderFunc == nil {
		opts.storageStatsReaderFunc = func(s *store) storageStatsReader {
			return &customStorageStatsReader{
				s:            s,
				addShardSize: false,
				capacity:     100 * gb,
				available:    90 * gb,
			}
		}
	}
}

func withStorageStatsReader(storageStatsReaderFunc func(*store) storageStatsReader) TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.storageStatsReaderFunc = storageStatsReaderFunc
	}
}

// WithTestClusterDataPath set data data storage directory
func WithTestClusterDataPath(path string) TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.tmpDir = path
	}
}

// WithTestClusterUseInitProphetCluster set using init prophet cluster config
func WithTestClusterUseInitProphetCluster() TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.useProphetInitCluster = true
	}
}

// WithTestClusterEnableAdvertiseAddr set data data storage directory
func WithTestClusterEnableAdvertiseAddr() TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.enableAdvertiseAddr = true
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

// WithTestClusterSplitPolicy adjust config
func WithTestClusterSplitPolicy(shardCapacityBytes, shardSplitCheckBytes uint64) TestClusterOption {
	return func(opts *testClusterOptions) {
		opts.shardCapacityBytes = shardCapacityBytes
		opts.shardSplitCheckBytes = shardSplitCheckBytes
	}
}

func recreateTestTempDir(fs vfs.FS, tmpDir string) {
	if err := fs.RemoveAll(tmpDir); err != nil {
		panic(err)
	}
	if err := fs.MkdirAll(tmpDir, 0755); err != nil {
		panic(err)
	}
}

type testShardAware struct {
	sync.RWMutex

	node         int
	wrapper      aware.ShardStateAware
	shards       []Shard
	leaders      map[uint64]bool
	applied      map[uint64]int
	splitedCount map[uint64]int

	removed map[uint64]Shard
}

func newTestShardAware(node int) *testShardAware {
	return &testShardAware{
		node:         node,
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
			assert.FailNowf(t, "", "wait shard %d removed at node %d timeout",
				id, ts.node)
		default:
			if ts.shardRemoved(id) {
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
			assert.FailNowf(t, "", "wait shards count %d at node %d timeout, actual %d",
				count,
				ts.node,
				ts.shardCount())
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
			assert.FailNowf(t, "", "wait shard %d split count %d at node %d timeout",
				id,
				count,
				ts.node)
		default:
			if ts.shardSplitedCount(id) >= count {
				return
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (ts *testShardAware) shardRemoved(id uint64) bool {
	ts.RLock()
	defer ts.RUnlock()

	_, ok := ts.removed[id]
	return ok
}

func (ts *testShardAware) getShardByIndex(index int) Shard {
	ts.RLock()
	defer ts.RUnlock()

	return ts.shards[index]
}

func (ts *testShardAware) getShardByID(id uint64) Shard {
	ts.RLock()
	defer ts.RUnlock()
	return ts.getShardByIDLocked(id)
}

func (ts *testShardAware) getShardByIDLocked(id uint64) Shard {
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

func (ts *testShardAware) shardCountByFilter(fn func(Shard) bool) int {
	ts.RLock()
	defer ts.RUnlock()

	n := 0
	for _, s := range ts.shards {
		if fn(s) {
			n++
		}
	}
	return n
}

func (ts *testShardAware) shardCountByGroup(group uint64) int {
	ts.RLock()
	defer ts.RUnlock()

	n := 0
	for _, s := range ts.shards {
		if s.Group == group {
			n++
		}
	}
	return n
}

func (ts *testShardAware) voterShardCount(storeID uint64) int {
	ts.RLock()
	defer ts.RUnlock()

	c := 0
	for _, s := range ts.shards {
		r := findReplica(s, storeID)
		if r != nil && r.Role == metapb.ReplicaRole_Voter {
			c++
		}
	}
	return c
}

func (ts *testShardAware) voterShardCountByFilter(storeID uint64, fn func(Shard) bool) int {
	ts.RLock()
	defer ts.RUnlock()

	c := 0
	for _, s := range ts.shards {
		r := findReplica(s, storeID)
		if r != nil && r.Role == metapb.ReplicaRole_Voter && fn(s) {
			c++
		}
	}
	return c
}

func (ts *testShardAware) voterShardCountByGroup(storeID, groupID uint64) int {
	ts.RLock()
	defer ts.RUnlock()

	c := 0
	for _, s := range ts.shards {
		r := findReplica(s, storeID)
		if r != nil && r.Role == metapb.ReplicaRole_Voter && s.Group == groupID {
			c++
		}
	}
	return c
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

func (ts *testShardAware) leaderCountByFilter(fn func(Shard) bool) int {
	ts.RLock()
	defer ts.RUnlock()

	c := 0
	for sid, ok := range ts.leaders {
		if ok && fn(ts.getShardByIDLocked(sid)) {
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

func (ts *testShardAware) Updated(shard Shard) {
	ts.Lock()
	defer ts.Unlock()

	for idx := range ts.shards {
		if ts.shards[idx].ID == shard.ID {
			ts.shards[idx] = shard
			break
		}
	}

	if ts.wrapper != nil {
		ts.wrapper.Updated(shard)
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
	// GetStoreByID returns the store
	GetStoreByID(id uint64) Store
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
	// StartNode start the node
	StartNode(node int)
	// StopNode stop the node
	StopNode(node int)
	// RestartNode restart the node
	RestartNode(node int)
	// StartNetworkPartition node will in network partition, must call after node started
	StartNetworkPartition(partitions [][]int)
	// StopNetworkPartition stop network partition
	StopNetworkPartition()
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
	// WaitRemovedByShardIDAt check whether the specific shard removed from specific node until timeout
	WaitRemovedByShardIDAt(shardID uint64, nodes []int, timeout time.Duration)
	// WaitLeadersByCount check that the number of leaders of the cluster reaches at least the specified value
	// until the timeout
	WaitLeadersByCount(count int, timeout time.Duration)
	// WaitLeadersByCountsAndShardGroupAndLabel check that the number of leaders of the cluster reaches at least the specified value
	// until the timeout
	WaitLeadersByCountsAndShardGroupAndLabel(counts []int, group uint64, key, value string, timeout time.Duration)
	// WaitShardByCount check that the number of shard of the cluster reaches at least the specified value
	// until the timeout
	WaitShardByCount(count int, timeout time.Duration)
	// WaitShardByLabel check that the shard has the specified label until the timeout
	WaitShardByLabel(id uint64, label, value string, timeout time.Duration)
	// WaitVoterReplicaByCount check that the number of voter shard of the cluster reaches at least the specified value
	// until the timeout
	WaitVoterReplicaByCountPerNode(count int, timeout time.Duration)
	// WaitVoterReplicaByCounts check that the number of voter shard of the cluster reaches at least the specified value
	// until the timeout
	WaitVoterReplicaByCounts(counts []int, timeout time.Duration)
	// WaitVoterReplicaByCountsAndShardGroup check that the number of voter shard of the cluster reaches at least the specified value
	// until the timeout
	WaitVoterReplicaByCountsAndShardGroup(counts []int, shardGroup uint64, timeout time.Duration)
	// WaitVoterReplicaByCountsAndShardGroupAndLabel check that the number of voter shard of the cluster reaches at least the specified value
	// until the timeout
	WaitVoterReplicaByCountsAndShardGroupAndLabel(counts []int, shardGroup uint64, label, value string, timeout time.Duration)
	// WaitShardByCountPerNode check that the number of shard of each node reaches at least the specified value
	// until the timeout
	WaitShardByCountPerNode(count int, timeout time.Duration)
	// WaitAllReplicasChangeToVoter check that the role of shard of each node change to voter until the timeout
	WaitAllReplicasChangeToVoter(shard uint64, timeout time.Duration)
	// WaitShardByCountOnNode check that the number of shard of the specified node reaches at least the specified value
	// until the timeout
	WaitShardByCountOnNode(node, count int, timeout time.Duration)
	// WaitShardSplitByCount check whether the count of shard split reaches a specific value until timeout
	WaitShardSplitByCount(id uint64, count int, timeout time.Duration)
	// WaitShardByCounts check whether the number of shards reaches a specific value until timeout
	WaitShardByCounts(counts []int, timeout time.Duration)
	// WaitShardStateChangedTo check whether the state of shard changes to the specific value until timeout
	WaitShardStateChangedTo(shardID uint64, to metapb.ShardState, timeout time.Duration)
	// GetShardLeaderStore return the leader node of the shard
	GetShardLeaderStore(shardID uint64) Store
	// GetProphet returns the prophet instance
	GetProphet() prophet.Prophet
	// CreateTestKVClient create and returns a kv client
	CreateTestKVClient(node int) TestKVClient
	// CreateTestKVClientWithAdjust create and returns a kv client with adjust func to modify request
	CreateTestKVClientWithAdjust(node int, adjust func(req *rpcpb.Request)) TestKVClient
}

// TestKVClient is a kv client that uses `TestRaftCluster` as Backend's KV storage engine
type TestKVClient interface {
	// Set set key-value to the backend kv storage
	Set(key, value string, timeout time.Duration) error
	// Get returns the value of the specific key from backend kv storage
	Get(key string, timeout time.Duration) (string, error)
	// SetWithShard set key-value to the backend kv storage
	SetWithShard(key, value string, id uint64, timeout time.Duration) error
	// GetWithShard returns the value of the specific key from backend kv storage
	GetWithShard(key string, id uint64, timeout time.Duration) (string, error)
	// UpdateLabel update the shard label
	UpdateLabel(shard, group uint64, key, value string, timeout time.Duration) error
	// Close close the test client
	Close()
}

func newTestKVClient(t *testing.T, store Store, adjust func(req *rpcpb.Request)) TestKVClient {
	kv := &testKVClient{
		errCtx:   make(map[string]chan error),
		doneCtx:  make(map[string]chan string),
		stopper:  stop.NewStopper("test-kv-client"),
		requests: make(map[string]rpcpb.Request),
		adjust:   adjust,
		rc:       newMockRetryController(),
	}
	kv.proxy = store.GetShardsProxy()
	kv.proxy.SetCallback(kv.done, kv.errorDone)
	kv.proxy.SetRetryController(kv)
	return kv
}

type testKVClient struct {
	sync.RWMutex

	stopper  *stop.Stopper
	proxy    ShardsProxy
	doneCtx  map[string]chan string
	errCtx   map[string]chan error
	requests map[string]rpcpb.Request
	adjust   func(req *rpcpb.Request)
	rc       *mockRetryController
}

func (kv *testKVClient) Set(key, value string, timeout time.Duration) error {
	return kv.SetWithShard(key, value, 0, timeout)
}

func (kv *testKVClient) SetWithShard(key, value string, shardID uint64, timeout time.Duration) error {
	doneC := make(chan string, 1)
	defer close(doneC)

	errorC := make(chan error, 1)
	defer close(errorC)

	id := kv.nextID()
	kv.addContext(id, doneC, errorC)
	defer kv.clearContext(id)

	req := createTestWriteReq(id, key, value)
	req.ToShard = shardID

	kv.Lock()
	kv.requests[id] = req
	kv.rc.setRequest(req, timeout)
	kv.Unlock()

	defer func() {
		kv.Lock()
		delete(kv.requests, id)
		kv.Unlock()
	}()

	if kv.adjust != nil {
		kv.adjust(&req)
	}
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

func (kv *testKVClient) UpdateLabel(shard, group uint64, key, value string, timeout time.Duration) error {
	doneC := make(chan string, 1)
	defer close(doneC)

	errorC := make(chan error, 1)
	defer close(errorC)

	id := kv.nextID()
	kv.addContext(id, doneC, errorC)
	defer kv.clearContext(id)

	req := rpcpb.Request{
		ID:         []byte(id),
		Type:       rpcpb.Admin,
		CustomType: uint64(rpcpb.CmdUpdateLabels),
		Group:      group,
		ToShard:    shard,
		Cmd: protoc.MustMarshal(&rpcpb.UpdateLabelsRequest{
			Labels: []metapb.Label{{Key: key, Value: value}},
			Policy: rpcpb.Add,
		}),
	}

	kv.Lock()
	kv.requests[id] = req
	kv.rc.setRequest(req, timeout)
	kv.Unlock()

	defer func() {
		kv.Lock()
		delete(kv.requests, id)
		kv.Unlock()
	}()

	if kv.adjust != nil {
		kv.adjust(&req)
	}
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

func (kv *testKVClient) GetWithShard(key string, shardID uint64, timeout time.Duration) (string, error) {
	doneC := make(chan string, 1)
	defer close(doneC)

	errorC := make(chan error, 1)
	defer close(errorC)

	id := kv.nextID()
	kv.addContext(id, doneC, errorC)
	defer kv.clearContext(id)

	req := createTestReadReq(id, key)
	req.ToShard = shardID

	kv.Lock()
	kv.requests[id] = req
	kv.rc.setRequest(req, timeout)
	kv.Unlock()

	defer func() {
		kv.Lock()
		delete(kv.requests, id)
		kv.Unlock()
	}()

	if kv.adjust != nil {
		kv.adjust(&req)
	}
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

func (kv *testKVClient) Get(key string, timeout time.Duration) (string, error) {
	return kv.GetWithShard(key, 0, timeout)
}

func (kv *testKVClient) Close() {
	kv.stopper.Stop()
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
	kv.rc.deleteRequest(id)
}

func (kv *testKVClient) done(resp rpcpb.Response) {
	kv.Lock()
	defer kv.Unlock()

	if c, ok := kv.doneCtx[string(resp.ID)]; ok {
		if resp.CustomType == uint64(rpcpb.CmdKVGet) {
			var v rpcpb.KVGetResponse
			protoc.MustUnmarshal(&v, resp.Value)
			c <- string(v.Value)
		} else {
			c <- "OK"
		}
	}
}

func (kv *testKVClient) errorDone(requestID []byte, err error) {
	kv.Lock()
	defer kv.Unlock()

	if IsShardUnavailableErr(err) && kv.adjust != nil {
		if req, ok := kv.retryLocked(requestID); ok {
			if err := kv.proxy.Dispatch(req); err != nil {
				panic(err)
			}
			return
		}
	}

	if c, ok := kv.errCtx[string(requestID)]; ok {
		c <- err
	}
}

func (kv *testKVClient) Retry(requestID []byte) (rpcpb.Request, bool) {
	kv.Lock()
	defer kv.Unlock()

	return kv.retryLocked(requestID)
}

func (kv *testKVClient) retryLocked(requestID []byte) (rpcpb.Request, bool) {
	v, ok := kv.requests[string(requestID)]
	if ok && kv.adjust != nil {
		kv.adjust(&v)
	}
	return v, ok
}

func (kv *testKVClient) nextID() string {
	return string(uuid.NewV4().Bytes())
}

func createTestWriteReq(id, k, v string) rpcpb.Request {
	wr := executor.NewWriteRequest([]byte(k), []byte(v))

	req := rpcpb.Request{}
	req.ID = []byte(id)
	req.CustomType = wr.CmdType
	req.Type = rpcpb.Write
	req.Key = wr.Key
	req.Cmd = wr.Cmd
	return req
}

func createTestReadReq(id, k string) rpcpb.Request {
	rr := executor.NewReadRequest([]byte(k))

	req := rpcpb.Request{}
	req.ID = []byte(id)
	req.CustomType = rr.CmdType
	req.Type = rpcpb.Read
	req.Key = rr.Key
	req.Cmd = rr.Cmd
	return req
}

type testRaftCluster struct {
	sync.RWMutex

	networkPartitions [][]uint64

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
	status       []bool
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

func (c *testRaftCluster) StartNetworkPartition(partitions [][]int) {
	c.Lock()
	defer c.Unlock()
	c.networkPartitions = c.networkPartitions[:0]
	for _, nodes := range partitions {
		var partition []uint64
		for _, node := range nodes {
			partition = append(partition, c.stores[node].Meta().ID)
		}
		c.networkPartitions = append(c.networkPartitions, partition)
	}
}

func (c *testRaftCluster) StopNetworkPartition() {
	c.Lock()
	defer c.Unlock()
	c.networkPartitions = c.networkPartitions[:0]
}

func (c *testRaftCluster) transportFilter(msg metapb.RaftMessage) bool {
	c.RLock()
	defer c.RUnlock()

	if len(c.networkPartitions) == 0 {
		return false
	}

	from, to := msg.From.StoreID, msg.To.StoreID
	for _, partition := range c.networkPartitions {
		n := 0
		for _, id := range partition {
			if id == from || id == to {
				n++
			}
		}
		if n > 0 && n == 2 {
			return false
		}
	}

	return true
}

func (c *testRaftCluster) reset(init bool, opts ...TestClusterOption) {
	if init {
		c.opts = newTestClusterOptions()
		for _, opt := range opts {
			opt(c.opts)
		}
		c.opts.adjust()

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

		if c.opts.disableSchedule {
			pconfig.DefaultSchedulers = nil
		}
	}

	c.status = make([]bool, c.opts.nodes)
	c.stores = make([]*store, c.opts.nodes)
	c.awares = make([]*testShardAware, c.opts.nodes)
	c.dataStorages = make([]storage.DataStorage, c.opts.nodes)
	for i := 0; i < c.opts.nodes; i++ {
		c.resetNode(i, init)
	}
}

func (c *testRaftCluster) resetNode(node int, init bool) {
	c.stores[node] = nil
	c.awares[node] = nil
	c.dataStorages[node] = nil

	cfg := &config.Config{}
	cfg.Logger = log.GetDefaultZapLoggerWithLevel(c.opts.logLevel).WithOptions(zap.OnFatal(zapcore.WriteThenPanic)).With(zap.String("case", c.t.Name()))
	cfg.UseMemoryAsStorage = true
	cfg.FS = c.fs
	cfg.DataPath = fmt.Sprintf("%s/node-%d", c.baseDataDir, node)
	if c.opts.recreate && init {
		recreateTestTempDir(cfg.FS, cfg.DataPath)
	}

	listenIP := "127.0.0.1"
	advertiseIP := "127.0.0.1"
	if c.opts.enableAdvertiseAddr {
		listenIP = "0.0.0.0"
	}

	cfg.RaftAddr = fmt.Sprintf("%s:%d", listenIP, c.portsRaftAddr[node])
	cfg.AdvertiseRaftAddr = fmt.Sprintf("%s:%d", advertiseIP, c.portsRaftAddr[node])
	cfg.ClientAddr = fmt.Sprintf("%s:%d", listenIP, c.portsClientAddr[node])
	cfg.AdvertiseClientAddr = fmt.Sprintf("%s:%d", advertiseIP, c.portsClientAddr[node])
	cfg.Labels = append(cfg.Labels, []string{"c", fmt.Sprintf("%d", node)})

	if c.opts.nodes < 3 {
		cfg.Prophet.Replication.MaxReplicas = uint64(c.opts.nodes)
	}

	cfg.Replication.ShardHeartbeatDuration = typeutil.NewDuration(time.Millisecond * 100)
	cfg.Replication.StoreHeartbeatDuration = typeutil.NewDuration(time.Second)
	cfg.Raft.TickInterval = typeutil.NewDuration(time.Millisecond * 100)

	cfg.Prophet.Name = fmt.Sprintf("node-%d", node)
	cfg.Prophet.RPCAddr = fmt.Sprintf("%s:%d", listenIP, c.portsRPCAddr[node])
	cfg.Prophet.AdvertiseRPCAddr = fmt.Sprintf("%s:%d", advertiseIP, c.portsRPCAddr[node])
	cfg.Prophet.Schedule.EnableJointConsensus = true
	if node < 3 {
		cfg.Prophet.ProphetNode = true
		if node != 0 {
			cfg.Prophet.EmbedEtcd.Join = fmt.Sprintf("http://%s:%d", advertiseIP, c.portsEtcdClient[0])
		}
		cfg.Prophet.EmbedEtcd.TickInterval.Duration = time.Millisecond * 30
		cfg.Prophet.EmbedEtcd.ElectionInterval.Duration = time.Millisecond * 150
		cfg.Prophet.EmbedEtcd.ClientUrls = fmt.Sprintf("http://%s:%d", listenIP, c.portsEtcdClient[node])
		cfg.Prophet.EmbedEtcd.AdvertiseClientUrls = fmt.Sprintf("http://%s:%d", advertiseIP, c.portsEtcdClient[node])
		cfg.Prophet.EmbedEtcd.PeerUrls = fmt.Sprintf("http://%s:%d", listenIP, c.portsEtcdPeer[node])
		cfg.Prophet.EmbedEtcd.AdvertisePeerUrls = fmt.Sprintf("http://%s:%d", advertiseIP, c.portsEtcdPeer[node])

		if c.opts.useProphetInitCluster {
			cfg.Prophet.EmbedEtcd.Join = ""
			for i := 0; i < c.opts.nodes; i++ {
				cfg.Prophet.EmbedEtcd.InitialCluster += fmt.Sprintf("node-%d=http://%s:%d,", i, advertiseIP, c.portsEtcdPeer[i])
			}
			cfg.Prophet.EmbedEtcd.InitialCluster = cfg.Prophet.EmbedEtcd.InitialCluster[:len(cfg.Prophet.EmbedEtcd.InitialCluster)-1]
		}
	} else {
		cfg.Prophet.ProphetNode = false
		cfg.Prophet.ExternalEtcd = []string{
			fmt.Sprintf("http://%s:%d", advertiseIP, c.portsEtcdClient[0]),
			fmt.Sprintf("http://%s:%d", advertiseIP, c.portsEtcdClient[1]),
			fmt.Sprintf("http://%s:%d", advertiseIP, c.portsEtcdClient[2]),
		}
	}

	for _, fn := range c.opts.adjustConfigFuncs {
		fn(node, cfg)
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
		dataStorage = kv.NewKVDataStorage(base, executor.NewKVExecutor(kvs),
			kv.WithLogger(cfg.Logger), kv.WithFeature(storage.Feature{
				ShardSplitCheckDuration: time.Millisecond * 100,
				ShardCapacityBytes:      c.opts.shardCapacityBytes,
				ShardSplitCheckBytes:    c.opts.shardSplitCheckBytes,
			}))

		cfg.Storage.DataStorageFactory = func(group uint64) storage.DataStorage {
			return dataStorage
		}
		cfg.Storage.ForeachDataStorageFunc = func(cb func(uint64, storage.DataStorage)) {
			cb(0, dataStorage)
		}
		c.dataStorages[node] = dataStorage
	}

	ts := newTestShardAware(node)
	cfg.Test.ShardStateAware = ts
	cfg.Customize.CustomTransportFilter = c.transportFilter

	var s *store
	if c.opts.storeFactory != nil {
		s = c.opts.storeFactory(node, cfg).(*store)
	} else {
		s = NewStore(cfg).(*store)
	}

	if c.opts.storageStatsReaderFunc != nil {
		s.storageStatsReader = c.opts.storageStatsReaderFunc(s)
	}

	c.stores[node] = s
	c.awares[node] = ts
}

func (c *testRaftCluster) EveryStore(fn func(i int, store Store)) {
	for idx, store := range c.stores {
		fn(idx, store)
	}
}

func (c *testRaftCluster) GetStore(node int) Store {
	return c.stores[node]
}

func (c *testRaftCluster) GetStoreByID(id uint64) Store {
	for _, s := range c.stores {
		if s.Meta().ID == id {
			return s
		}
	}

	return nil
}

func (c *testRaftCluster) Start() {
	c.StartWithConcurrent(c.opts.useProphetInitCluster)
}

func (c *testRaftCluster) StartWithConcurrent(concurrent bool) {
	var notProphetNodes []int
	var wg sync.WaitGroup

	fn := func(i int) {
		c.StartNode(i)
		wg.Done()
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
	c.stop(false)
	c.reset(false)
	if beforeStartFunc != nil {
		beforeStartFunc()
	}
	c.StartWithConcurrent(true)
}

func (c *testRaftCluster) StartNode(node int) {
	s := c.stores[node]
	if c.opts.nodeStartFunc != nil {
		c.opts.nodeStartFunc(node, s)
	} else {
		s.Start()
	}
	c.status[node] = true
}

func (c *testRaftCluster) StopNode(node int) {
	c.stores[node].Stop()
	s := c.dataStorages[node]
	if s != nil {
		s.Close()
	}
	c.status[node] = false
}

func (c *testRaftCluster) RestartNode(node int) {
	if c.status[node] {
		c.StopNode(node)
	}
	c.resetNode(node, false)
	c.StartNode(node)
}

func (c *testRaftCluster) Stop() {
	c.stop(true)
}

func (c *testRaftCluster) stop(clean bool) {
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

	if clean {
		c.removeDataIfSucceed()
	}
}

func (c *testRaftCluster) removeDataIfSucceed() {
	if !c.t.Failed() {
		assert.NoError(c.t, c.fs.RemoveAll(c.baseDataDir))
	}
}

func (c *testRaftCluster) closeLogDBKVStorage() {
	for _, s := range c.stores {
		if s != nil && s.kvStorage != nil {
			s.kvStorage.Close()
		}
	}
}

func (c *testRaftCluster) closeStorage() {
	for _, s := range c.dataStorages {
		if s != nil {
			s.Close()
		}
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

func (c *testRaftCluster) WaitRemovedByShardIDAt(shardID uint64, nodes []int, timeout time.Duration) {
	for _, node := range nodes {
		c.awares[node].waitRemovedByShardID(c.t, shardID, timeout)
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

func (c *testRaftCluster) WaitLeadersByCountsAndShardGroupAndLabel(counts []int, group uint64, key, value string, timeout time.Duration) {

	timeoutC := time.After(timeout)
	for {
		select {
		case <-timeoutC:
			assert.FailNowf(c.t, "", "wait leader by counts %+v of cluster timeout", counts)
		default:
			ok := true
			for idx := range c.stores {
				ok = c.awares[idx].leaderCountByFilter(func(s Shard) bool {
					return s.Group == group && hasLabel(s, key, value)
				}) == counts[idx]
				if !ok {
					break
				}
			}

			if ok {
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

func (c *testRaftCluster) WaitShardByLabel(id uint64, label, value string, timeout time.Duration) {
	timeoutC := time.After(timeout)
	for {
		select {
		case <-timeoutC:
			assert.FailNowf(c.t, "", "wait shard %d label %s %s of cluster timeout", id, label, value)
		default:
			ok := false
			for idx := range c.stores {
				ok = false
				for _, l := range c.awares[idx].getShardByID(id).Labels {
					if l.Key == label && l.Value == value {
						ok = true
					}
				}
			}
			if ok {
				return
			}
			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (c *testRaftCluster) WaitVoterReplicaByCountPerNode(count int, timeout time.Duration) {
	timeoutC := time.After(timeout)
	for {
		select {
		case <-timeoutC:
			assert.FailNowf(c.t, "", "wait voter count %d of cluster timeout", count)
		default:
			ok := true
			for idx, s := range c.stores {
				ok = c.awares[idx].voterShardCount(s.Meta().ID) == count && c.awares[idx].shardCount() == count
				if !ok {
					break
				}
			}

			if ok {
				return
			}

			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (c *testRaftCluster) WaitVoterReplicaByCounts(counts []int, timeout time.Duration) {
	timeoutC := time.After(timeout)
	for {
		select {
		case <-timeoutC:
			assert.FailNowf(c.t, "", "wait voter count %+v of cluster timeout", counts)
		default:
			ok := true
			for idx, s := range c.stores {
				ok = c.awares[idx].voterShardCount(s.Meta().ID) == counts[idx] && c.awares[idx].shardCount() == counts[idx]
				if !ok {
					break
				}
			}

			if ok {
				return
			}

			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (c *testRaftCluster) WaitVoterReplicaByCountsAndShardGroup(counts []int, shardGroup uint64, timeout time.Duration) {
	timeoutC := time.After(timeout)
	for {
		select {
		case <-timeoutC:
			assert.FailNowf(c.t, "", "wait voter count %+v of cluster timeout", counts)
		default:
			ok := true
			for idx, s := range c.stores {
				ok = c.awares[idx].voterShardCountByGroup(s.Meta().ID, shardGroup) == counts[idx] &&
					c.awares[idx].shardCountByGroup(shardGroup) == counts[idx]
				if !ok {
					break
				}
			}

			if ok {
				return
			}

			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (c *testRaftCluster) WaitVoterReplicaByCountsAndShardGroupAndLabel(counts []int, shardGroup uint64, key, value string, timeout time.Duration) {
	timeoutC := time.After(timeout)
	fn := func(s Shard) bool {
		return s.Group == shardGroup && hasLabel(s, key, value)
	}

	for {
		select {
		case <-timeoutC:
			assert.FailNowf(c.t, "", "wait voter count %+v of cluster timeout", counts)
		default:
			ok := true
			for idx, s := range c.stores {
				ok = c.awares[idx].voterShardCountByFilter(s.Meta().ID, fn) == counts[idx] &&
					c.awares[idx].shardCountByFilter(fn) == counts[idx]
				if !ok {
					break
				}
			}

			if ok {
				return
			}

			time.Sleep(time.Millisecond * 100)
		}
	}
}

func (c *testRaftCluster) WaitAllReplicasChangeToVoter(shardID uint64, timeout time.Duration) {
	timeoutC := time.After(timeout)
	for {
		select {
		case <-timeoutC:
			assert.FailNowf(c.t, "", "wait replica of shard %d change to voter timeout", shardID)
		default:
			n := 0
			for idx := range c.stores {
				r := findReplica(c.awares[idx].getShardByID(shardID),
					c.stores[idx].Meta().ID)
				if r != nil && r.Role == metapb.ReplicaRole_Voter {
					n++
				}
			}
			if n == int(c.stores[0].cfg.Prophet.Replication.MaxReplicas) {
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

func (c *testRaftCluster) WaitShardByCountOnNode(node, count int, timeout time.Duration) {
	c.awares[node].waitByShardCount(c.t, count, timeout)
}

func (c *testRaftCluster) WaitShardSplitByCount(id uint64, count int, timeout time.Duration) {
	for idx := range c.stores {
		c.awares[idx].waitByShardSplitCount(c.t, id, count, timeout)
	}
}

func (c *testRaftCluster) WaitShardByCounts(counts []int, timeout time.Duration) {
	for idx, n := range counts {
		c.awares[idx].waitByShardCount(c.t, n, timeout)
	}
}

func (c *testRaftCluster) WaitShardStateChangedTo(shardID uint64, to metapb.ShardState, timeout time.Duration) {
	timeoutC := time.After(timeout)
	for {
		select {
		case <-timeoutC:
			assert.FailNowf(c.t, "", "wait shard state changed to %+v timeout", to)
		default:
			res, err := c.GetProphet().GetStorage().GetShard(shardID)
			if err == nil && res != nil && res.GetState() == to {
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
	return c.CreateTestKVClientWithAdjust(node, nil)
}

func (c *testRaftCluster) CreateTestKVClientWithAdjust(node int, adjust func(req *rpcpb.Request)) TestKVClient {
	return newTestKVClient(c.t, c.GetStore(node), adjust)
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
					r.StoreID = format.MustParseStringUint64(field)
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
	return c.GetStore(0).(*store), func() {
		c.closeStorage()
		c.closeLogDBKVStorage()
		c.removeDataIfSucceed()
	}
}

func hasLabel(s Shard, key, value string) bool {
	for _, l := range s.Labels {
		if l.Key == key && l.Value == value {
			return true
		}
	}
	return false
}

var (
	gb = uint64(1 << 30)
)

type customStorageStatsReader struct {
	sync.RWMutex

	s            *store
	addShardSize bool
	capacity     uint64
	available    uint64
}

func (s *customStorageStatsReader) setStatsWithGB(capacity, available uint64) {
	s.Lock()
	defer s.Unlock()
	s.capacity = capacity * gb
	s.available = available * gb
}

func (s *customStorageStatsReader) stats() (storageStats, error) {
	s.RLock()
	defer s.RUnlock()

	used := uint64(0)
	if s.addShardSize {
		used += s.s.getReplicaCount() * gb
	}

	return storageStats{
		capacity:  s.capacity,
		available: s.available - used,
		usedSize:  s.capacity - s.available + used,
	}, nil
}

type mockRetryController struct {
	sync.Mutex
	stopAt   map[string]int64
	requests map[string]rpcpb.Request
}

func newMockRetryController() *mockRetryController {
	return &mockRetryController{
		stopAt:   make(map[string]int64),
		requests: make(map[string]rpcpb.Request),
	}
}

func (c *mockRetryController) setRequest(request rpcpb.Request, timeout time.Duration) {
	c.Lock()
	defer c.Unlock()
	c.requests[string(request.ID)] = request
	c.stopAt[string(request.ID)] = time.Now().Add(timeout).Unix()
}

func (c *mockRetryController) deleteRequest(id string) {
	c.Lock()
	defer c.Unlock()
	delete(c.requests, id)
	delete(c.stopAt, id)
}

func (c *mockRetryController) Retry(requestID []byte) (rpcpb.Request, bool) {
	c.Lock()
	defer c.Unlock()

	v, ok := c.stopAt[string(requestID)]
	if !ok || time.Now().Unix() >= v {
		return rpcpb.Request{}, false
	}

	return c.requests[string(requestID)], true
}
