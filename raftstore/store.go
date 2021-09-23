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
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/codec"
	"github.com/fagongzi/goetty/codec/length"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"

	"github.com/matrixorigin/matrixcube/aware"
	"github.com/matrixorigin/matrixcube/components/keys"
	"github.com/matrixorigin/matrixcube/components/prophet"
	"github.com/matrixorigin/matrixcube/components/prophet/event"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/errorpb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/snapshot"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/transport"
	"github.com/matrixorigin/matrixcube/util"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

var (
	logger = log.NewLoggerWithPrefix("[raftstore]")
)

// Store manage a set of raft group
type Store interface {
	// Start the raft store
	Start()
	// Stop the raft store
	Stop()
	// GetConfig returns the config of the store
	GetConfig() *config.Config
	// Meta returns store meta
	Meta() meta.Store
	// GetRouter returns a router
	GetRouter() Router
	// RegisterLocalRequestCB register local request cb to process response
	RegisterLocalRequestCB(func(*rpc.ResponseBatchHeader, *rpc.Response))
	// RegisterRPCRequestCB register rpc request cb to process response
	RegisterRPCRequestCB(func(*rpc.ResponseBatchHeader, *rpc.Response))
	// OnRequest receive a request, and call cb while the request is completed
	OnRequest(*rpc.Request) error
	// DataStorage returns a DataStorage of the shard group
	DataStorageByGroup(uint64) storage.DataStorage
	// MaybeLeader returns the shard replica maybe leader
	MaybeLeader(uint64) bool
	// AllocID returns a uint64 id, panic if has a error
	MustAllocID() uint64
	// Prophet return current prophet instance
	Prophet() prophet.Prophet
	// CreateRPCCliendSideCodec returns the rpc codec at client side
	CreateRPCCliendSideCodec() (codec.Encoder, codec.Decoder)

	// CreateResourcePool create resource pools, the resource pool will create shards,
	// and try to maintain the number of shards in the pool not less than the `capacity`
	// parameter. This is an idempotent operation.
	CreateResourcePool(...metapb.ResourcePool) (ShardsPool, error)
	// GetResourcePool returns `ShardsPool`, nil if `CreateResourcePool` not completed
	GetResourcePool() ShardsPool
}

const (
	applyWorkerName      = "apply-%d-%d"
	snapshotWorkerName   = "snapshot-%d"
	splitCheckWorkerName = "split"
)

type store struct {
	cfg *config.Config

	meta       *containerAdapter
	pd         prophet.Prophet
	bootOnce   sync.Once
	pdStartedC chan struct{}

	logdb           logdb.LogDB
	runner          *task.Runner
	trans           transport.Transport
	snapshotManager snapshot.SnapshotManager
	rpc             *defaultRPC
	router          Router
	routerOnce      sync.Once
	keyRanges       sync.Map // group id -> *util.ShardTree
	peers           sync.Map // peer  id -> peer
	replicas        sync.Map // shard id -> *peerReplica
	droppedVoteMsgs sync.Map // shard id -> raftpb.Message

	state    uint32
	stopOnce sync.Once

	localCB func(*rpc.ResponseBatchHeader, *rpc.Response)
	rpcCB   func(*rpc.ResponseBatchHeader, *rpc.Response)

	allocWorkerLock sync.Mutex
	applyWorkers    []map[string]int
	eventWorkers    []map[uint64]int
	workReady       *workReady

	aware aware.ShardStateAware

	// shard pool processor
	shardPool *dynamicShardsPool
}

// NewStore returns a raft store
func NewStore(cfg *config.Config) Store {
	cfg.Adjust()

	s := &store{
		meta:      &containerAdapter{},
		cfg:       cfg,
		logdb:     logdb.NewKVLogDB(cfg.Storage.MetaStorage),
		runner:    task.NewRunner(),
		workReady: newWorkReady(cfg.ShardGroups, cfg.Worker.RaftEventWorkers),
		shardPool: newDynamicShardsPool(cfg),
	}

	if s.cfg.Customize.CustomShardStateAwareFactory != nil {
		s.aware = cfg.Customize.CustomShardStateAwareFactory()
	}

	if s.cfg.Customize.CustomSnapshotManagerFactory != nil {
		s.snapshotManager = s.cfg.Customize.CustomSnapshotManagerFactory()
	} else {
		s.snapshotManager = newDefaultSnapshotManager(s)
	}

	s.rpc = newRPC(s)
	s.initWorkers()
	return s
}

func (s *store) GetConfig() *config.Config {
	return s.cfg
}

func (s *store) Start() {
	logger.Infof("begin start raftstore")

	s.startProphet()
	logger.Infof("prophet started")

	s.startTransport()
	logger.Infof("start listen at %s for raft", s.cfg.RaftAddr)

	s.startRaftWorkers()
	logger.Infof("raft shards workers started")

	s.startShards()
	logger.Infof("shards started")

	s.startTimerTasks()
	logger.Infof("shard timer based tasks started")

	s.startRPC()
	logger.Infof("start listen at %s for client", s.cfg.ClientAddr)

	s.startRouter()
	logger.Infof("router started")

	s.doStoreHeartbeat(time.Now())
}

func (s *store) Stop() {
	atomic.StoreUint32(&s.state, 1)

	s.stopOnce.Do(func() {
		logger.Infof("store %d begin to stop", s.Meta().ID)

		s.pd.Stop()
		logger.Infof("store %d pd stopped", s.Meta().ID)

		s.trans.Stop()
		logger.Infof("store %d transport stopped", s.Meta().ID)

		s.foreachPR(func(pr *replica) bool {
			pr.stopEventLoop()
			return true
		})
		logger.Infof("store %d all shards stopped", s.Meta().ID)

		s.snapshotManager.Close()
		logger.Infof("store %d all snapshot manager stopped", s.Meta().ID)

		s.runner.Stop()
		logger.Infof("store %d task runner stopped", s.Meta().ID)

		s.rpc.Stop()
		logger.Infof("store %d rpc stopped", s.Meta().ID)
	})
}

func (s *store) GetRouter() Router {
	s.startRouter()
	return s.router
}

func (s *store) startRouter() {
	s.routerOnce.Do(func() {
		watcher, err := s.pd.GetClient().NewWatcher(uint32(event.EventFlagAll))
		if err != nil {
			logger.Fatalf("create router failed with %+v", err)
		}

		r, err := newRouter(watcher, s.runner, func(id uint64) {
			s.destoryPR(id, true, "remove by event")
		}, s.doDynamicallyCreate)
		if err != nil {
			logger.Fatalf("create router failed with %+v", err)
		}
		err = r.Start()
		if err != nil {
			logger.Fatalf("start router failed with %+v", err)
		}

		s.router = r
	})
}

func (s *store) Meta() meta.Store {
	return s.meta.meta
}

func (s *store) RegisterLocalRequestCB(cb func(*rpc.ResponseBatchHeader, *rpc.Response)) {
	s.localCB = cb
}

func (s *store) RegisterRPCRequestCB(cb func(*rpc.ResponseBatchHeader, *rpc.Response)) {
	s.rpcCB = cb
}

func (s *store) OnRequest(req *rpc.Request) error {
	return s.onRequestWithCB(req, s.cb)
}

func (s *store) onRequestWithCB(req *rpc.Request, cb func(resp *rpc.ResponseBatch)) error {
	if logger.DebugEnabled() {
		logger.Debugf("%s store received", hex.EncodeToString(req.ID))
	}

	var pr *replica
	var err error
	if req.ToShard > 0 {
		pr = s.getPR(req.ToShard, false)
		if pr == nil {
			respStoreNotMatch(errStoreNotMatch, req, cb)
			return nil
		}
	} else {
		pr, err = s.selectShard(req.Group, req.Key)
		if err != nil {
			if err == errStoreNotMatch {
				respStoreNotMatch(err, req, cb)
				return nil
			}

			return err
		}
	}

	return pr.onReq(req, cb)
}

func (s *store) DataStorageByGroup(group uint64) storage.DataStorage {
	return s.cfg.Storage.DataStorageFactory(group)
}

func (s *store) MaybeLeader(shard uint64) bool {
	return nil != s.getPR(shard, true)
}

func (s *store) cb(resp *rpc.ResponseBatch) {
	for _, rsp := range resp.Responses {
		if rsp.PID != 0 {
			s.rpcCB(resp.Header, rsp)
		} else {
			s.localCB(resp.Header, rsp)
		}
	}

	pb.ReleaseResponseBatch(resp)
}

func (s *store) MustAllocID() uint64 {
	id, err := s.pd.GetClient().AllocID()
	if err != nil {
		logger.Fatalf("alloc id failed with %+v", err)
	}

	return id
}

func (s *store) Prophet() prophet.Prophet {
	return s.pd
}

func (s *store) CreateRPCCliendSideCodec() (codec.Encoder, codec.Decoder) {
	v := &rpcCodec{clientSide: true}
	return length.NewWithSize(v, v, 0, 0, 0, int(s.cfg.Raft.MaxEntryBytes)*2)
}

func (s *store) initWorkers() {
	for g := uint64(0); g < s.cfg.ShardGroups; g++ {
		s.applyWorkers = append(s.applyWorkers, make(map[string]int))

		for i := uint64(0); i < s.cfg.Worker.ApplyWorkerCount; i++ {
			name := fmt.Sprintf(applyWorkerName, g, i)
			s.applyWorkers[g][name] = 0
			s.runner.AddNamedWorker(name)
		}
	}

	for g := uint64(0); g < s.cfg.ShardGroups; g++ {
		name := fmt.Sprintf(snapshotWorkerName, g)
		s.runner.AddNamedWorker(name)
	}

	s.runner.AddNamedWorker(splitCheckWorkerName)
}

func (s *store) startProphet() {
	logger.Infof("begin to start prophet")

	s.cfg.Prophet.Adapter = newProphetAdapter()
	s.cfg.Prophet.Handler = s
	s.cfg.Prophet.Adjust(nil, false)

	s.pdStartedC = make(chan struct{})
	s.pd = prophet.NewProphet(&s.cfg.Prophet)
	s.pd.Start()
	<-s.pdStartedC
	s.shardPool.setProphetClient(s.pd.GetClient())
}

func (s *store) startTransport() {
	if s.cfg.Customize.CustomTransportFactory != nil {
		s.trans = s.cfg.Customize.CustomTransportFactory()
	} else {
		s.trans = transport.NewDefaultTransport(s.Meta().ID,
			s.cfg.RaftAddr,
			s.snapshotManager,
			s.handle,
			s.pd.GetStorage().GetContainer,
			transport.WithMaxBodyBytes(int(s.cfg.Raft.MaxEntryBytes)*2),
			transport.WithTimeout(10*s.cfg.Raft.GetElectionTimeoutDuration(),
				10*s.cfg.Raft.GetElectionTimeoutDuration()),
			transport.WithSendBatch(int64(s.cfg.Raft.SendRaftBatchSize)),
			transport.WithWorkerCount(s.cfg.Worker.SendRaftMsgWorkerCount, s.cfg.Snapshot.MaxConcurrencySnapChunks),
			transport.WithErrorHandler(func(msg *meta.RaftMessage, err error) {
				if pr := s.getPR(msg.ShardID, true); pr != nil {
					pr.addReport(msg.Message)
				}
			}))
	}

	s.trans.Start()
}

func (s *store) startRaftWorkers() {
	var wg sync.WaitGroup
	for i := uint64(0); i < s.cfg.ShardGroups; i++ {
		s.eventWorkers = append(s.eventWorkers, make(map[uint64]int))
		g := i
		for j := uint64(0); j < s.cfg.Worker.RaftEventWorkers; j++ {
			s.eventWorkers[g][j] = 0
			idx := j
			wg.Add(1)
			s.runner.RunCancelableTask(func(ctx context.Context) {
				wg.Done()
				s.runPRTask(ctx, g, idx)
			})
		}
	}
	wg.Wait()
}

func (s *store) runPRTask(ctx context.Context, g, id uint64) {
	logger.Infof("raft worker %d/%d start", g, id)

	run := func() {
		for {
			hasEvent := false
			s.replicas.Range(func(key, value interface{}) bool {
				pr := value.(*replica)
				if pr.eventWorker == id && pr.getShard().Group == g && pr.handleEvent() {
					hasEvent = true
				}

				return true
			})

			if !hasEvent {
				return
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			logger.Infof("raft worker worker %d/%d exit", g, id)
			return
		case <-s.workReady.waitC(g, id):
			run()
		}
	}
}

func (s *store) startShards() {
	totalCount := 0
	tomebstoneCount := 0

	s.cfg.Storage.ForeachDataStorageFunc(func(ds storage.DataStorage) {
		initStates, err := ds.GetInitialStates()
		if err != nil {
			logger.Fatalf("init store failed with %+v", err)
		}

		var tomebstoneShards []meta.Shard
		for _, metadata := range initStates {
			totalCount++
			sls := &meta.ShardLocalState{}
			protoc.MustUnmarshal(sls, metadata.Metadata)

			if sls.Shard.ID != metadata.ShardID {
				logger.Fatalf("BUG: shard id not match in metadata, %d != %d",
					sls.Shard.ID,
					metadata.ShardID)
			}

			if sls.State == meta.PeerState_Tombstone {
				tomebstoneShards = append(tomebstoneShards, sls.Shard)
				tomebstoneCount++
				logger.Infof("shard %d is tombstone in store",
					sls.Shard.ID)
				continue
			}

			pr, err := createPeerReplica(s, &sls.Shard, "bootstrap")
			if err != nil {
				logger.Fatalf("init store failed with %+v", err)
			}

			s.updateShardKeyRange(sls.Shard)
			s.addPR(pr)
			pr.start()
		}

		s.cleanup(tomebstoneShards)
	})

	logger.Infof("starts with %d shards, including %d tombstones shards",
		totalCount,
		tomebstoneCount)

}

func (s *store) startTimerTasks() {
	s.runner.RunCancelableTask(func(ctx context.Context) {
		last := time.Now()

		compactTicker := time.NewTicker(s.cfg.Raft.RaftLog.CompactDuration.Duration)
		defer compactTicker.Stop()

		splitCheckTicker := time.NewTicker(s.cfg.Replication.ShardSplitCheckDuration.Duration)
		defer splitCheckTicker.Stop()

		stateCheckTicker := time.NewTicker(s.cfg.Replication.ShardStateCheckDuration.Duration)
		defer stateCheckTicker.Stop()

		shardLeaderheartbeatTicker := time.NewTicker(s.cfg.Replication.ShardHeartbeatDuration.Duration)
		defer shardLeaderheartbeatTicker.Stop()

		storeheartbeatTicker := time.NewTicker(s.cfg.Replication.StoreHeartbeatDuration.Duration)
		defer storeheartbeatTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				logger.Infof("timer based tasks stopped")
				return
			case <-compactTicker.C:
				s.handleCompactRaftLog()
			case <-splitCheckTicker.C:
				if !s.cfg.Replication.DisableShardSplit {
					s.handleSplitCheck()
				}
			case <-stateCheckTicker.C:
				s.handleShardStateCheck()
			case <-shardLeaderheartbeatTicker.C:
				s.doShardHeartbeat()
			case <-storeheartbeatTicker.C:
				s.doStoreHeartbeat(last)
				last = time.Now()
			}
		}
	})
}

func (s *store) destoryPR(shardID uint64, tombstoneInCluster bool, why string) {
	pr := s.getPR(shardID, false)
	if pr != nil {
		pr.startApplyDestroy(tombstoneInCluster, why)
	}
}

func (s *store) addPR(pr *replica) bool {
	_, loaded := s.replicas.LoadOrStore(pr.shardID, pr)
	return !loaded
}

func (s *store) removePR(pr *replica) {
	s.replicas.Delete(pr.shardID)
	if s.aware != nil {
		s.aware.Destory(pr.getShard())
	}
	s.revokeWorker(pr)
}

func (s *store) startRPC() {
	err := s.rpc.Start()
	if err != nil {
		logger.Fatalf("start RPC at %s failed with %+v",
			s.cfg.ClientAddr,
			err)
	}
}

func (s *store) cleanup(shards []meta.Shard) {
	for _, shard := range shards {
		s.doClearData(shard)
	}

	logger.Infof("cleanup possible garbage data complete")
}

func (s *store) addSnapJob(g uint64, task func() error, cb func(*task.Job)) error {
	return s.addNamedJobWithCB("", fmt.Sprintf(snapshotWorkerName, g), task, cb)
}

func (s *store) addApplyJob(worker string, desc string, task func() error, cb func(*task.Job)) error {
	return s.addNamedJobWithCB(desc, worker, task, cb)
}

func (s *store) addSplitJob(task func() error) error {
	return s.addNamedJob("", splitCheckWorkerName, task)
}

func (s *store) addNamedJob(desc, worker string, task func() error) error {
	return s.runner.RunJobWithNamedWorker(desc, worker, task)
}

func (s *store) addNamedJobWithCB(desc, worker string, task func() error, cb func(*task.Job)) error {
	return s.runner.RunJobWithNamedWorkerWithCB(desc, worker, task, cb)
}

func (s *store) revokeWorker(pr *replica) {
	if pr == nil {
		return
	}

	s.allocWorkerLock.Lock()
	defer s.allocWorkerLock.Unlock()

	g := pr.getShard().Group
	s.applyWorkers[g][pr.applyWorker]--
	s.eventWorkers[g][pr.eventWorker]--
}

func (s *store) allocWorker(g uint64) (string, uint64) {
	s.allocWorkerLock.Lock()
	defer s.allocWorkerLock.Unlock()

	applyWorker := ""
	value := math.MaxInt32
	for name, c := range s.applyWorkers[g] {
		if value > c {
			value = c
			applyWorker = name
		}
	}
	s.applyWorkers[g][applyWorker]++

	raftEventWorker := uint64(0)
	value = math.MaxInt32
	for k, v := range s.eventWorkers[g] {
		if v < value {
			value = v
			raftEventWorker = k
		}
	}

	s.eventWorkers[g][raftEventWorker]++

	return applyWorker, raftEventWorker
}

func (s *store) getPeer(id uint64) (metapb.Peer, bool) {
	value, ok := s.peers.Load(id)
	if !ok {
		return metapb.Peer{}, false
	}

	return value.(metapb.Peer), true
}

func (s *store) foreachPR(consumerFunc func(*replica) bool) {
	s.replicas.Range(func(key, value interface{}) bool {
		return consumerFunc(value.(*replica))
	})
}

func (s *store) getPR(id uint64, mustLeader bool) *replica {
	if value, ok := s.replicas.Load(id); ok {
		pr := value.(*replica)
		if mustLeader && !pr.isLeader() {
			return nil
		}

		return pr
	}

	return nil
}

// In some case, the vote raft msg maybe dropped, so follower node can't response the vote msg
// shard a has 3 peers p1, p2, p3. The p1 split to new shard b
// case 1: in most sence, p1 apply split raft log is before p2 and p3.
//         At this time, if p2, p3 received the shard b's vote msg,
//         and this vote will dropped by p2 and p3 node,
//         because shard a and shard b has overlapped range at p2 and p3 node
// case 2: p2 or p3 apply split log is before p1, we can't mock shard b's vote msg
func (s *store) cacheDroppedVoteMsg(id uint64, msg raftpb.Message) {
	if msg.Type == raftpb.MsgVote || msg.Type == raftpb.MsgPreVote {
		s.droppedVoteMsgs.Store(id, msg)
	}
}

func (s *store) removeDroppedVoteMsg(id uint64) (raftpb.Message, bool) {
	if value, ok := s.droppedVoteMsgs.Load(id); ok {
		s.droppedVoteMsgs.Delete(id)
		return value.(raftpb.Message), true
	}

	return raftpb.Message{}, false
}

func (s *store) validateStoreID(req *rpc.RequestBatch) error {
	if req.Header.Peer.ContainerID != s.meta.meta.ID {
		return fmt.Errorf("store not match, give=<%d> want=<%d>",
			req.Header.Peer.ContainerID,
			s.meta.meta.ID)
	}

	return nil
}

func (s *store) validateShard(req *rpc.RequestBatch) *errorpb.Error {
	shardID := req.Header.ShardID
	peerID := req.Header.Peer.ID

	pr := s.getPR(shardID, false)
	if nil == pr {
		err := new(errorpb.ShardNotFound)
		err.ShardID = shardID
		return &errorpb.Error{
			Message:       errShardNotFound.Error(),
			ShardNotFound: err,
		}
	}

	allowFollow := req.AdminRequest == nil && len(req.Requests) > 0 && req.Requests[0].AllowFollower
	if !allowFollow && !pr.isLeader() {
		err := new(errorpb.NotLeader)
		err.ShardID = shardID
		err.Leader, _ = s.getPeer(pr.getLeaderPeerID())

		return &errorpb.Error{
			Message:   errNotLeader.Error(),
			NotLeader: err,
		}
	}

	if pr.peer.ID != peerID {
		return &errorpb.Error{
			Message: fmt.Sprintf("mismatch peer id, give=<%d> want=<%d>", peerID, pr.peer.ID),
		}
	}

	shard := pr.getShard()
	if !checkEpoch(shard, req) {
		err := new(errorpb.StaleEpoch)
		// Attach the next shard which might be split from the current shard. But it doesn't
		// matter if the next shard is not split from the current shard. If the shard meta
		// received by the KV driver is newer than the meta cached in the driver, the meta is
		// updated.
		newShard := s.nextShard(shard)
		if newShard != nil {
			err.NewShards = append(err.NewShards, *newShard)
		}

		return &errorpb.Error{
			Message:    errStaleEpoch.Error(),
			StaleEpoch: err,
		}
	}

	return nil
}

func checkEpoch(shard meta.Shard, req *rpc.RequestBatch) bool {
	checkVer := false
	checkConfVer := false

	if req.AdminRequest != nil {
		switch req.AdminRequest.CmdType {
		case rpc.AdminCmdType_BatchSplit:
			checkVer = true
		case rpc.AdminCmdType_ConfigChange:
			checkConfVer = true
		case rpc.AdminCmdType_TransferLeader:
			checkVer = true
			checkConfVer = true
		}
	} else {
		// for redis command, we don't care conf version.
		checkVer = true
	}

	if !checkConfVer && !checkVer {
		return true
	}

	if req.Header == nil {
		return false
	}

	fromEpoch := req.Header.Epoch
	lastestEpoch := shard.Epoch

	if req.Header.IgnoreEpochCheck {
		checkVer = false
	}

	if (checkConfVer && fromEpoch.ConfVer < lastestEpoch.ConfVer) ||
		(checkVer && fromEpoch.Version < lastestEpoch.Version) {
		if logger.DebugEnabled() {
			logger.Debugf("shard %d reveiced stale epoch, lastest=<%s> reveived=<%s>",
				shard.ID,
				lastestEpoch.String(),
				fromEpoch.String())
		}
		return false
	}

	return true
}

func newAdminResponseBatch(adminType rpc.AdminCmdType, rsp protoc.PB) *rpc.ResponseBatch {
	adminResp := new(rpc.AdminResponse)
	adminResp.CmdType = adminType

	switch adminType {
	case rpc.AdminCmdType_ConfigChange:
		adminResp.ConfigChange = rsp.(*rpc.ConfigChangeResponse)
	case rpc.AdminCmdType_TransferLeader:
		adminResp.TransferLeader = rsp.(*rpc.TransferLeaderResponse)
	case rpc.AdminCmdType_BatchSplit:
		adminResp.Splits = rsp.(*rpc.BatchSplitResponse)
	}

	resp := pb.AcquireResponseBatch()
	resp.AdminResponse = adminResp
	return resp
}

func (s *store) updateShardKeyRange(shard meta.Shard) {
	if value, ok := s.keyRanges.Load(shard.Group); ok {
		value.(*util.ShardTree).Update(shard)
		return
	}

	tree := util.NewShardTree()
	tree.Update(shard)

	value, loaded := s.keyRanges.LoadOrStore(shard.Group, tree)
	if loaded {
		value.(*util.ShardTree).Update(shard)
	}
}

func (s *store) removeShardKeyRange(shard meta.Shard) bool {
	if value, ok := s.keyRanges.Load(shard.Group); ok {
		return value.(*util.ShardTree).Remove(shard)
	}

	return false
}

func (s *store) selectShard(group uint64, key []byte) (*replica, error) {
	shard := s.searchShard(group, key)
	if shard.ID == 0 {
		return nil, errStoreNotMatch
	}

	pr, ok := s.replicas.Load(shard.ID)
	if !ok {
		return nil, errStoreNotMatch
	}

	return pr.(*replica), nil
}

func (s *store) searchShard(group uint64, key []byte) meta.Shard {
	if value, ok := s.keyRanges.Load(group); ok {
		return value.(*util.ShardTree).Search(key)
	}

	return meta.Shard{}
}

func (s *store) nextShard(shard meta.Shard) *meta.Shard {
	if value, ok := s.keyRanges.Load(shard.Group); ok {
		return value.(*util.ShardTree).NextShard(shard.Start)
	}

	return nil
}

// doClearData Delete all data belong to the shard.
// If return Err, data may get partial deleted.
func (s *store) doClearData(shard meta.Shard) error {
	logger.Infof("shard %d deleting data", shard.ID)
	err := s.removeShardData(shard, nil)
	if err != nil {
		logger.Errorf("shard %d delete data failed with %+v",
			shard.ID,
			err)
	}
	return err
}

func (s *store) startClearDataJob(shard meta.Shard) error {
	return s.addSnapJob(shard.Group, func() error {
		return s.doClearData(shard)
	}, nil)
}

func (s *store) removeShardData(shard meta.Shard, job *task.Job) error {
	if job != nil &&
		job.IsCancelling() {
		return task.ErrJobCancelled
	}

	start := keys.EncStartKey(&shard)
	end := keys.EncEndKey(&shard)
	return s.DataStorageByGroup(shard.Group).RemoveShardData(shard, start, end)
}
