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
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
	"github.com/lni/goutils/syncutil"
	"github.com/matrixorigin/matrixcube/aware"
	"github.com/matrixorigin/matrixcube/components/keys"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet"
	"github.com/matrixorigin/matrixcube/components/prophet/event"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/pb/errorpb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/snapshot"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/transport"
	"github.com/matrixorigin/matrixcube/util"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
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
	// GetShardsProxy get shards proxy to dispatch requests
	GetShardsProxy() ShardsProxy
	// OnRequest receive a request, and call cb while the request is completed
	OnRequest(rpc.Request) error
	// DataStorage returns a DataStorage of the shard group
	DataStorageByGroup(uint64) storage.DataStorage
	// MaybeLeader returns the shard replica maybe leader
	MaybeLeader(uint64) bool
	// AllocID returns a uint64 id, panic if has a error
	MustAllocID() uint64
	// Prophet return current prophet instance
	Prophet() prophet.Prophet

	// CreateResourcePool create resource pools, the resource pool will create shards,
	// and try to maintain the number of shards in the pool not less than the `capacity`
	// parameter. This is an idempotent operation.
	CreateResourcePool(...metapb.ResourcePool) (ShardsPool, error)
	// GetResourcePool returns `ShardsPool`, nil if `CreateResourcePool` not completed
	GetResourcePool() ShardsPool
}

type store struct {
	cfg    *config.Config
	logger *zap.Logger

	meta       *containerAdapter
	pd         prophet.Prophet
	bootOnce   sync.Once
	pdStartedC chan struct{}

	logdb           logdb.LogDB
	trans           transport.Transport
	snapshotManager snapshot.SnapshotManager
	shardsProxy     ShardsProxy
	router          Router
	watcher         prophet.Watcher
	keyRanges       sync.Map // group id -> *util.ShardTree
	replicaRecords  sync.Map // peer  id -> metapb.Replica
	replicas        sync.Map // shard id -> *peerReplica
	droppedVoteMsgs sync.Map // shard id -> raftpb.Message

	state    uint32
	stopOnce sync.Once

	aware   aware.ShardStateAware
	stopper *syncutil.Stopper
	// the worker pool used to drive all replicas
	workerPool *workerPool
	// shard pool processor
	shardPool *dynamicShardsPool
}

// NewStore returns a raft store
func NewStore(cfg *config.Config) Store {
	cfg.Adjust()

	s := &store{
		meta:    &containerAdapter{},
		cfg:     cfg,
		logger:  cfg.Logger.Named("store").With(zap.String("store", cfg.Prophet.Name)),
		logdb:   logdb.NewKVLogDB(cfg.Storage.MetaStorage),
		stopper: syncutil.NewStopper(),
	}
	// TODO: make workerCount configurable
	s.workerPool = newWorkerPool(s.logger, &storeReplicaLoader{s}, 64)
	s.shardPool = newDynamicShardsPool(cfg, s.logger)

	if s.cfg.Customize.CustomShardStateAwareFactory != nil {
		s.aware = cfg.Customize.CustomShardStateAwareFactory()
	}

	if s.cfg.Customize.CustomSnapshotManagerFactory != nil {
		s.snapshotManager = s.cfg.Customize.CustomSnapshotManagerFactory()
	} else {
		s.snapshotManager = newDefaultSnapshotManager(s)
	}

	return s
}

func (s *store) GetConfig() *config.Config {
	return s.cfg
}

func (s *store) Start() {
	s.logger.Info("begin to start raftstore")
	s.startProphet()
	s.logger.Info("prophet started",
		s.storeField())

	s.startTransport()
	s.logger.Info("raft internal transport started",
		s.storeField(),
		log.ListenAddressField(s.cfg.RaftAddr))

	s.startShards()
	s.logger.Info("shards started",
		s.storeField())

	s.startTimerTasks()
	s.logger.Info("shard timer based tasks started",
		s.storeField())

	s.startRouter()
	s.logger.Info("router started",
		s.storeField())

	s.startShardsProxy()
	s.logger.Info("proxy started",
		s.storeField(),
		log.ListenAddressField(s.cfg.ClientAddr))

	s.doStoreHeartbeat(time.Now())
}

func (s *store) Stop() {
	atomic.StoreUint32(&s.state, 1)

	s.stopOnce.Do(func() {
		s.logger.Info("begin to stop raftstore",
			s.storeField())
		s.pd.Stop()
		s.logger.Info("pd stopped",
			s.storeField())

		s.trans.Stop()
		s.logger.Info("raft internal transport stopped",
			s.storeField())

		s.forEachReplica(func(pr *replica) bool {
			pr.stopEventLoop()
			return true
		})
		s.logger.Info("shards stopped",
			s.storeField())

		s.workerPool.close()
		s.logger.Info("worker pool stopped",
			s.storeField())

		s.snapshotManager.Close()
		s.logger.Info("snapshot manager stopped",
			s.storeField())

		s.stopper.Stop()
		s.logger.Info("stoppers topped",
			s.storeField())

		s.shardsProxy.Stop()
		s.logger.Info("proxy stopped",
			s.storeField())
	})
}

func (s *store) GetShardsProxy() ShardsProxy {
	return s.shardsProxy
}

func (s *store) GetRouter() Router {
	return s.router
}

func (s *store) startRouter() {
	watcher, err := s.pd.GetClient().NewWatcher(uint32(event.EventFlagAll))
	if err != nil {
		s.logger.Fatal("fail to create router",
			s.storeField(),
			zap.Error(err))
	}
	r, err := newRouterBuilder().withLogger(s.logger).withStopper(s.stopper).withCreatShardHandle(s.doDynamicallyCreate).withRemoveShardHandle(func(id uint64) {
		s.destroyReplica(id, true, "remove by event")
	}).build(watcher.GetNotify())
	if err != nil {
		s.logger.Fatal("fail to create router",
			s.storeField(),
			zap.Error(err))
	}
	err = r.Start()
	if err != nil {
		s.logger.Fatal("fail to start router",
			s.storeField(),
			zap.Error(err))
	}

	s.router = r
	s.watcher = watcher
}

func (s *store) Meta() meta.Store {
	return s.meta.meta
}

func (s *store) OnRequest(req rpc.Request) error {
	return s.onRequestWithCB(req, s.shardsProxy.OnResponse)
}

func (s *store) onRequestWithCB(req rpc.Request, cb func(resp rpc.ResponseBatch)) error {
	if ce := s.logger.Check(zap.DebugLevel, "receive request"); ce != nil {
		ce.Write(log.RequestIDField(req.ID),
			s.storeField())
	}

	var pr *replica
	var err error
	if req.ToShard > 0 {
		pr = s.getReplica(req.ToShard, false)
		if pr == nil {
			if ce := s.logger.Check(zap.DebugLevel, "fail to handle request"); ce != nil {
				ce.Write(log.RequestIDField(req.ID),
					s.storeField(),
					log.ShardIDField(req.ToShard),
					log.ReasonField("shard not found"))
			}
			respStoreNotMatch(errStoreNotMatch, req, cb)
			return nil
		}
	} else {
		pr, err = s.selectShard(req.Group, req.Key)
		if err != nil {
			if ce := s.logger.Check(zap.DebugLevel, "fail to handle request"); ce != nil {
				ce.Write(log.RequestIDField(req.ID),
					s.storeField(),
					log.HexField("key", req.Key),
					log.ReasonField("key not match"))
			}

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
	return nil != s.getReplica(shard, true)
}

func (s *store) MustAllocID() uint64 {
	id, err := s.pd.GetClient().AllocID()
	if err != nil {
		s.logger.Fatal("fail to alloc id",
			s.storeField(),
			zap.Error(err))
	}

	return id
}

func (s *store) Prophet() prophet.Prophet {
	return s.pd
}

func (s *store) startProphet() {
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
			transport.WithErrorHandler(func(msg meta.RaftMessage, err error) {
				if pr := s.getReplica(msg.ShardID, true); pr != nil {
					pr.addFeedback(msg.Message)
				}
			}))
	}

	s.trans.Start()
}

func (s *store) startShards() {
	totalCount := 0
	tomebstoneCount := 0

	s.cfg.Storage.ForeachDataStorageFunc(func(ds storage.DataStorage) {
		initStates, err := ds.GetInitialStates()
		if err != nil {
			s.logger.Fatal("fail to get initial state",
				s.storeField(),
				zap.Error(err))
		}

		var tomebstoneShards []Shard
		for _, metadata := range initStates {
			totalCount++
			sls := &meta.ShardLocalState{}
			protoc.MustUnmarshal(sls, metadata.Metadata)

			if sls.Shard.ID != metadata.ShardID {
				s.logger.Fatal("BUG: shard id not match in metadata",
					s.storeField(),
					zap.Uint64("expect", sls.Shard.ID),
					zap.Uint64("actual", metadata.ShardID))
			}

			if sls.State == meta.ReplicaState_Tombstone {
				tomebstoneShards = append(tomebstoneShards, sls.Shard)
				tomebstoneCount++
				s.logger.Info("shard is tombstone in store",
					s.storeField(),
					log.ShardIDField(sls.Shard.ID))
				continue
			}

			pr, err := createReplica(s, &sls.Shard, "bootstrap")
			if err != nil {
				s.logger.Fatal("fail to create replica",
					s.storeField(),
					zap.Error(err))
			}

			s.updateShardKeyRange(sls.Shard)
			s.addReplica(pr)
			pr.start()
		}

		s.cleanup(tomebstoneShards)
	})

	s.logger.Info("shards started",
		s.storeField(),
		zap.Int("total", totalCount),
		zap.Int("tomebstone", tomebstoneCount))
}

func (s *store) startTimerTasks() {
	s.stopper.RunWorker(func() {
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
			case <-s.stopper.ShouldStop():
				s.logger.Info("timer based tasks stopped",
					s.storeField())
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

func (s *store) destroyReplica(shardID uint64, tombstoneInCluster bool, why string) {
	if replica := s.getReplica(shardID, false); replica != nil {
		replica.startApplyDestroy(tombstoneInCluster, why)
	}
}

func (s *store) addReplica(pr *replica) bool {
	_, loaded := s.replicas.LoadOrStore(pr.shardID, pr)
	return !loaded
}

func (s *store) removeReplica(pr *replica) {
	s.replicas.Delete(pr.shardID)
	if s.aware != nil {
		s.aware.Destory(pr.getShard())
	}
}

func (s *store) startShardsProxy() {
	maxBodySize := int(s.cfg.Raft.MaxEntryBytes) * 2

	rpc := newProxyRPC(s.logger.Named("proxy.rpc").With(s.storeField()),
		s.Meta().ClientAddr,
		maxBodySize,
		s.OnRequest)

	l := s.logger.Named("proxy").With(s.storeField())
	sp, err := newShardsProxyBuilder().
		withLogger(l).
		withBackendFactory(newBackendFactory(l, s)).
		withMaxBodySize(maxBodySize).
		withRPC(rpc).
		build(s.router)
	if err != nil {
		s.logger.Fatal("fail to create shards proxy", zap.Error(err))
	}

	s.shardsProxy = sp

	err = s.shardsProxy.Start()
	if err != nil {
		s.logger.Fatal("fail to start shards proxy",
			s.storeField(),
			log.ListenAddressField(s.cfg.ClientAddr),
			zap.Error(err))
	}
}

func (s *store) cleanup(shards []Shard) {
	for _, shard := range shards {
		s.doClearData(shard)
	}

	s.logger.Info("cleanup possible garbage data complete",
		s.storeField())
}

func (s *store) getReplicaRecord(id uint64) (Replica, bool) {
	value, ok := s.replicaRecords.Load(id)
	if !ok {
		return Replica{}, false
	}

	return value.(Replica), true
}

func (s *store) forEachReplica(consumerFunc func(*replica) bool) {
	s.replicas.Range(func(key, value interface{}) bool {
		return consumerFunc(value.(*replica))
	})
}

func (s *store) getReplica(id uint64, mustLeader bool) *replica {
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

func (s *store) validateStoreID(req rpc.RequestBatch) error {
	if req.Header.Replica.ContainerID != s.meta.meta.ID {
		return fmt.Errorf("store not match, give=<%d> want=<%d>",
			req.Header.Replica.ContainerID,
			s.meta.meta.ID)
	}

	return nil
}

func (s *store) validateShard(req rpc.RequestBatch) (errorpb.Error, bool) {
	shardID := req.Header.ShardID
	peerID := req.Header.Replica.ID

	pr := s.getReplica(shardID, false)
	if nil == pr {
		err := new(errorpb.ShardNotFound)
		err.ShardID = shardID
		return errorpb.Error{
			Message:       errShardNotFound.Error(),
			ShardNotFound: err,
		}, true
	}

	allowFollow := len(req.Requests) > 0 && req.Requests[0].AllowFollower
	if !allowFollow && !pr.isLeader() {
		err := new(errorpb.NotLeader)
		err.ShardID = shardID
		err.Leader, _ = s.getReplicaRecord(pr.getLeaderPeerID())

		return errorpb.Error{
			Message:   errNotLeader.Error(),
			NotLeader: err,
		}, true
	}

	if pr.replica.ID != peerID {
		return errorpb.Error{
			Message: fmt.Sprintf("mismatch peer id, give=<%d> want=<%d>", peerID, pr.replica.ID),
		}, true
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

		return errorpb.Error{
			Message:    errStaleEpoch.Error(),
			StaleEpoch: err,
		}, true
	}

	return errorpb.Error{}, false
}

func checkEpoch(shard Shard, req rpc.RequestBatch) bool {
	checkVer := false
	checkConfVer := false

	if req.IsAdmin() {
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

	if req.Header.IsEmpty() {
		return false
	}

	fromEpoch := req.Header.Epoch
	lastestEpoch := shard.Epoch

	if req.Header.IgnoreEpochCheck {
		checkVer = false
	}

	if (checkConfVer && fromEpoch.ConfVer < lastestEpoch.ConfVer) ||
		(checkVer && fromEpoch.Version < lastestEpoch.Version) {
		return false
	}

	return true
}

func newAdminResponseBatch(adminType rpc.AdminCmdType, rsp protoc.PB) rpc.ResponseBatch {
	adminResp := rpc.AdminResponse{}
	adminResp.CmdType = adminType

	switch adminType {
	case rpc.AdminCmdType_ConfigChange:
		adminResp.ConfigChange = rsp.(*rpc.ConfigChangeResponse)
	case rpc.AdminCmdType_TransferLeader:
		adminResp.TransferLeader = rsp.(*rpc.TransferLeaderResponse)
	case rpc.AdminCmdType_BatchSplit:
		adminResp.Splits = rsp.(*rpc.BatchSplitResponse)
	}

	resp := rpc.ResponseBatch{}
	resp.AdminResponse = adminResp
	return resp
}

func (s *store) updateShardKeyRange(shard Shard) {
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

func (s *store) removeShardKeyRange(shard Shard) bool {
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

func (s *store) searchShard(group uint64, key []byte) Shard {
	if value, ok := s.keyRanges.Load(group); ok {
		return value.(*util.ShardTree).Search(key)
	}

	return Shard{}
}

func (s *store) nextShard(shard Shard) *Shard {
	if value, ok := s.keyRanges.Load(shard.Group); ok {
		return value.(*util.ShardTree).NextShard(shard.Start)
	}

	return nil
}

// doClearData Delete all data belong to the shard.
// If return Err, data may get partial deleted.
func (s *store) doClearData(shard Shard) error {
	s.logger.Info("deleting shard data",
		s.storeField(),
		log.ShardIDField(shard.ID))
	err := s.removeShardData(shard, nil)
	if err != nil {
		s.logger.Fatal("fail to delete shard data",
			s.storeField(),
			log.ShardIDField(shard.ID),
			zap.Error(err))
	}
	return err
}

// FIXME: move this to a suitable thread
// FIXME: this should only be called when the replica is fully unloaded
func (s *store) startClearDataJob(shard Shard) error {
	return s.doClearData(shard)
}

func (s *store) removeShardData(shard Shard, job *task.Job) error {
	if job != nil &&
		job.IsCancelling() {
		return task.ErrJobCancelled
	}

	start := keys.EncStartKey(&shard)
	end := keys.EncEndKey(&shard)
	return s.DataStorageByGroup(shard.Group).RemoveShardData(shard, start, end)
}

func (s *store) storeField() zap.Field {
	return log.StoreIDField(s.Meta().ID)
}
