package raftstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/errorpb"
	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/pb/raftpb"
	"github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/prophet"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
)

var (
	eventsPath = "/events/shards"
)

// ShardStateAware shard state aware
type ShardStateAware interface {
	// Created the shard was created on the current store
	Created(metapb.Shard)
	// Splited the shard was splited on the current store
	Splited(metapb.Shard)
	// Destory the shard was destoryed on the current store
	Destory(metapb.Shard)
	// BecomeLeader the shard was become leader on the current store
	BecomeLeader(metapb.Shard)
	// BecomeLeader the shard was become follower on the current store
	BecomeFollower(metapb.Shard)
}

// CommandWriteBatch command write batch
type CommandWriteBatch interface {
	// Add add a request to this batch, returns true if it can be executed in this batch,
	// otherwrise false
	Add(uint64, *raftcmdpb.Request, map[string]interface{}) (bool, *raftcmdpb.Response, error)
	// Execute excute the batch, and return the write bytes, and diff bytes that used to
	// modify the size of the current shard
	Execute() (uint64, int64, error)
	// Reset reset the current batch for reuse
	Reset()
}

// CommandReadBatch command read batch
type CommandReadBatch interface {
	// Add add a request to this batch, returns true if it can be executed in this batch,
	// otherwrise false
	Add(uint64, *raftcmdpb.Request, map[string]interface{}) (bool, error)
	// Execute excute the batch, and return the responses
	Execute() ([]*raftcmdpb.Response, error)
	// Reset reset the current batch for reuse
	Reset()
}

// ReadCommandFunc the read command handler func
type ReadCommandFunc func(metapb.Shard, *raftcmdpb.Request, map[string]interface{}) *raftcmdpb.Response

// WriteCommandFunc the write command handler func, returns write bytes and the diff bytes
// that used to modify the size of the current shard
type WriteCommandFunc func(metapb.Shard, *raftcmdpb.Request, map[string]interface{}) (uint64, int64, *raftcmdpb.Response)

// LocalCommandFunc directly exec on local func
type LocalCommandFunc func(metapb.Shard, *raftcmdpb.Request) (*raftcmdpb.Response, error)

// Store manage a set of raft group
type Store interface {
	// Start the raft store
	Start()
	// Stop the raft store
	Stop()
	// Meta returns store meta
	Meta() metapb.Store
	// NewRouter returns a new router
	NewRouter() Router
	// RegisterReadFunc register read command handler
	RegisterReadFunc(uint64, ReadCommandFunc)
	// RegisterWriteFunc register write command handler
	RegisterWriteFunc(uint64, WriteCommandFunc)
	// RegisterLocalFunc register local command handler
	RegisterLocalFunc(uint64, LocalCommandFunc)
	// RegisterLocalRequestCB register local request cb to process response
	RegisterLocalRequestCB(func(*raftcmdpb.RaftResponseHeader, *raftcmdpb.Response))
	// RegisterRPCRequestCB register rpc request cb to process response
	RegisterRPCRequestCB(func(*raftcmdpb.RaftResponseHeader, *raftcmdpb.Response))
	// OnRequest receive a request, and call cb while the request is completed
	OnRequest(*raftcmdpb.Request) error
	// MetadataStorage returns a MetadataStorage of the shard group
	MetadataStorage() storage.MetadataStorage
	// DataStorage returns a DataStorage of the shard group
	DataStorageByGroup(uint64) storage.DataStorage
	// MaybeLeader returns the shard replica maybe leader
	MaybeLeader(uint64) bool
	// AddShards add shards meta on the current store, and than prophet will
	// schedule this shard replicas to other nodes.
	AddShards(...metapb.Shard) error
	// AllocID returns a uint64 id, panic if has a error
	MustAllocID() uint64
	// Prophet return current prophet instance
	Prophet() prophet.Prophet
}

const (
	applyWorkerName      = "apply-%d-%d"
	snapshotWorkerName   = "snapshot-%d"
	splitCheckWorkerName = "split"
)

type keyConvertFunc func(uint64, []byte, func(uint64, []byte) metapb.Shard) metapb.Shard

type store struct {
	cfg  Cfg
	opts *options

	meta       *containerAdapter
	pd         prophet.Prophet
	bootOnce   sync.Once
	pdStartedC chan struct{}
	pdState    uint32
	adapter    prophet.Adapter

	runner          *task.Runner
	trans           Transport
	snapshotManager SnapshotManager
	rpc             RPC
	keyRanges       sync.Map // group id -> *util.ShardTree
	peers           sync.Map // peer  id -> peer
	replicas        sync.Map // shard id -> *peerReplica
	delegates       sync.Map // shard id -> *applyDelegate
	droppedVoteMsgs sync.Map // shard id -> etcdraftpb.Message

	readHandlers  map[uint64]ReadCommandFunc
	writeHandlers map[uint64]WriteCommandFunc
	localHandlers map[uint64]LocalCommandFunc

	keyConvertFunc keyConvertFunc

	ensureNewShardTaskID uint64

	stopWG sync.WaitGroup
	state  uint32

	localCB func(*raftcmdpb.RaftResponseHeader, *raftcmdpb.Response)
	rpcCB   func(*raftcmdpb.RaftResponseHeader, *raftcmdpb.Response)

	allocApplyLock sync.Mutex
	applies        []map[string]int
}

// NewStore returns a raft store
func NewStore(cfg Cfg, opts ...Option) Store {
	s := &store{
		meta: &containerAdapter{},
	}

	s.cfg = cfg
	s.meta.meta.ShardAddr = s.cfg.RaftAddr
	s.meta.meta.RPCAddr = s.cfg.RPCAddr

	s.opts = &options{}
	for _, opt := range opts {
		opt(s.opts)
	}
	s.opts.adjust()

	s.snapshotManager = s.opts.snapshotManager
	if s.snapshotManager == nil {
		s.snapshotManager = newDefaultSnapshotManager(s)
	}

	s.trans = s.opts.trans
	if s.trans == nil {
		s.trans = newTransport(s)
	}

	s.rpc = s.opts.rpc
	if s.rpc == nil {
		s.rpc = newRPC(s)
	}

	if s.opts.shardStateAware == nil {
		s.opts.shardStateAware = s
	}

	s.readHandlers = make(map[uint64]ReadCommandFunc)
	s.writeHandlers = make(map[uint64]WriteCommandFunc)
	s.localHandlers = make(map[uint64]LocalCommandFunc)
	s.runner = task.NewRunner()
	s.initWorkers()
	return s
}

func (s *store) Start() {
	logger.Infof("begin start raftstore")

	s.startProphet()
	logger.Infof("prophet started")

	s.trans.Start()
	logger.Infof("transport started at %s", s.cfg.RaftAddr)

	s.startRaftWorkers()
	logger.Infof("raft shards workers started")

	s.startShards()
	logger.Infof("shards started")

	s.startCompactRaftLogTask()
	logger.Infof("shard raft log compact task started")

	s.startSplitCheckTask()
	logger.Infof("shard shard split check task started")

	s.startRPC()
	logger.Infof("store start RPC at %s", s.cfg.RPCAddr)
}

func (s *store) Stop() {
	atomic.StoreUint32(&s.state, 1)

	s.foreachPR(func(pr *peerReplica) bool {
		s.stopWG.Add(1)
		pr.stopEventLoop()
		return true
	})
	s.stopWG.Wait()

	s.runner.Stop()
	s.trans.Stop()
	s.rpc.Stop()
	s.pd.Stop()
}

func (s *store) prStopped() {
	if atomic.LoadUint32(&s.state) == 1 {
		s.stopWG.Done()
	}
}

func (s *store) NewRouter() Router {
	for {
		if atomic.LoadUint32(&s.pdState) == 1 {
			break
		}

		time.Sleep(time.Second)
	}

	return newRouter(s.pd, s.runner, s.keyConvertFunc)
}

func (s *store) Meta() metapb.Store {
	return s.meta.meta
}

func (s *store) RegisterReadFunc(ct uint64, handler ReadCommandFunc) {
	s.readHandlers[ct] = handler
}

func (s *store) RegisterWriteFunc(ct uint64, handler WriteCommandFunc) {
	s.writeHandlers[ct] = handler
}

func (s *store) RegisterLocalFunc(ct uint64, handler LocalCommandFunc) {
	s.localHandlers[ct] = handler
}

func (s *store) RegisterLocalRequestCB(cb func(*raftcmdpb.RaftResponseHeader, *raftcmdpb.Response)) {
	s.localCB = cb
}

func (s *store) RegisterRPCRequestCB(cb func(*raftcmdpb.RaftResponseHeader, *raftcmdpb.Response)) {
	s.rpcCB = cb
}

func (s *store) OnRequest(req *raftcmdpb.Request) error {
	if logger.DebugEnabled() {
		logger.Debugf("%s store received", hex.EncodeToString(req.ID))
	}

	var pr *peerReplica
	var err error
	if req.ToShard > 0 {
		pr = s.getPR(req.ToShard, !req.AllowFollower)
		if pr == nil {
			respStoreNotMatch(err, req, s.cb)
			return nil
		}
	} else {
		pr, err = s.selectShard(req.Group, req.Key)
		if err != nil {
			if err == errStoreNotMatch {
				respStoreNotMatch(err, req, s.cb)
				return nil
			}

			return err
		}
	}

	return pr.onReq(req, s.cb)
}

func (s *store) MetadataStorage() storage.MetadataStorage {
	return s.cfg.MetadataStorage
}

func (s *store) DataStorageByGroup(group uint64) storage.DataStorage {
	return s.cfg.DataStorages[group]
}

func (s *store) MaybeLeader(shard uint64) bool {
	return nil != s.getPR(shard, true)
}

func (s *store) cb(resp *raftcmdpb.RaftCMDResponse) {
	for _, rsp := range resp.Responses {
		if rsp.PID != 0 {
			s.rpcCB(resp.Header, rsp)
		} else {
			s.localCB(resp.Header, rsp)
		}
	}

	pb.ReleaseRaftCMDResponse(resp)
}

func hasGap(left, right metapb.Shard) bool {
	if left.Group != right.Group {
		return false
	}

	leftS := EncodeDataKey(left.Group, left.Start)
	leftE := getDataEndKey(left.Group, left.End)
	rightS := EncodeDataKey(right.Group, right.Start)
	rightE := getDataEndKey(right.Group, right.End)

	return (bytes.Compare(leftS, rightS) >= 0 && bytes.Compare(leftS, rightE) < 0) ||
		(bytes.Compare(rightS, leftS) >= 0 && bytes.Compare(rightS, leftE) < 0)
}

func (s *store) AddShards(shards ...metapb.Shard) error {
	doShards := make(map[uint64]*metapb.Shard)
	for idx := range shards {
		shards[idx].ID = s.MustAllocID()
		shards[idx].Peers = append(shards[idx].Peers, metapb.Peer{
			ID:      s.MustAllocID(),
			StoreID: s.meta.ID(),
		})
		doShards[shards[idx].ID] = &shards[idx]
	}

	// check overlap
	err := s.doWithNewShards(16, func(createAt int64, prev metapb.Shard) (bool, error) {
		for _, shard := range shards {
			if hasGap(shard, prev) {
				delete(doShards, shard.ID)
			}
		}

		return true, nil
	})
	if err != nil {
		return err
	}

	err = s.pd.GetStore().LoadResources(16, func(res prophet.Resource) {
		prev := res.(*resourceAdapter).meta
		for _, shard := range shards {
			if hasGap(shard, prev) {
				delete(doShards, shard.ID)
			}
		}
	})
	if err != nil {
		return err
	}

	if len(doShards) == 0 {
		return nil
	}

	var createShards []metapb.Shard
	var createShardIDs []uint64
	var cmps []clientv3.Cmp
	var ops []clientv3.Op
	var buf bytes.Buffer
	now := time.Now().Unix()
	for _, shard := range doShards {
		createShards = append(createShards, *shard)
		createShardIDs = append(createShardIDs, shard.ID)

		buf.Reset()
		buf.Write(goetty.Int64ToBytes(now))
		buf.Write(protoc.MustMarshal(shard))

		cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(uint64Key(shard.ID, eventsPath)), "=", 0))
		ops = append(ops, clientv3.OpPut(uint64Key(shard.ID, eventsPath), string(buf.Bytes())))
	}
	s.mustSaveShards(createShards...)

	cli := s.pd.GetEtcdClient()
	resp, err := cli.Txn(cli.Ctx()).If(cmps...).Then(ops...).Commit()
	if err != nil || !resp.Succeeded {
		s.mustRemoveShards(createShardIDs...)
		return err
	}

	for _, shard := range doShards {
		if err := s.createPR(*shard); err != nil {
			logger.Fatalf("create shards failed with %+v", err)
		}
	}

	return nil
}

func (s *store) MustAllocID() uint64 {
	id, err := s.pd.GetRPC().AllocID()
	if err != nil {
		logger.Fatalf("alloc id failed with %+v", err)
	}

	return id
}

func (s *store) Prophet() prophet.Prophet {
	return s.pd
}

func (s *store) initWorkers() {
	for g := uint64(0); g < s.opts.groups; g++ {
		s.applies = append(s.applies, make(map[string]int))

		for i := uint64(0); i < s.opts.applyWorkerCount; i++ {
			name := fmt.Sprintf(applyWorkerName, g, i)
			s.runner.AddNamedWorker(name)
			s.applies[g][name] = 0
		}
	}

	for g := uint64(0); g < s.opts.groups; g++ {
		name := fmt.Sprintf(snapshotWorkerName, g)
		s.runner.AddNamedWorker(name)
	}

	s.runner.AddNamedWorker(splitCheckWorkerName)
}

func (s *store) startProphet() {
	logger.Infof("begin to start prophet")
	s.meta.meta.Labels = s.opts.labels

	options := s.opts.prophetOptions
	if len(options) == 0 {
		flag.Set("prophet-data", s.opts.prophetDir())
		options = prophet.ParseProphetOptions(s.cfg.Name)
	}

	s.adapter = newProphetAdapter(s)
	s.pdStartedC = make(chan struct{})
	options = append(options, prophet.WithRoleChangeHandler(s))
	if len(s.opts.locationLabels) > 0 {
		options = append(options, prophet.WithLocationLabels(s.opts.locationLabels))
	}
	s.pd = prophet.NewProphet(s.cfg.Name, s.adapter, options...)
	s.pd.Start()
	<-s.pdStartedC
	atomic.StoreUint32(&s.pdState, 1)
}

func (s *store) startRaftWorkers() {
	for i := uint64(0); i < s.opts.groups; i++ {
		g := i
		for j := uint64(0); j < s.opts.raftMaxWorkers; j++ {
			idx := j
			s.runner.RunCancelableTask(func(ctx context.Context) {
				s.runPRTask(ctx, g, idx)
			})
		}
	}
}

func (s *store) runPRTask(ctx context.Context, g, id uint64) {
	logger.Infof("raft worker %d/%d start", g, id)

	hasEvent := true
	for {
		select {
		case <-ctx.Done():
			logger.Infof("raft worker worker %d/%d exit", g, id)
			return
		default:
			if !hasEvent {
				time.Sleep(time.Millisecond * 10)
			}

			hasEvent = false
			s.replicas.Range(func(key, value interface{}) bool {
				pr := value.(*peerReplica)
				if pr.workerID == id && pr.ps.shard.Group == g && pr.handleEvent() {
					hasEvent = true
				}

				return true
			})
		}
	}
}

func (s *store) startShards() {
	totalCount := 0
	tomebstoneCount := 0
	applyingCount := 0

	wb := util.NewWriteBatch()
	err := s.cfg.MetadataStorage.Scan(metaMinKey, metaMaxKey, func(key, value []byte) (bool, error) {
		shardID, suffix, err := decodeMetaKey(key)
		if err != nil {
			return false, err
		}

		if suffix != stateSuffix {
			return true, nil
		}

		totalCount++

		localState := new(raftpb.ShardLocalState)
		protoc.MustUnmarshal(localState, value)

		for _, p := range localState.Shard.Peers {
			s.peers.Store(p.ID, p)
		}

		if localState.State == raftpb.PeerTombstone {
			s.clearMeta(shardID, wb)
			tomebstoneCount++
			logger.Infof("shard %d is tombstone in store",
				shardID)
			return true, nil
		}

		pr, err := createPeerReplica(s, &localState.Shard)
		if err != nil {
			return false, err
		}

		if localState.State == raftpb.PeerApplying {
			applyingCount++
			logger.Infof("shard %d is applying in store", shardID)
			pr.startApplyingSnapJob()
		}

		pr.startRegistrationJob()

		s.updateShardKeyRange(localState.Shard)
		s.addPR(pr)

		return true, nil
	}, false)

	if err != nil {
		logger.Fatalf("init store failed, errors:\n %+v", err)
	}
	err = s.cfg.MetadataStorage.Write(wb, false)
	if err != nil {
		logger.Fatalf("init store failed, errors:\n %+v", err)
	}

	logger.Infof("starts with %d shards, including %d tombstones and %d applying shards",
		totalCount,
		tomebstoneCount,
		applyingCount)

	s.cleanup()
}

func (s *store) startCompactRaftLogTask() {
	s.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(s.opts.raftLogCompactDuration)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				logger.Infof("compact raft log task stopped")
				return
			case <-ticker.C:
				s.handleCompactRaftLog()
			}
		}
	})
}

func (s *store) startSplitCheckTask() {
	if s.opts.disableShardSplit {
		logger.Infof("shard split disabled")
		return
	}

	s.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(s.opts.shardSplitCheckDuration)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				logger.Infof("shard split check task stopped")
				return
			case <-ticker.C:
				s.handleSplitCheck()
			}
		}
	})
}

func uint64Key(id uint64, base string) string {
	return fmt.Sprintf("%s/%020d", base, id)
}

func (s *store) stopEnsureNewShardsTask() {
	if s.ensureNewShardTaskID > 0 {
		s.runner.StopCancelableTask(s.ensureNewShardTaskID)
	}
}

func (s *store) startEnsureNewShardsTask() {
	s.ensureNewShardTaskID, _ = s.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(s.opts.ensureNewShardInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				logger.Infof("ensure new shards task stopped")
				return
			case <-ticker.C:
				s.doEnsureNewShards(16)
			}
		}
	})
}

func (s *store) doEnsureNewShards(limit int64) {
	now := time.Now().Unix()
	timeout := int64(time.Minute * 5)
	var ops []clientv3.Op
	var recreate []metapb.Shard

	err := s.doWithNewShards(limit, func(createAt int64, shard metapb.Shard) (bool, error) {
		res, err := s.pd.GetStore().GetResource(shard.ID)
		if err != nil {
			return false, err
		}

		n := 2
		if shard.LeastReplicas > 0 {
			n = int(shard.LeastReplicas)
		}

		if res != nil && len(res.Peers()) >= n {
			if s.opts.shardAddHandleFunc != nil {
				err := s.opts.shardAddHandleFunc(shard)
				if err != nil {
					return false, err
				}
			}

			ops = append(ops, clientv3.OpDelete(uint64Key(shard.ID, eventsPath)))
			return true, nil
		}

		isTimeout := now-createAt > timeout
		if !isTimeout {
			return true, nil
		}

		s.doDestroy(shard.ID, shard.Peers[0])

		logger.Warningf("shard %d created timeout after %d seconds, recreated at current node",
			shard.ID,
			now-createAt)
		recreate = append(recreate, shard)
		return true, nil
	})

	if err != nil {
		logger.Errorf("ensure new shards failed with %+v", err)
		return
	}

	if len(recreate) > 0 {
		for i := 0; i < len(recreate); i++ {
			recreate[i].Peers = recreate[i].Peers[:0]
			recreate[i].Peers = append(recreate[i].Peers, metapb.Peer{ID: s.MustAllocID(), StoreID: s.meta.ID()})
			recreate[i].Epoch.ConfVer += 100

			var buf bytes.Buffer
			buf.Write(goetty.Int64ToBytes(time.Now().Unix()))
			buf.Write(protoc.MustMarshal(&recreate[i]))
			ops = append(ops, clientv3.OpPut(uint64Key(recreate[i].ID, eventsPath), string(buf.Bytes())))
		}
	}

	if len(ops) > 0 {
		_, err := s.pd.GetEtcdClient().Txn(context.Background()).Then(ops...).Commit()
		if err != nil {
			logger.Errorf("remove complete and update new shards failed with %+v", err)
		}
	}

	if len(recreate) > 0 {
		s.mustSaveShards(recreate...)
		for i := 0; i < len(recreate); i++ {
			err := s.createPR(recreate[i])
			if err != nil {
				logger.Errorf("create shard %d failed with %+v", recreate[i].ID, err)
			}
		}
	}
}

func (s *store) doWithNewShards(limit int64, fn func(int64, metapb.Shard) (bool, error)) error {
	startID := uint64(0)
	endKey := uint64Key(math.MaxUint64, eventsPath)
	withRange := clientv3.WithRange(endKey)
	withLimit := clientv3.WithLimit(limit)

	for {
		startKey := uint64Key(startID, eventsPath)
		resp, err := s.getFromProphetStore(startKey, withRange, withLimit)
		if err != nil {
			logger.Errorf("ensure new shards failed with %+v", err)
			return err
		}

		for _, item := range resp.Kvs {
			createAt := goetty.Byte2Int64(item.Value)
			shard := metapb.Shard{}
			protoc.MustUnmarshal(&shard, item.Value[8:])

			next, err := fn(createAt, shard)
			if err != nil {
				return err
			}

			if !next {
				return nil
			}

			startID = shard.ID
		}

		// read complete
		if len(resp.Kvs) < int(limit) {
			break
		}

		startID++
	}

	return nil
}

func (s *store) createPR(shard metapb.Shard) error {
	if _, ok := s.replicas.Load(shard.ID); ok {
		return nil
	}

	pr, err := createPeerReplica(s, &shard)
	if err != nil {
		s.mustRemoveShards(shard.ID)
		s.revokeApplyWorker(shard.Group, pr.applyWorker)
		return err
	}

	s.updateShardKeyRange(shard)
	pr.startRegistrationJob()
	s.addPR(pr)

	s.pd.GetRPC().TiggerResourceHeartbeat(shard.ID)
	return nil
}

func (s *store) addPR(pr *peerReplica) {
	count := make([]uint64, s.opts.raftMaxWorkers, s.opts.raftMaxWorkers)
	s.replicas.Range(func(key, value interface{}) bool {
		p := value.(*peerReplica)
		if p.ps.shard.Group == pr.ps.shard.Group {
			count[p.workerID]++
		}
		return true
	})

	best := 0
	for id, v := range count[1:] {
		if v < count[best] {
			best = id
		}
	}

	pr.workerID = uint64(best)
	s.replicas.Store(pr.shardID, pr)

	logger.Infof("shard %d added with peer %+v, epoch %+v, peers %+v, raft worker %d, apply worker %s",
		pr.shardID,
		pr.peer,
		pr.ps.shard.Epoch,
		pr.ps.shard.Peers,
		pr.workerID,
		pr.applyWorker)
}

func (s *store) getFromProphetStore(key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(s.pd.GetEtcdClient().Ctx(), prophet.DefaultRequestTimeout)
	defer cancel()

	resp, err := clientv3.NewKV(s.pd.GetEtcdClient()).Get(ctx, key, opts...)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (s *store) startRPC() {
	err := s.rpc.Start()
	if err != nil {
		logger.Fatalf("start RPC at %s failed with %+v",
			s.cfg.RPCAddr,
			err)
	}
}

func (s *store) clearMeta(id uint64, wb *util.WriteBatch) error {
	metaCount := 0
	raftCount := 0

	var keys [][]byte
	defer func() {
		for _, key := range keys {
			s.MetadataStorage().Free(key)
		}
	}()

	// meta must in the range [id, id + 1)
	metaStart := getMetaPrefix(id)
	metaEnd := getMetaPrefix(id + 1)

	err := s.MetadataStorage().Scan(metaStart, metaEnd, func(key, value []byte) (bool, error) {
		keys = append(keys, key)
		err := wb.Delete(key)
		if err != nil {
			return false, err
		}

		metaCount++
		return true, nil
	}, true)

	if err != nil {
		return err
	}

	raftStart := getRaftPrefix(id)
	raftEnd := getRaftPrefix(id + 1)

	err = s.MetadataStorage().Scan(raftStart, raftEnd, func(key, value []byte) (bool, error) {
		keys = append(keys, key)
		err := wb.Delete(key)
		if err != nil {
			return false, err
		}

		raftCount++
		return true, nil
	}, true)

	if err != nil {
		return err
	}

	logger.Infof("shard %d clear %d meta keys and %d raft keys",
		id,
		metaCount,
		raftCount)

	return nil
}

func (s *store) cleanup() {
	s.keyRanges.Range(func(key, value interface{}) bool {
		// clean up all possible garbage data
		lastStartKey := EncodeDataKey(key.(uint64), nil)

		value.(*util.ShardTree).Ascend(func(shard *metapb.Shard) bool {
			start := encStartKey(shard)
			err := s.DataStorageByGroup(shard.Group).RangeDelete(lastStartKey, start)
			if err != nil {
				logger.Fatalf("cleanup possible garbage data failed, [%+v, %+v) failed with %+v",
					lastStartKey,
					start,
					err)
			}

			lastStartKey = encEndKey(shard)
			return true
		})

		dataMaxKey := getDataMaxKey(key.(uint64))
		for _, driver := range s.cfg.DataStorages {
			err := driver.RangeDelete(lastStartKey, dataMaxKey)
			if err != nil {
				logger.Fatalf("cleanup possible garbage data failed, [%+v, %+v) failed with %+v",
					lastStartKey,
					dataMaxKey,
					err)
			}
		}

		return true
	})

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

func (s *store) revokeApplyWorker(g uint64, name string) {
	s.allocApplyLock.Lock()
	defer s.allocApplyLock.Unlock()

	s.applies[g][name]--
}

func (s *store) allocApplyWorker(g uint64) string {
	s.allocApplyLock.Lock()
	defer s.allocApplyLock.Unlock()

	target := ""
	value := math.MaxInt32
	for name, c := range s.applies[g] {
		if value > c {
			value = c
			target = name
		}
	}

	s.applies[g][target]++
	return target
}

func (s *store) getPeer(id uint64) (metapb.Peer, bool) {
	value, ok := s.peers.Load(id)
	if !ok {
		return metapb.Peer{}, false
	}

	return value.(metapb.Peer), true
}

func (s *store) foreachPR(consumerFunc func(*peerReplica) bool) {
	s.replicas.Range(func(key, value interface{}) bool {
		return consumerFunc(value.(*peerReplica))
	})
}

func (s *store) getPR(id uint64, mustLeader bool) *peerReplica {
	if value, ok := s.replicas.Load(id); ok {
		pr := value.(*peerReplica)
		if mustLeader && !pr.isLeader() {
			return nil
		}

		return pr
	}

	return nil
}

func (s *store) destroyPR(id uint64, target metapb.Peer) {
	logger.Infof("shard %d asking destroying stale peer, peer=<%v>",
		id,
		target)
	s.startDestroyJob(id, target)
}

// In some case, the vote raft msg maybe dropped, so follwer node can't response the vote msg
// DB a has 3 peers p1, p2, p3. The p1 split to new DB b
// case 1: in most sence, p1 apply split raft log is before p2 and p3.
//         At this time, if p2, p3 received the DB b's vote msg,
//         and this vote will dropped by p2 and p3 node,
//         because DB a and DB b has overlapped range at p2 and p3 node
// case 2: p2 or p3 apply split log is before p1, we can't mock DB b's vote msg
func (s *store) cacheDroppedVoteMsg(id uint64, msg etcdraftpb.Message) {
	if msg.Type == etcdraftpb.MsgVote {
		s.droppedVoteMsgs.Store(id, msg)
	}
}

func (s *store) removeDroppedVoteMsg(id uint64) (etcdraftpb.Message, bool) {
	if value, ok := s.droppedVoteMsgs.Load(id); ok {
		s.droppedVoteMsgs.Delete(id)
		return value.(etcdraftpb.Message), true
	}

	return etcdraftpb.Message{}, false
}

func (s *store) validateStoreID(req *raftcmdpb.RaftCMDRequest) error {
	if req.Header.Peer.StoreID != s.meta.meta.ID {
		return fmt.Errorf("store not match, give=<%d> want=<%d>",
			req.Header.Peer.StoreID,
			s.meta.meta.ID)
	}

	return nil
}

func (s *store) validateShard(req *raftcmdpb.RaftCMDRequest) *errorpb.Error {
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

	if !pr.isLeader() {
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

	// If header's term is 2 verions behind current term,
	// leadership may have been changed away.
	if req.Header.Term > 0 && pr.getCurrentTerm() > req.Header.Term+1 {
		return &errorpb.Error{
			Message:      errStaleCMD.Error(),
			StaleCommand: infoStaleCMD,
		}
	}

	shard := pr.ps.shard
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

func checkEpoch(shard metapb.Shard, req *raftcmdpb.RaftCMDRequest) bool {
	checkVer := false
	checkConfVer := false

	if req.AdminRequest != nil {
		switch req.AdminRequest.CmdType {
		case raftcmdpb.Split:
			checkVer = true
		case raftcmdpb.ChangePeer:
			checkConfVer = true
		case raftcmdpb.TransferLeader:
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

	fromEpoch := req.Header.ShardEpoch
	lastestEpoch := shard.Epoch

	if (checkConfVer && fromEpoch.ConfVer < lastestEpoch.ConfVer) ||
		(checkVer && fromEpoch.ShardVer < lastestEpoch.ShardVer) {
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

func newAdminRaftCMDResponse(adminType raftcmdpb.AdminCmdType, rsp protoc.PB) *raftcmdpb.RaftCMDResponse {
	adminResp := new(raftcmdpb.AdminResponse)
	adminResp.Type = adminType

	switch adminType {
	case raftcmdpb.ChangePeer:
		adminResp.ChangePeer = rsp.(*raftcmdpb.ChangePeerResponse)
		break
	case raftcmdpb.TransferLeader:
		adminResp.Transfer = rsp.(*raftcmdpb.TransferLeaderResponse)
		break
	case raftcmdpb.CompactRaftLog:
		adminResp.Compact = rsp.(*raftcmdpb.CompactRaftLogResponse)
		break
	case raftcmdpb.Split:
		adminResp.Split = rsp.(*raftcmdpb.SplitResponse)
		break
	}

	resp := pb.AcquireRaftCMDResponse()
	resp.AdminResponse = adminResp
	return resp
}

func (s *store) updateShardKeyRange(shard metapb.Shard) {
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

func (s *store) removeShardKeyRange(shard metapb.Shard) bool {
	if value, ok := s.keyRanges.Load(shard.Group); ok {
		return value.(*util.ShardTree).Remove(shard)
	}

	return false
}

func (s *store) selectShard(group uint64, key []byte) (*peerReplica, error) {
	shard := s.keyConvertFunc(group, key, s.searchShard)
	if shard.ID == 0 {
		return nil, errStoreNotMatch
	}

	pr, ok := s.replicas.Load(shard.ID)
	if !ok {
		return nil, errStoreNotMatch
	}

	return pr.(*peerReplica), nil
}

func (s *store) searchShard(group uint64, key []byte) metapb.Shard {
	if value, ok := s.keyRanges.Load(group); ok {
		return value.(*util.ShardTree).Search(key)
	}

	return metapb.Shard{}
}

func (s *store) nextShard(shard metapb.Shard) *metapb.Shard {
	if value, ok := s.keyRanges.Load(shard.Group); ok {
		return value.(*util.ShardTree).NextShard(shard.Start)
	}

	return nil
}

func (s *store) mustSaveShards(shards ...metapb.Shard) {
	for _, shard := range shards {
		driver := s.MetadataStorage()
		wb := util.NewWriteBatch()

		// shard local state
		wb.Set(getStateKey(shard.ID), protoc.MustMarshal(&raftpb.ShardLocalState{Shard: shard}))

		// shard raft state
		raftState := new(raftpb.RaftLocalState)
		raftState.LastIndex = raftInitLogIndex
		raftState.HardState = protoc.MustMarshal(&etcdraftpb.HardState{
			Term:   raftInitLogTerm,
			Commit: raftInitLogIndex,
		})
		wb.Set(getRaftStateKey(shard.ID), protoc.MustMarshal(raftState))

		// shard raft apply state
		applyState := new(raftpb.RaftApplyState)
		applyState.AppliedIndex = raftInitLogIndex
		applyState.TruncatedState = raftpb.RaftTruncatedState{
			Term:  raftInitLogTerm,
			Index: raftInitLogIndex,
		}
		wb.Set(getApplyStateKey(shard.ID), protoc.MustMarshal(applyState))

		err := driver.Write(wb, true)
		if err != nil {
			logger.Fatalf("create init shard failed, errors:\n %+v", err)
		}
	}
}

func (s *store) mustRemoveShards(ids ...uint64) {
	for _, id := range ids {
		driver := s.MetadataStorage()
		wb := util.NewWriteBatch()

		wb.Delete(getStateKey(id))
		wb.Delete(getRaftStateKey(id))
		wb.Delete(getApplyStateKey(id))

		err := driver.Write(wb, true)
		if err != nil {
			logger.Fatalf("remove shards failed with %d", err)
		}
	}
}
