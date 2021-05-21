package raftstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty/buf"
	"github.com/fagongzi/goetty/codec"
	"github.com/fagongzi/goetty/codec/length"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
	"github.com/matrixorigin/matrixcube/aware"
	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/components/prophet"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/option"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
	"github.com/matrixorigin/matrixcube/pb/errorpb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/snapshot"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/transport"
	"github.com/matrixorigin/matrixcube/util"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/raft/raftpb"
)

var (
	addShardsPath    = "/events/shards/add"
	removeShardsPath = "/events/shards/remove"
	logger           = log.NewLoggerWithPrefix("[raftstore]")

	maxShardsOnce = 8
)

// Store manage a set of raft group
type Store interface {
	// Start the raft store
	Start()
	// Stop the raft store
	Stop()
	// Meta returns store meta
	Meta() bhmetapb.Store
	// NewRouter returns a new router
	NewRouter() Router
	// RegisterReadFunc register read command handler
	RegisterReadFunc(uint64, command.ReadCommandFunc)
	// RegisterWriteFunc register write command handler
	RegisterWriteFunc(uint64, command.WriteCommandFunc)
	// RegisterLocalFunc register local command handler
	RegisterLocalFunc(uint64, command.LocalCommandFunc)
	// RegisterLocalRequestCB register local request cb to process response
	RegisterLocalRequestCB(func(*raftcmdpb.RaftResponseHeader, *raftcmdpb.Response))
	// RegisterRPCRequestCB register rpc request cb to process response
	RegisterRPCRequestCB(func(*raftcmdpb.RaftResponseHeader, *raftcmdpb.Response))
	// OnRequest receive a request, and call cb while the request is completed
	OnRequest(*raftcmdpb.Request) error
	// MetadataStorage returns a MetadataStorage of the shard group
	MetadataStorage() storage.MetadataStorage
	// DataStorage returns a DataStorage of the shard group
	DataStorageByGroup(uint64, uint64) storage.DataStorage
	// MaybeLeader returns the shard replica maybe leader
	MaybeLeader(uint64) bool
	// AsyncAddShards add shards meta on the current store, and than prophet will
	// schedule this shard replicas to other nodes.
	AsyncAddShards(...bhmetapb.Shard) error
	// AsyncRemoveShards schedule remove shards asynchronously
	AsyncRemoveShards(ids ...uint64) error
	// AllocID returns a uint64 id, panic if has a error
	MustAllocID() uint64
	// Prophet return current prophet instance
	Prophet() prophet.Prophet
	// CreateRPCCliendSideCodec returns the rpc codec at client side
	CreateRPCCliendSideCodec() (codec.Encoder, codec.Decoder)
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
	pdState    uint32

	runner          *task.Runner
	trans           transport.Transport
	snapshotManager snapshot.SnapshotManager
	rpc             *defaultRPC
	keyRanges       sync.Map // group id -> *util.ShardTree
	peers           sync.Map // peer  id -> peer
	replicas        sync.Map // shard id -> *peerReplica
	delegates       sync.Map // shard id -> *applyDelegate
	droppedVoteMsgs sync.Map // shard id -> raftpb.Message

	readHandlers  map[uint64]command.ReadCommandFunc
	writeHandlers map[uint64]command.WriteCommandFunc
	localHandlers map[uint64]command.LocalCommandFunc

	ensureNewShardTaskID uint64

	stopWG sync.WaitGroup
	state  uint32

	localCB func(*raftcmdpb.RaftResponseHeader, *raftcmdpb.Response)
	rpcCB   func(*raftcmdpb.RaftResponseHeader, *raftcmdpb.Response)

	allocWorkerLock sync.Mutex
	applyWorkers    []map[string]int
	eventWorkers    []map[uint64]int

	aware aware.ShardStateAware
}

// NewStore returns a raft store
func NewStore(cfg *config.Config) Store {
	cfg.Adjust()
	s := &store{
		meta:          &containerAdapter{},
		cfg:           cfg,
		readHandlers:  make(map[uint64]command.ReadCommandFunc),
		writeHandlers: make(map[uint64]command.WriteCommandFunc),
		localHandlers: make(map[uint64]command.LocalCommandFunc),
		runner:        task.NewRunner(),
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

	r, err := newRouter(s.pd, s.runner, s.doDestroy)
	if err != nil {
		logger.Fatalf("create router failed with %+v", err)
	}
	return r
}

func (s *store) Meta() bhmetapb.Store {
	return s.meta.meta
}

func (s *store) RegisterReadFunc(ct uint64, handler command.ReadCommandFunc) {
	s.readHandlers[ct] = handler
}

func (s *store) RegisterWriteFunc(ct uint64, handler command.WriteCommandFunc) {
	s.writeHandlers[ct] = handler
}

func (s *store) RegisterLocalFunc(ct uint64, handler command.LocalCommandFunc) {
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
			respStoreNotMatch(errStoreNotMatch, req, s.cb)
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
	return s.cfg.Storage.MetaStorage
}

func (s *store) DataStorageByGroup(group, shardID uint64) storage.DataStorage {
	return s.cfg.Storage.DataStorageFactory(group, shardID)
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

func hasGap(left, right bhmetapb.Shard) bool {
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

func (s *store) AsyncAddShards(shards ...bhmetapb.Shard) error {
	if len(shards) > maxShardsOnce {
		return fmt.Errorf("exceed the maximum number of add shards, current %d, max %d",
			len(shards), maxShardsOnce)
	}

	doShards := make(map[uint64]*bhmetapb.Shard)
	for idx := range shards {
		shards[idx].ID = s.MustAllocID()
		shards[idx].Peers = append(shards[idx].Peers, metapb.Peer{
			ID:          s.MustAllocID(),
			ContainerID: s.meta.ID(),
		})
		doShards[shards[idx].ID] = &shards[idx]
	}

	// check overlap
	err := s.doWithNewShards(16, func(createAt int64, prev bhmetapb.Shard) (bool, error) {
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

	err = s.pd.GetStorage().LoadResources(16, func(res metadata.Resource) {
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

	var createShards []bhmetapb.Shard
	var cmps []clientv3.Cmp
	var ops []clientv3.Op
	var buffer bytes.Buffer
	now := time.Now().Unix()
	for _, shard := range doShards {
		createShards = append(createShards, *shard)
		buffer.Reset()
		buffer.Write(buf.Int64ToBytes(now))
		buffer.Write(protoc.MustMarshal(shard))

		cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(uint64Key(shard.ID, addShardsPath)), "=", 0))
		ops = append(ops, clientv3.OpPut(uint64Key(shard.ID, addShardsPath), buffer.String()))
	}
	cli := s.pd.GetMember().Client()
	resp, err := cli.Txn(cli.Ctx()).If(cmps...).Then(ops...).Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return fmt.Errorf("save create shards failed")
	}

	s.mustSaveShards(createShards...)
	for _, shard := range doShards {
		if err := s.createPR(*shard); err != nil {
			logger.Fatalf("create shards failed with %+v", err)
		}
	}

	return nil
}

func (s *store) AsyncRemoveShards(ids ...uint64) error {
	if len(ids) > maxShardsOnce {
		return fmt.Errorf("exceed the maximum number of remove shards, current %d, max %d",
			len(ids), maxShardsOnce)
	}

	var cmps []clientv3.Cmp
	var ops []clientv3.Op
	for _, id := range ids {
		cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(uint64Key(id, removeShardsPath)), "=", 0))
		ops = append(ops, clientv3.OpPut(uint64Key(id, removeShardsPath), string(format.Uint64ToBytes(id))))
	}

	cli := s.pd.GetMember().Client()
	resp, err := cli.Txn(cli.Ctx()).If(cmps...).Then(ops...).Commit()
	if err != nil {
		return err
	}
	if !resp.Succeeded {
		return fmt.Errorf("save remove shards failed")
	}

	return nil
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
			transport.WithTimeout(3*time.Duration(s.cfg.Raft.HeartbeatTicks)*s.cfg.Raft.TickInterval.Duration, time.Minute),
			transport.WithSendBatch(int64(s.cfg.Raft.SendRaftBatchSize)),
			transport.WithWorkerCount(s.cfg.Worker.SendRaftMsgWorkerCount, s.cfg.Snapshot.MaxConcurrencySnapChunks),
			transport.WithErrorHandler(func(msg *bhraftpb.RaftMessage, err error) {
				if pr := s.getPR(msg.ShardID, true); pr != nil {
					pr.addReport(msg.Message)
				}
			}))
	}

	s.trans.Start()
}

func (s *store) startRaftWorkers() {
	for i := uint64(0); i < s.cfg.ShardGroups; i++ {
		s.eventWorkers = append(s.eventWorkers, make(map[uint64]int))
		g := i
		for j := uint64(0); j < s.cfg.Worker.RaftEventWorkers; j++ {
			s.eventWorkers[g][j] = 0
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
				if pr.eventWorker == id && pr.ps.shard.Group == g && pr.handleEvent() {
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
	err := s.MetadataStorage().Scan(metaMinKey, metaMaxKey, func(key, value []byte) (bool, error) {
		shardID, suffix, err := decodeMetaKey(key)
		if err != nil {
			return false, err
		}

		if suffix != stateSuffix {
			return true, nil
		}

		totalCount++

		localState := new(bhraftpb.ShardLocalState)
		protoc.MustUnmarshal(localState, value)

		for _, p := range localState.Shard.Peers {
			s.peers.Store(p.ID, p)
		}

		if localState.State == bhraftpb.PeerState_Tombstone {
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

		if localState.State == bhraftpb.PeerState_Applying {
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

	err = s.MetadataStorage().Write(wb, false)
	if err != nil {
		logger.Fatalf("init store failed, errors:\n %+v", err)
	}

	logger.Infof("starts with %d shards, including %d tombstones and %d applying shards",
		totalCount,
		tomebstoneCount,
		applyingCount)

	s.cleanup()
}

func (s *store) startTimerTasks() {
	s.runner.RunCancelableTask(func(ctx context.Context) {
		compactTicker := time.NewTicker(s.cfg.Raft.RaftLog.CompactDuration.Duration)
		defer compactTicker.Stop()

		splitCheckTicker := time.NewTicker(s.cfg.Replication.ShardSplitCheckDuration.Duration)
		defer splitCheckTicker.Stop()

		stateCheckTicker := time.NewTicker(s.cfg.Replication.ShardStateCheckDuration.Duration)
		defer stateCheckTicker.Stop()

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
		ticker := time.NewTicker(s.cfg.Replication.EnsureNewShardInterval.Duration)
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
	var recreate []bhmetapb.Shard

	err := s.doWithNewShards(limit, func(createAt int64, shard bhmetapb.Shard) (bool, error) {
		res, err := s.pd.GetStorage().GetResource(shard.ID)
		if err != nil {
			return false, err
		}

		n := 2
		if shard.LeastReplicas > 0 {
			n = int(shard.LeastReplicas)
		}

		if res != nil && len(res.Peers()) >= n {
			if s.cfg.Customize.CustomShardAddHandleFunc != nil {
				err := s.cfg.Customize.CustomShardAddHandleFunc(shard)
				if err != nil {
					return false, err
				}
			}

			ops = append(ops, clientv3.OpDelete(uint64Key(shard.ID, addShardsPath)))
			return true, nil
		}

		isTimeout := now-createAt > timeout
		if !isTimeout {
			return true, nil
		}

		s.doDestroy(shard.ID)

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
			recreate[i].Peers = append(recreate[i].Peers, metapb.Peer{ID: s.MustAllocID(), ContainerID: s.meta.ID()})
			recreate[i].Epoch.ConfVer += 100

			var buffer bytes.Buffer
			buffer.Write(buf.Int64ToBytes(time.Now().Unix()))
			buffer.Write(protoc.MustMarshal(&recreate[i]))
			ops = append(ops, clientv3.OpPut(uint64Key(recreate[i].ID, addShardsPath), buffer.String()))
		}
	}

	if len(ops) > 0 {
		_, err := s.pd.GetMember().Client().Txn(context.Background()).Then(ops...).Commit()
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

func (s *store) doWithNewShards(limit int64, fn func(int64, bhmetapb.Shard) (bool, error)) error {
	startID := uint64(0)
	endKey := uint64Key(math.MaxUint64, addShardsPath)
	withRange := clientv3.WithRange(endKey)
	withLimit := clientv3.WithLimit(limit)

	for {
		startKey := uint64Key(startID, addShardsPath)
		resp, err := s.getFromProphetStore(startKey, withRange, withLimit)
		if err != nil {
			logger.Errorf("ensure new shards failed with %+v", err)
			return err
		}

		for _, item := range resp.Kvs {
			createAt := buf.Byte2Int64(item.Value)
			shard := bhmetapb.Shard{}
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

func (s *store) createPR(shard bhmetapb.Shard) error {
	if _, ok := s.replicas.Load(shard.ID); ok {
		return nil
	}

	pr, err := createPeerReplica(s, &shard)
	if err != nil {
		s.mustRemoveShards(shard.ID)
		s.revokeWorker(pr)
		return err
	}

	s.updateShardKeyRange(shard)
	pr.startRegistrationJob()
	s.addPR(pr)
	pr.addAction(action{actionType: heartbeatAction})
	return nil
}

func (s *store) addPR(pr *peerReplica) {
	s.replicas.Store(pr.shardID, pr)
	logger.Infof("shard %d added with peer %+v, epoch %+v, peers %+v, raft worker %d, apply worker %s",
		pr.shardID,
		pr.peer,
		pr.ps.shard.Epoch,
		pr.ps.shard.Peers,
		pr.eventWorker,
		pr.applyWorker)
}

func (s *store) removePR(pr *peerReplica) {
	s.replicas.Delete(pr.shardID)
	if s.aware != nil {
		s.aware.Destory(pr.ps.shard)
	}
	s.revokeWorker(pr)
}

func (s *store) getFromProphetStore(key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(s.pd.GetMember().Client().Ctx(), option.DefaultRequestTimeout)
	defer cancel()

	resp, err := clientv3.NewKV(s.pd.GetMember().Client()).Get(ctx, key, opts...)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (s *store) startRPC() {
	err := s.rpc.Start()
	if err != nil {
		logger.Fatalf("start RPC at %s failed with %+v",
			s.cfg.ClientAddr,
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

		value.(*util.ShardTree).Ascend(func(shard *bhmetapb.Shard) bool {
			start := encStartKey(shard)
			err := s.DataStorageByGroup(shard.Group, shard.ID).RangeDelete(lastStartKey, start)
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
		s.cfg.Storage.ForeachDataStorageFunc(func(ds storage.DataStorage) {
			err := ds.RangeDelete(lastStartKey, dataMaxKey)
			if err != nil {
				logger.Fatalf("cleanup possible garbage data failed, [%+v, %+v) failed with %+v",
					lastStartKey,
					dataMaxKey,
					err)
			}
		})

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

func (s *store) revokeWorker(pr *peerReplica) {
	if pr == nil {
		return
	}

	s.allocWorkerLock.Lock()
	defer s.allocWorkerLock.Unlock()

	g := pr.ps.shard.Group
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

func (s *store) validateStoreID(req *raftcmdpb.RaftCMDRequest) error {
	if req.Header.Peer.ContainerID != s.meta.meta.ID {
		return fmt.Errorf("store not match, give=<%d> want=<%d>",
			req.Header.Peer.ContainerID,
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

func checkEpoch(shard bhmetapb.Shard, req *raftcmdpb.RaftCMDRequest) bool {
	checkVer := false
	checkConfVer := false

	if req.AdminRequest != nil {
		switch req.AdminRequest.CmdType {
		case raftcmdpb.AdminCmdType_BatchSplit:
			checkVer = true
		case raftcmdpb.AdminCmdType_ChangePeer:
			checkConfVer = true
		case raftcmdpb.AdminCmdType_TransferLeader:
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

func newAdminRaftCMDResponse(adminType raftcmdpb.AdminCmdType, rsp protoc.PB) *raftcmdpb.RaftCMDResponse {
	adminResp := new(raftcmdpb.AdminResponse)
	adminResp.CmdType = adminType

	switch adminType {
	case raftcmdpb.AdminCmdType_ChangePeer:
		adminResp.ChangePeer = rsp.(*raftcmdpb.ChangePeerResponse)
	case raftcmdpb.AdminCmdType_TransferLeader:
		adminResp.TransferLeader = rsp.(*raftcmdpb.TransferLeaderResponse)
	case raftcmdpb.AdminCmdType_CompactLog:
		adminResp.CompactLog = rsp.(*raftcmdpb.CompactLogResponse)
	case raftcmdpb.AdminCmdType_BatchSplit:
		adminResp.Splits = rsp.(*raftcmdpb.BatchSplitResponse)
	}

	resp := pb.AcquireRaftCMDResponse()
	resp.AdminResponse = adminResp
	return resp
}

func (s *store) updateShardKeyRange(shard bhmetapb.Shard) {
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

func (s *store) removeShardKeyRange(shard bhmetapb.Shard) bool {
	if value, ok := s.keyRanges.Load(shard.Group); ok {
		return value.(*util.ShardTree).Remove(shard)
	}

	return false
}

func (s *store) selectShard(group uint64, key []byte) (*peerReplica, error) {
	shard := s.searchShard(group, key)
	if shard.ID == 0 {
		return nil, errStoreNotMatch
	}

	pr, ok := s.replicas.Load(shard.ID)
	if !ok {
		return nil, errStoreNotMatch
	}

	return pr.(*peerReplica), nil
}

func (s *store) searchShard(group uint64, key []byte) bhmetapb.Shard {
	if value, ok := s.keyRanges.Load(group); ok {
		return value.(*util.ShardTree).Search(key)
	}

	return bhmetapb.Shard{}
}

func (s *store) nextShard(shard bhmetapb.Shard) *bhmetapb.Shard {
	if value, ok := s.keyRanges.Load(shard.Group); ok {
		return value.(*util.ShardTree).NextShard(shard.Start)
	}

	return nil
}
