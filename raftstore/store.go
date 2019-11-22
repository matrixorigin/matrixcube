package raftstore

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/errorpb"
	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/pb/raftpb"
	"github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/prophet"
	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
)

// CommandWriteBatch command write batch
type CommandWriteBatch interface {
	// Add add a request to this batch, returns true if it can be executed in this batch,
	// otherwrise false
	Add(uint64, *raftcmdpb.Request) (bool, *raftcmdpb.Response, error)
	// Execute excute the batch, and return the write bytes, and diff bytes that used to
	// modify the size of the current shard
	Execute() (uint64, int64, error)
	// Reset reset the current batch for reuse
	Reset()
}

// ReadCommandFunc the read command handler func
type ReadCommandFunc func(uint64, *raftcmdpb.Request) *raftcmdpb.Response

// WriteCommandFunc the write command handler func, returns write bytes and the diff bytes
// that used to modify the size of the current shard
type WriteCommandFunc func(uint64, *raftcmdpb.Request) (uint64, int64, *raftcmdpb.Response)

// Store manage a set of raft group
type Store interface {
	// Start the raft store
	Start()
	// Meta returns store meta
	Meta() metapb.Store
	// NewRouter returns a new router
	NewRouter() Router
	// RegisterReadFunc register read command handler
	RegisterReadFunc(uint64, ReadCommandFunc)
	// RegisterWriteFunc register write command handler
	RegisterWriteFunc(uint64, WriteCommandFunc)
	// OnRequest receive a request, and call cb while the request is completed
	OnRequest(*raftcmdpb.Request, func(*raftcmdpb.RaftCMDResponse)) error
	// MetadataStorage returns a MetadataStorage of the shard
	MetadataStorage(uint64) storage.MetadataStorage
	// DataStorage returns a DataStorage of the shard
	DataStorage(uint64) storage.DataStorage
}

const (
	applyWorkerName      = "apply-%d"
	snapshotWorkerName   = "snapshot"
	splitCheckWorkerName = "split"
)

type keyConvertFunc func([]byte, func([]byte) metapb.Shard) metapb.Shard

type store struct {
	cfg      Cfg
	opts     *options
	raftMask uint64
	dataMask uint64

	meta       *containerAdapter
	pd         prophet.Prophet
	bootOnce   sync.Once
	pdStartedC chan struct{}
	adapter    prophet.Adapter

	runner          *task.Runner
	trans           Transport
	snapshotManager SnapshotManager
	rpc             RPC
	keyRanges       *util.ShardTree
	peers           sync.Map // peer  id -> peer
	replicas        sync.Map // shard id -> *peerReplica
	delegates       sync.Map // shard id -> *applyDelegate
	droppedVoteMsgs sync.Map // shard id -> etcdraftpb.Message

	sendingSnapCount   uint64
	reveivingSnapCount uint64

	readHandlers  map[uint64]ReadCommandFunc
	writeHandlers map[uint64]WriteCommandFunc

	keyConvertFunc keyConvertFunc
}

// NewStore returns a raft store
func NewStore(cfg Cfg, opts ...Option) Store {
	s := &store{
		meta: &containerAdapter{},
	}

	s.cfg = cfg
	s.meta.meta.ShardAddr = s.cfg.RaftAddr
	s.meta.meta.RPCAddr = s.cfg.RPCAddr
	s.raftMask = uint64(len(cfg.MetadataStorages) - 1)
	s.dataMask = uint64(len(cfg.DataStorages) - 1)

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

	s.readHandlers = make(map[uint64]ReadCommandFunc)
	s.writeHandlers = make(map[uint64]WriteCommandFunc)
	s.keyRanges = util.NewShardTree()
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

	s.startShards()
	logger.Infof("shards started")

	s.startCompactRaftLogTask()
	logger.Infof("shard raft log compact task started")

	s.startRPC()
	logger.Infof("store start RPC at %s", s.cfg.RPCAddr)
}

func (s *store) NewRouter() Router {
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

func (s *store) OnRequest(req *raftcmdpb.Request, cb func(*raftcmdpb.RaftCMDResponse)) error {
	if logger.DebugEnabled() {
		logger.Debugf("received %s", formatRequest(req))
	}

	pr, err := s.selectShard(req.Key)
	if err != nil {
		if err == errStoreNotMatch {
			respStoreNotMatch(err, req, cb)
			return nil
		}

		return err
	}

	return pr.onReq(req, cb)
}

func (s *store) MetadataStorage(id uint64) storage.MetadataStorage {
	return s.cfg.MetadataStorages[id&s.raftMask]
}

func (s *store) DataStorage(id uint64) storage.DataStorage {
	return s.cfg.DataStorages[id&s.dataMask]
}

func (s *store) initWorkers() {
	for i := uint64(0); i < s.opts.applyWorkerCount; i++ {
		s.runner.AddNamedWorker(fmt.Sprintf(applyWorkerName, i))
	}
	s.runner.AddNamedWorker(snapshotWorkerName)
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
}

func (s *store) startShards() {
	totalCount := 0
	tomebstoneCount := 0
	applyingCount := 0

	for _, driver := range s.cfg.MetadataStorages {
		wb := driver.NewWriteBatch()
		err := driver.Scan(metaMinKey, metaMaxKey, func(key, value []byte) (bool, error) {
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

			s.keyRanges.Update(localState.Shard)
			s.replicas.Store(shardID, pr)

			return true, nil
		}, false)

		if err != nil {
			logger.Fatalf("init store failed, errors:\n %+v", err)
		}
		err = driver.Write(wb, false)
		if err != nil {
			logger.Fatalf("init store failed, errors:\n %+v", err)
		}
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

func (s *store) startRPC() {
	err := s.rpc.Start()
	if err != nil {
		logger.Fatalf("start RPC at %s failed with %+v",
			s.cfg.RPCAddr,
			err)
	}
}

func (s *store) clearMeta(id uint64, wb util.WriteBatch) error {
	metaCount := 0
	raftCount := 0

	var keys [][]byte
	defer func() {
		for _, key := range keys {
			s.MetadataStorage(id).Free(key)
		}
	}()

	// meta must in the range [id, id + 1)
	metaStart := getMetaPrefix(id)
	metaEnd := getMetaPrefix(id + 1)

	err := s.MetadataStorage(id).Scan(metaStart, metaEnd, func(key, value []byte) (bool, error) {
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

	err = s.MetadataStorage(id).Scan(raftStart, raftEnd, func(key, value []byte) (bool, error) {
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
	// clean up all possible garbage data
	lastStartKey := getDataKey([]byte(""))

	s.keyRanges.Ascend(func(shard *metapb.Shard) bool {
		start := encStartKey(shard)
		err := s.DataStorage(shard.ID).RangeDelete(lastStartKey, start)
		if err != nil {
			logger.Fatalf("cleanup possible garbage data failed, [%+v, %+v) failed with %+v",
				lastStartKey,
				start,
				err)
		}

		lastStartKey = encEndKey(shard)
		return true
	})

	for _, driver := range s.cfg.DataStorages {
		err := driver.RangeDelete(lastStartKey, dataMaxKey)
		if err != nil {
			logger.Fatalf("cleanup possible garbage data failed, [%+v, %+v) failed with %+v",
				lastStartKey,
				dataMaxKey,
				err)
		}
	}

	logger.Infof("cleanup possible garbage data complete")
}

func (s *store) addSnapJob(task func() error, cb func(*task.Job)) error {
	return s.addNamedJobWithCB("", snapshotWorkerName, task, cb)
}

func (s *store) addApplyJob(id uint64, desc string, task func() error, cb func(*task.Job)) error {
	index := (s.opts.applyWorkerCount - 1) & id
	return s.addNamedJobWithCB(desc, fmt.Sprintf(applyWorkerName, index), task, cb)
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
		newShard := s.keyRanges.NextShard(shard.Start)
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
		logger.Infof("shard %d reveiced stale epoch, lastest=<%s> reveived=<%s>",
			shard.ID,
			lastestEpoch.String(),
			fromEpoch.String())
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

func (s *store) selectShard(key []byte) (*peerReplica, error) {
	shard := s.keyConvertFunc(key, s.searchShard)
	if shard.ID == 0 {
		return nil, errStoreNotMatch
	}

	pr, ok := s.replicas.Load(shard.ID)
	if !ok {
		return nil, errStoreNotMatch
	}

	return pr.(*peerReplica), nil
}

func (s *store) searchShard(value []byte) metapb.Shard {
	return s.keyRanges.Search(value)
}
