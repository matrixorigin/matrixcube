package raftstore

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/fagongzi/goetty/buf"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/task"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/util"
	"go.etcd.io/etcd/raft"
)

type readContext struct {
	offset    int
	batchSize int
	buf       *buf.ByteBuf
	attrs     map[string]interface{}
}

func newReadContext() *readContext {
	return &readContext{
		buf:   buf.NewByteBuf(512),
		attrs: make(map[string]interface{}),
	}
}

func (ctx *readContext) reset() {
	for key := range ctx.attrs {
		delete(ctx.attrs, key)
	}
	ctx.buf.Clear()
	ctx.offset = 0
	ctx.batchSize = 0
}

func (ctx *readContext) WriteBatch() *util.WriteBatch {
	logger.Fatalf("read context can not call WriteBatch()")
	return nil
}

func (ctx *readContext) Attrs() map[string]interface{} {
	return ctx.attrs
}

func (ctx *readContext) ByteBuf() *buf.ByteBuf {
	return ctx.buf
}

func (ctx *readContext) LogIndex() uint64 {
	logger.Fatalf("read context can not call LogIndex()")
	return 0
}

func (ctx *readContext) Offset() int {
	return ctx.offset
}

func (ctx *readContext) BatchSize() int {
	return ctx.batchSize
}

type peerReplica struct {
	shardID               uint64
	eventWorker           uint64
	applyWorker           string
	disableCompactProtect bool
	peer                  metapb.Peer
	rn                    *raft.RawNode
	stopRaftTick          bool

	store *store
	ps    *peerStorage

	peerHeartbeatsMap sync.Map
	lastHBTime        uint64

	batch        *proposeBatch
	pendingReads *readIndexQueue
	ctx          context.Context
	cancel       context.CancelFunc
	items        []interface{}
	events       *task.RingBuffer
	ticks        *task.Queue
	steps        *task.Queue
	reports      *task.Queue
	applyResults *task.Queue
	requests     *task.Queue
	actions      *task.Queue

	writtenKeys     uint64
	writtenBytes    uint64
	readKeys        uint64
	readBytes       uint64
	sizeDiffHint    uint64
	raftLogSizeHint uint64
	deleteKeysHint  uint64
	// TODO: setting on split check
	approximateSize uint64
	approximateKeys uint64

	metrics  localMetrics
	stopOnce sync.Once
	readyCtx *readyContext

	readCtx *readContext
}

func createPeerReplica(store *store, shard *bhmetapb.Shard) (*peerReplica, error) {
	peer := findPeer(shard, store.meta.meta.ID)
	if peer == nil {
		return nil, fmt.Errorf("no peer found on store %d in shard %+v",
			store.meta.meta.ID,
			shard)
	}

	return newPeerReplica(store, shard, *peer)
}

// The peer can be created from another node with raft membership changes, and we only
// know the shard_id and peer_id when creating this replicated peer, the shard info
// will be retrieved later after applying snapshot.
func createPeerReplicaWithRaftMessage(store *store, msg *bhraftpb.RaftMessage, peer metapb.Peer) (*peerReplica, error) {
	// We will remove tombstone key when apply snapshot
	logger.Infof("shard %d replicate peer %+v",
		msg.ShardID,
		peer)

	shard := &bhmetapb.Shard{
		ID:           msg.ShardID,
		Epoch:        msg.ShardEpoch,
		Start:        msg.Start,
		End:          msg.End,
		Group:        msg.Group,
		DisableSplit: msg.DisableSplit,
		Unique:       msg.Unique,
	}

	return newPeerReplica(store, shard, peer)
}

func newPeerReplica(store *store, shard *bhmetapb.Shard, peer metapb.Peer) (*peerReplica, error) {
	if peer.ID == 0 {
		return nil, fmt.Errorf("invalid peer %+v", peer)
	}

	ps, err := newPeerStorage(store, *shard)
	if err != nil {
		return nil, err
	}

	pr := new(peerReplica)
	pr.peer = peer
	pr.shardID = shard.ID
	pr.ps = ps

	for _, g := range store.cfg.Raft.RaftLog.DisableCompactProtect {
		if shard.Group == g {
			pr.disableCompactProtect = true
			break
		}
	}

	pr.batch = newBatch(pr)
	c := getRaftConfig(peer.ID, ps.getAppliedIndex(), ps, store.cfg)
	rn, err := raft.NewRawNode(c)
	if err != nil {
		return nil, err
	}

	pr.rn = rn
	pr.readCtx = newReadContext()
	pr.events = task.NewRingBuffer(2)
	pr.ticks = &task.Queue{}
	pr.steps = &task.Queue{}
	pr.reports = &task.Queue{}
	pr.applyResults = &task.Queue{}
	pr.requests = &task.Queue{}
	pr.actions = &task.Queue{}
	pr.readyCtx = &readyContext{
		wb: util.NewWriteBatch(),
	}

	pr.store = store
	pr.pendingReads = &readIndexQueue{
		shardID:  shard.ID,
		readyCnt: 0,
	}

	// If this shard has only one peer and I am the one, campaign directly.
	if len(shard.Peers) == 1 && shard.Peers[0].ContainerID == store.meta.meta.ID {
		err = rn.Campaign()
		if err != nil {
			return nil, err
		}

		logger.Debugf("shard %d try to campaign leader",
			pr.shardID)
	}

	if pr.store.aware != nil {
		pr.store.aware.Created(pr.ps.shard)
	}

	pr.ctx, pr.cancel = context.WithCancel(context.Background())
	pr.items = make([]interface{}, readyBatch)

	pr.applyWorker, pr.eventWorker = store.allocWorker(shard.Group)
	pr.onRaftTick(nil)
	return pr, nil
}

func (pr *peerReplica) maybeCampaign() (bool, error) {
	if len(pr.ps.shard.Peers) <= 1 {
		// The peer campaigned when it was created, no need to do it again.
		return false, nil
	}

	err := pr.rn.Campaign()
	if err != nil {
		return false, err
	}

	return true, nil
}

func (pr *peerReplica) mustDestroy() {
	if pr.ps.isApplyingSnapshot() {
		util.DefaultTimeoutWheel().Schedule(time.Second*30, func(interface{}) {
			pr.mustDestroy()
		}, nil)
		logger.Infof("shard %d is applying snapshot, retry destory later", pr.shardID)
		return
	}

	logger.Infof("shard %d begin to destroy",
		pr.shardID)

	pr.stopEventLoop()
	pr.store.removeDroppedVoteMsg(pr.shardID)

	wb := util.NewWriteBatch()
	err := pr.store.clearMeta(pr.shardID, wb)
	if err != nil {
		logger.Fatal("shard %d do destroy failed with %+v",
			pr.shardID,
			err)
	}

	err = pr.ps.updatePeerState(pr.ps.shard, bhraftpb.PeerState_Tombstone, wb)
	if err != nil {
		logger.Fatal("shard %d do destroy failed with %+v",
			pr.shardID,
			err)
	}

	err = pr.store.MetadataStorage().Write(wb, false)
	if err != nil {
		logger.Fatal("shard %d do destroy failed with %+v",
			pr.shardID,
			err)
	}

	if pr.ps.isInitialized() {
		err := pr.ps.clearData()
		if err != nil {
			logger.Fatal("shard %d do destroy failed with %+v",
				pr.shardID,
				err)
		}
	}

	pr.cancel()

	if pr.ps.isInitialized() && !pr.store.removeShardKeyRange(pr.ps.shard) {
		logger.Warningf("shard %d remove key range failed",
			pr.shardID)
	}

	pr.store.removePR(pr)
	logger.Infof("shard %d destroy self complete.",
		pr.shardID)
}

func (pr *peerReplica) onReq(req *raftcmdpb.Request, cb func(*raftcmdpb.RaftCMDResponse)) error {
	metric.IncComandCount(hack.SliceToString(format.UInt64ToString(req.CustemType)))

	r := reqCtx{}
	r.req = req
	r.cb = cb
	return pr.addRequest(r)
}

func (pr *peerReplica) stopEventLoop() {
	pr.events.Dispose()
}

func (pr *peerReplica) doExecReadCmd(c cmd) {
	resp := pb.AcquireRaftCMDResponse()

	pr.readCtx.reset()
	pr.readCtx.batchSize = len(c.req.Requests)
	for idx, req := range c.req.Requests {
		if logger.DebugEnabled() {
			logger.Debugf("%s exec", hex.EncodeToString(req.ID))
		}
		pr.readKeys++
		pr.readCtx.offset = idx
		if h, ok := pr.store.readHandlers[req.CustemType]; ok {
			rsp, readBytes := h(pr.ps.shard, req, pr.readCtx)
			resp.Responses = append(resp.Responses, rsp)
			pr.readBytes += readBytes
			if logger.DebugEnabled() {
				logger.Debugf("%s exec completed", hex.EncodeToString(req.ID))
			}
		} else {
			logger.Fatalf("%s missing handle func", hex.EncodeToString(req.ID))
		}
	}

	c.resp(resp)
}

func (pr *peerReplica) supportSplit() bool {
	return !pr.ps.shard.DisableSplit
}

func (pr *peerReplica) isLeader() bool {
	return pr.rn.Status().RaftState == raft.StateLeader
}

func (pr *peerReplica) getLeaderPeerID() uint64 {
	return pr.rn.Status().Lead
}

func (pr *peerReplica) pendingReadCount() int {
	return pr.rn.PendingReadCount()
}

func (pr *peerReplica) readyReadCount() int {
	return pr.rn.ReadyReadCount()
}

func (pr *peerReplica) resetBatch() {
	pr.batch = newBatch(pr)
}

func (pr *peerReplica) collectDownPeers() []metapb.PeerStats {
	now := time.Now()
	var downPeers []metapb.PeerStats
	for _, p := range pr.ps.shard.Peers {
		if p.ID == pr.peer.ID {
			continue
		}

		if value, ok := pr.peerHeartbeatsMap.Load(p.ID); ok {
			last := value.(time.Time)
			if now.Sub(last) >= pr.store.cfg.Replication.MaxPeerDownTime.Duration {
				state := metapb.PeerStats{}
				state.Peer = metapb.Peer{ID: p.ID, ContainerID: p.ContainerID}
				state.DownSeconds = uint64(now.Sub(last).Seconds())

				downPeers = append(downPeers, state)
			}
		}
	}
	return downPeers
}

func (pr *peerReplica) collectPendingPeers() []metapb.Peer {
	var pendingPeers []metapb.Peer
	status := pr.rn.Status()
	truncatedIdx := pr.ps.getTruncatedIndex()

	for id, progress := range status.Progress {
		if id == pr.peer.ID {
			continue
		}

		if progress.Match < truncatedIdx {
			if v, ok := pr.store.peers.Load(id); ok {
				p := v.(metapb.Peer)
				pendingPeers = append(pendingPeers, metapb.Peer{ID: p.ID, ContainerID: p.ContainerID})
			}
		}
	}

	return pendingPeers
}

func (pr *peerReplica) readyToHandleRead() bool {
	// If applied_index_term isn't equal to current term, there may be some values that are not
	// applied by this leader yet but the old leader.
	return pr.ps.appliedIndexTerm == pr.getCurrentTerm()
}

func (pr *peerReplica) getCurrentTerm() uint64 {
	return pr.rn.Status().Term
}

func (pr *peerReplica) nextProposalIndex() uint64 {
	return pr.rn.NextProposalIndex()
}

func getRaftConfig(id, appliedIndex uint64, store raft.Storage, cfg *config.Config) *raft.Config {
	return &raft.Config{
		ID:              id,
		Applied:         appliedIndex,
		ElectionTick:    cfg.Raft.ElectionTimeoutTicks,
		HeartbeatTick:   cfg.Raft.HeartbeatTicks,
		MaxSizePerMsg:   uint64(cfg.Raft.MaxSizePerMsg),
		MaxInflightMsgs: cfg.Raft.MaxInflightMsgs,
		Storage:         store,
		CheckQuorum:     true,
		PreVote:         cfg.Raft.EnablePreVote,
	}
}
