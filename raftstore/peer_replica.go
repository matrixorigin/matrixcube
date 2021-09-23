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

	"github.com/fagongzi/goetty/buf"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/task"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/util"
	"go.etcd.io/etcd/raft/v3"
)

type readContext struct {
	offset    int
	batchSize int
	buf       *buf.ByteBuf
	attrs     map[string]interface{}
	pr        *peerReplica
}

func newReadContext(pr *peerReplica) *readContext {
	return &readContext{
		buf:   buf.NewByteBuf(512),
		attrs: make(map[string]interface{}),
		pr:    pr,
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

func (ctx *readContext) DataStorage() storage.DataStorage {
	return ctx.pr.store.DataStorageByGroup(ctx.pr.ps.shard.Group, ctx.pr.shardID)
}

func (ctx *readContext) StoreID() uint64 {
	return ctx.pr.store.Meta().ID
}

type peerReplica struct {
	shardID               uint64
	eventWorker           uint64
	applyWorker           string
	startedC              chan struct{}
	disableCompactProtect bool
	peer                  metapb.Peer
	rn                    *raft.RawNode
	stopRaftTick          bool
	leaderID              uint64
	currentTerm           uint64

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

// createPeerReplica called in:
// 1. Event worker goroutine: After the split of the old shard, to create new shard.
// 2. Goroutine that calls start method of store: Load all local shards.
// 3. Prophet event loop: Create shard dynamically.
func createPeerReplica(store *store, shard *bhmetapb.Shard, why string) (*peerReplica, error) {
	peer := findPeer(shard, store.meta.meta.ID)
	if peer == nil {
		return nil, fmt.Errorf("no peer found on store %d in shard %+v",
			store.meta.meta.ID,
			shard)
	}

	return newPeerReplica(store, shard, *peer, why)
}

// createPeerReplicaWithRaftMessage the peer can be created from another node with raft membership changes, and we only
// know the shard_id and peer_id when creating this replicated peer, the shard info
// will be retrieved later after applying snapshot.
func createPeerReplicaWithRaftMessage(store *store, msg *bhraftpb.RaftMessage, peer metapb.Peer, why string) (*peerReplica, error) {
	shard := &bhmetapb.Shard{
		ID:           msg.ShardID,
		Epoch:        msg.ShardEpoch,
		Start:        msg.Start,
		End:          msg.End,
		Group:        msg.Group,
		DisableSplit: msg.DisableSplit,
		Unique:       msg.Unique,
	}

	return newPeerReplica(store, shard, peer, why)
}

func newPeerReplica(store *store, shard *bhmetapb.Shard, peer metapb.Peer, why string) (*peerReplica, error) {
	// We will remove tombstone key when apply snapshot
	logger.Infof("shard %d peer %d begin to create at store %d, peers: %+v, because %s",
		shard.ID,
		peer.ID,
		store.Meta().ID,
		shard.Peers,
		why)

	if peer.ID == 0 {
		return nil, fmt.Errorf("invalid peer %+v", peer)
	}

	pr := new(peerReplica)
	pr.eventWorker = math.MaxUint64
	pr.store = store
	pr.peer = peer
	pr.shardID = shard.ID
	pr.ps = newPeerStorage(store, *shard)
	pr.startedC = make(chan struct{})
	return pr, nil
}

func (pr *peerReplica) start() {
	for _, g := range pr.store.cfg.Raft.RaftLog.DisableCompactProtect {
		if pr.ps.shard.Group == g {
			pr.disableCompactProtect = true
			break
		}
	}

	pr.batch = newBatch(pr)
	pr.readCtx = newReadContext(pr)
	pr.events = task.NewRingBuffer(2)
	pr.ticks = task.New(32)
	pr.steps = task.New(32)
	pr.reports = task.New(32)
	pr.applyResults = task.New(32)
	pr.requests = task.New(32)
	pr.actions = task.New(32)
	pr.readyCtx = &readyContext{
		wb: util.NewWriteBatch(),
	}
	pr.pendingReads = &readIndexQueue{
		shardID: pr.ps.shard.ID,
	}

	pr.ctx, pr.cancel = context.WithCancel(context.Background())
	pr.items = make([]interface{}, readyBatch)

	if pr.store.aware != nil {
		pr.store.aware.Created(pr.ps.shard)
	}

	pr.ps.start(pr.peer)

	c := getRaftConfig(pr.peer.ID, pr.ps.getAppliedIndex(), pr.ps, pr.store.cfg)
	rn, err := raft.NewRawNode(c)
	if err != nil {
		logger.Fatalf("shard %d peer %d create raft node failed with %+v",
			pr.ps.shard.ID,
			pr.peer.ID,
			err)
	}
	pr.rn = rn

	applyWorker, eventWorker := pr.store.allocWorker(pr.ps.shard.Group)
	pr.applyWorker = applyWorker
	pr.registerDelegate()
	logger.Infof("shard %d peer %d delegate register completed",
		pr.ps.shard.ID,
		pr.peer.ID)

	// start drive raft
	pr.eventWorker = eventWorker
	close(pr.startedC)
	logger.Infof("shard %d peer %d added, epoch %+v, peers %+v, raft worker %d, apply worker %s",
		pr.shardID,
		pr.peer.ID,
		pr.ps.shard.Epoch,
		pr.ps.shard.Peers,
		pr.eventWorker,
		pr.applyWorker)

	// If this shard has only one peer and I am the one, campaign directly.
	if len(pr.ps.shard.Peers) == 1 && pr.ps.shard.Peers[0].ContainerID == pr.store.meta.meta.ID {
		logger.Infof("shard %d peer %d try to campaign leader, because only self",
			pr.shardID,
			pr.peer.ID)

		err := pr.rn.Campaign()
		if err != nil {
			logger.Fatalf("shard %d peer %d campaign failed with %+v",
				pr.shardID,
				pr.peer.ID,
				err)
		}
	} else if pr.ps.shard.State == metapb.ResourceState_WaittingCreate &&
		pr.ps.shard.Peers[0].ContainerID == pr.store.Meta().ID {
		logger.Infof("shard %d peer %d try to campaign leader, because first peer of dynamically created",
			pr.shardID,
			pr.peer.ID)

		err := pr.rn.Campaign()
		if err != nil {
			logger.Fatalf("shard %d peer %d campaign failed with %+v",
				pr.shardID,
				pr.peer.ID,
				err)
		}
	}

	pr.onRaftTick(nil)
}

func (pr *peerReplica) registerDelegate() {
	delegate := &applyDelegate{
		store:            pr.store,
		ps:               pr.ps,
		peerID:           pr.peer.ID,
		shard:            pr.ps.shard,
		term:             pr.getCurrentTerm(),
		applyState:       pr.ps.raftApplyState,
		appliedIndexTerm: pr.ps.appliedIndexTerm,
		ctx:              newApplyContext(pr),
		syncData: pr.store.cfg.Customize.CustomAdjustInitAppliedIndexFactory != nil &&
			pr.store.cfg.Customize.CustomAdjustInitAppliedIndexFactory(pr.ps.shard.Group) != nil,
	}

	value, loaded := pr.store.delegates.LoadOrStore(delegate.shard.ID, delegate)
	if loaded {
		err := pr.store.addApplyJob(pr.applyWorker, "clearOldDelegate", func() error {
			old := value.(*applyDelegate)
			if old.peerID != delegate.peerID {
				logger.Fatalf("shard %d delegate peer id not match, old=<%d> curr=<%d>",
					pr.shardID,
					old.peerID,
					delegate.peerID)
			}

			// upgrade old delgate to new
			old.peerID = delegate.peerID
			old.shard = delegate.shard
			old.term = delegate.term
			old.applyState = delegate.applyState
			old.appliedIndexTerm = delegate.appliedIndexTerm
			old.clearAllCommandsAsStale()
			return nil
		}, nil)

		if err != nil {
			if !pr.store.isStopped() {
				logger.Fatalf("shard %d add registration job failed with %+v",
					pr.ps.shard.ID,
					err)
			}
		}
	}
}

func (pr *peerReplica) getPeer(id uint64) (metapb.Peer, bool) {
	value, ok := pr.store.getPeer(id)
	if ok {
		return value, ok
	}

	for _, p := range pr.ps.shard.Peers {
		if p.ID == id {
			pr.store.peers.Store(id, p)
			return p, true
		}
	}

	return metapb.Peer{}, false
}

func (pr *peerReplica) setCurrentTerm(term uint64) {
	atomic.StoreUint64(&pr.currentTerm, term)
}

func (pr *peerReplica) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&pr.currentTerm)
}

func (pr *peerReplica) setLeaderPeerID(id uint64) {
	atomic.StoreUint64(&pr.leaderID, id)
}

func (pr *peerReplica) isLeader() bool {
	return pr.getLeaderPeerID() == pr.peer.ID
}

func (pr *peerReplica) getLeaderPeerID() uint64 {
	return atomic.LoadUint64(&pr.leaderID)
}

func (pr *peerReplica) waitStarted() {
	<-pr.startedC
}

func (pr *peerReplica) notifyWorker() {
	pr.waitStarted()
	pr.store.workReady.notify(pr.ps.shard.Group, pr.eventWorker)
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

func (pr *peerReplica) mustDestroy(why string) {
	if pr.ps.isApplyingSnapshot() {
		util.DefaultTimeoutWheel().Schedule(time.Second*30, func(interface{}) {
			pr.mustDestroy(why)
		}, nil)
		logger.Infof("shard %d peer %d  is applying snapshot, retry destory later",
			pr.shardID,
			pr.peer.ID)
		return
	}

	logger.Infof("shard %d peer %d begin to destroy, because %s",
		pr.shardID,
		pr.peer.ID,
		why)

	pr.stopEventLoop()
	pr.store.removeDroppedVoteMsg(pr.shardID)

	// Shard destory need 2 phase
	// Phase1, clean metadata and update the state to Tombstone
	// Phase2, clean up data asynchronously and remove the state key
	// When we restart store, we can see partially data, because Phase1 and Phase2 are not atomic.
	// We will execute cleanup if we found the Tombstone key.

	wb := util.NewWriteBatch()
	pr.store.clearMeta(pr.shardID, wb)
	pr.store.updatePeerState(pr.ps.shard, bhraftpb.PeerState_Tombstone, wb)
	err := pr.store.MetadataStorage().Write(wb, false)
	if err != nil {
		logger.Fatal("shard %d do destroy failed with %+v",
			pr.shardID,
			err)
	}

	if pr.ps.isInitialized() {
		err := pr.store.startClearDataJob(pr.ps.shard)
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
	metric.IncComandCount(format.Uint64ToString(req.CustemType))

	r := reqCtx{}
	r.req = req
	r.cb = cb
	return pr.addRequest(r)
}

func (pr *peerReplica) stopEventLoop() {
	pr.events.Dispose()
}

func (pr *peerReplica) maybeExecRead() {
	pr.pendingReads.doReadLEAppliedIndex(pr.ps.raftApplyState.AppliedIndex, pr)
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
			logger.Fatalf("%s missing read handle func for type %d, registers %+v",
				hex.EncodeToString(req.ID),
				req.CustemType,
				pr.store.readHandlers)
		}
	}

	c.resp(resp)
}

func (pr *peerReplica) supportSplit() bool {
	return !pr.ps.shard.DisableSplit
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

func (pr *peerReplica) nextProposalIndex() uint64 {
	return pr.rn.NextProposalIndex()
}

func getRaftConfig(id, appliedIndex uint64, ps *peerStorage, cfg *config.Config) *raft.Config {
	return &raft.Config{
		ID:              id,
		Applied:         appliedIndex,
		ElectionTick:    cfg.Raft.ElectionTimeoutTicks,
		HeartbeatTick:   cfg.Raft.HeartbeatTicks,
		MaxSizePerMsg:   uint64(cfg.Raft.MaxSizePerMsg),
		MaxInflightMsgs: cfg.Raft.MaxInflightMsgs,
		Storage:         ps,
		CheckQuorum:     true,
		PreVote:         true,
	}
}
