package raftstore

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/deepfabric/beehive/metric"
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/pb/raftpb"
	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/prophet"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/task"
	"golang.org/x/time/rate"
)

type peerReplica struct {
	shardID               uint64
	workerID              uint64
	applyWorker           string
	disableCompactProtect bool
	peer                  metapb.Peer
	rn                    *raft.RawNode
	stopRaftTick          bool

	store *store
	ps    *peerStorage

	peerHeartbeatsMap sync.Map
	lastHBJob         *task.Job

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
	sizeDiffHint    uint64
	raftLogSizeHint uint64
	deleteKeysHint  uint64

	metrics  localMetrics
	buf      *goetty.ByteBuf
	stopOnce sync.Once

	requestIdxs      []int
	readCommandBatch CommandReadBatch
	readyCtx         *readyContext
	attrs            map[string]interface{}

	writeLimiter *rate.Limiter
}

func createPeerReplica(store *store, shard *metapb.Shard) (*peerReplica, error) {
	peer := findPeer(shard, store.meta.meta.ID)
	if peer == nil {
		return nil, fmt.Errorf("no peer found on store %d in shard %+v",
			store.meta.meta.ID,
			shard)
	}

	return newPeerReplica(store, shard, peer.ID)
}

// The peer can be created from another node with raft membership changes, and we only
// know the shard_id and peer_id when creating this replicated peer, the shard info
// will be retrieved later after applying snapshot.
func createPeerReplicaWithRaftMessage(store *store, msg *raftpb.RaftMessage, peerID uint64) (*peerReplica, error) {
	// We will remove tombstone key when apply snapshot
	logger.Infof("shard %d replicate peer, peerID=<%d>",
		msg.ShardID,
		peerID)

	shard := &metapb.Shard{
		ID:              msg.ShardID,
		Epoch:           msg.ShardEpoch,
		Start:           msg.Start,
		End:             msg.End,
		Group:           msg.Group,
		DisableSplit:    msg.DisableSplit,
		Data:            msg.Data,
		DataAppendToMsg: msg.DataAppendToMsg,
	}

	return newPeerReplica(store, shard, peerID)
}

func newPeerReplica(store *store, shard *metapb.Shard, peerID uint64) (*peerReplica, error) {
	if peerID == 0 {
		return nil, fmt.Errorf("invalid peer id: %d", peerID)
	}

	ps, err := newPeerStorage(store, *shard)
	if err != nil {
		return nil, err
	}

	pr := new(peerReplica)
	pr.applyWorker = store.allocApplyWorker(shard.Group)
	pr.peer = newPeer(peerID, store.meta.meta.ID)
	pr.shardID = shard.ID
	pr.ps = ps

	for _, g := range store.opts.disableRaftLogCompactProtect {
		if shard.Group == g {
			pr.disableCompactProtect = true
			break
		}
	}

	pr.batch = newBatch(pr)
	pr.writeLimiter = rate.NewLimiter(rate.Every(time.Second/time.Duration(store.opts.maxConcurrencyWritesPerShard)),
		int(store.opts.maxConcurrencyWritesPerShard))

	c := getRaftConfig(peerID, ps.getAppliedIndex(), ps, store.opts)
	rn, err := raft.NewRawNode(c, nil)
	if err != nil {
		return nil, err
	}

	pr.buf = goetty.NewByteBuf(256)
	pr.attrs = make(map[string]interface{})
	pr.attrs[AttrBuf] = pr.buf
	if store.opts.readBatchFunc != nil {
		pr.readCommandBatch = store.opts.readBatchFunc()
	}
	pr.rn = rn
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
	if len(shard.Peers) == 1 && shard.Peers[0].StoreID == store.meta.meta.ID {
		err = rn.Campaign()
		if err != nil {
			return nil, err
		}

		logger.Debugf("shard %d try to campaign leader",
			pr.shardID)
	}

	pr.store.opts.shardStateAware.Created(pr.ps.shard)

	pr.ctx, pr.cancel = context.WithCancel(context.Background())
	pr.items = make([]interface{}, readyBatch, readyBatch)
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
		logger.Fatalf("shard %d destroy db is apply for snapshot", pr.shardID)
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

	err = pr.ps.updatePeerState(pr.ps.shard, raftpb.PeerTombstone, wb)
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

	pr.store.replicas.Delete(pr.shardID)
	pr.store.opts.shardStateAware.Destory(pr.ps.shard)
	pr.store.revokeApplyWorker(pr.ps.shard.Group, pr.applyWorker)
	logger.Infof("shard %d destroy self complete.",
		pr.shardID)
}

func (pr *peerReplica) onReq(req *raftcmdpb.Request, cb func(*raftcmdpb.RaftCMDResponse)) error {
	metric.IncComandCount(hack.SliceToString(format.UInt64ToString(req.CustemType)))

	if _, ok := pr.store.writeHandlers[req.CustemType]; ok {
		pr.writeLimiter.Wait(context.TODO())
	}

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
	pr.buf.Clear()
	pr.requestIdxs = pr.requestIdxs[:0]

	if pr.readCommandBatch != nil {
		pr.readCommandBatch.Reset()
	}

	for idx, req := range c.req.Requests {
		if logger.DebugEnabled() {
			logger.Debugf("%s exec", hex.EncodeToString(req.ID))
		}
		resp.Responses = append(resp.Responses, nil)

		if pr.readCommandBatch != nil {
			added, err := pr.readCommandBatch.Add(pr.shardID, req, pr.attrs)
			if err != nil {
				logger.Fatalf("shard %s add %+v to read batch failed with %+v",
					pr.shardID,
					req,
					err)
			}

			if added {
				pr.requestIdxs = append(pr.requestIdxs, idx)

				if logger.DebugEnabled() {
					logger.Debugf("%s added to read batch", hex.EncodeToString(req.ID))
				}
				continue
			}
		}

		if h, ok := pr.store.readHandlers[req.CustemType]; ok {
			resp.Responses[idx] = h(pr.ps.shard, req, pr.attrs)

			if logger.DebugEnabled() {
				logger.Debugf("%s exec completed", hex.EncodeToString(req.ID))
			}
		} else {
			if logger.DebugEnabled() {
				logger.Debugf("%s missing handle func", hex.EncodeToString(req.ID))
			}
		}
	}

	if len(pr.requestIdxs) > 0 {
		responses, err := pr.readCommandBatch.Execute()
		if err != nil {
			logger.Fatalf("shard %s exec read batch failed with %+v",
				pr.shardID,
				err)
		}

		for idx, response := range responses {
			resp.Responses[pr.requestIdxs[idx]] = response
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

func (pr *peerReplica) checkPeers() {
	if !pr.isLeader() {
		pr.peerHeartbeatsMap = sync.Map{}
		return
	}

	peers := pr.ps.shard.Peers
	// Insert heartbeats in case that some peers never response heartbeats.
	for _, p := range peers {
		pr.peerHeartbeatsMap.LoadOrStore(p.ID, time.Now())
	}
}

func (pr *peerReplica) collectDownPeers(maxDuration time.Duration) []*prophet.PeerStats {
	now := time.Now()
	var downPeers []*prophet.PeerStats
	for _, p := range pr.ps.shard.Peers {
		if p.ID == pr.peer.ID {
			continue
		}

		if value, ok := pr.peerHeartbeatsMap.Load(p.ID); ok {
			last := value.(time.Time)
			if now.Sub(last) >= maxDuration {
				state := &prophet.PeerStats{}
				state.Peer = &prophet.Peer{ID: p.ID, ContainerID: p.StoreID}
				state.DownSeconds = uint64(now.Sub(last).Seconds())

				downPeers = append(downPeers, state)
			}
		}
	}
	return downPeers
}

func (pr *peerReplica) collectPendingPeers() []*prophet.Peer {
	var pendingPeers []*prophet.Peer
	status := pr.rn.Status()
	truncatedIdx := pr.ps.getTruncatedIndex()

	for id, progress := range status.Progress {
		if id == pr.peer.ID {
			continue
		}

		if progress.Match < truncatedIdx {
			if v, ok := pr.store.peers.Load(id); ok {
				p := v.(metapb.Peer)
				pendingPeers = append(pendingPeers, &prophet.Peer{ID: p.ID, ContainerID: p.StoreID})
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

func getRaftConfig(id, appliedIndex uint64, store raft.Storage, opts *options) *raft.Config {
	return &raft.Config{
		ID:              id,
		Applied:         appliedIndex,
		ElectionTick:    opts.raftElectionTick,
		HeartbeatTick:   opts.raftHeartbeatTick,
		MaxSizePerMsg:   opts.raftMaxBytesPerMsg,
		MaxInflightMsgs: opts.raftMaxInflightMsgCount,
		Storage:         store,
		CheckQuorum:     true,
		PreVote:         opts.raftPreVote,
	}
}
