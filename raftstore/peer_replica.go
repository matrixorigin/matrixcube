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
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/task"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/util"
)

var dn = util.DescribeReplica

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
	store                 *store
	lr                    *LogReader
	peerHeartbeatsMap     sync.Map
	lastHBTime            uint64
	batch                 *proposeBatch
	ctx                   context.Context
	cancel                context.CancelFunc
	items                 []interface{}
	events                *task.RingBuffer
	ticks                 *task.Queue
	steps                 *task.Queue
	reports               *task.Queue
	applyResults          *task.Queue
	requests              *task.Queue
	actions               *task.Queue

	sm       *stateMachine
	pendings *pendingProposals

	appliedIndex    uint64
	lastReadyIndex  uint64
	pendingReads    *readIndexQueue
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

	readCtx *executeContext
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

	pr := &peerReplica{
		eventWorker: math.MaxUint64,
		store:       store,
		peer:        peer,
		shardID:     shard.ID,
		startedC:    make(chan struct{}),
		lr:          NewLogReader(shard.ID, peer.ID, store.logdb),
		pendings:    newPendingProposals(),
	}
	pr.createStateMachine(shard)
	return pr, nil
}

func (pr *peerReplica) start() {
	shard := pr.getShard()
	for _, g := range pr.store.cfg.Raft.RaftLog.DisableCompactProtect {
		if shard.Group == g {
			pr.disableCompactProtect = true
			break
		}
	}

	pr.batch = newBatch(uint64(pr.store.cfg.Raft.MaxEntryBytes), shard.ID, pr.peer)
	pr.readCtx = newExecuteContext()
	pr.events = task.NewRingBuffer(2)
	pr.ticks = &task.Queue{}
	pr.steps = &task.Queue{}
	pr.reports = &task.Queue{}
	pr.applyResults = &task.Queue{}
	pr.requests = &task.Queue{}
	pr.actions = &task.Queue{}
	pr.pendingReads = &readIndexQueue{
		shardID: pr.shardID,
	}

	pr.ctx, pr.cancel = context.WithCancel(context.Background())
	pr.items = make([]interface{}, readyBatch)

	if pr.store.aware != nil {
		pr.store.aware.Created(shard)
	}

	if err := pr.initConfState(); err != nil {
		panic(err)
	}
	if _, err := pr.initLogState(); err != nil {
		panic(err)
	}

	c := getRaftConfig(pr.peer.ID, pr.appliedIndex, pr.lr, pr.store.cfg)
	rn, err := raft.NewRawNode(c)
	if err != nil {
		logger.Fatalf("shard %d peer %d create raft node failed with %+v",
			pr.shardID,
			pr.peer.ID,
			err)
	}
	pr.rn = rn

	applyWorker, eventWorker := pr.store.allocWorker(shard.Group)
	pr.applyWorker = applyWorker
	logger.Infof("shard %d peer %d delegate register completed",
		pr.shardID,
		pr.peer.ID)

	// start drive raft
	pr.eventWorker = eventWorker
	close(pr.startedC)
	logger.Infof("shard %d peer %d added, epoch %+v, peers %+v, raft worker %d, apply worker %s",
		pr.shardID,
		pr.peer.ID,
		shard.Epoch,
		shard.Peers,
		pr.eventWorker,
		pr.applyWorker)

	// TODO: is it okay to invoke pr.rn methods from this thread?
	// If this shard has only one peer and I am the one, campaign directly.
	if len(shard.Peers) == 1 && shard.Peers[0].ContainerID == pr.store.meta.meta.ID {
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
	} else if shard.State == metapb.ResourceState_WaittingCreate &&
		shard.Peers[0].ContainerID == pr.store.Meta().ID {
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

func (pr *peerReplica) id() string {
	return dn(pr.shardID, pr.peer.ID)
}

func (pr *peerReplica) getShard() bhmetapb.Shard {
	return pr.sm.getShard()
}

// initConfState initializes the ConfState of the LogReader which will be
// applied to the raft module.
func (pr *peerReplica) initConfState() error {
	// FIXME: this is using the latest confState, should be using the confState
	// consistent with the aoe state.
	confState := raftpb.ConfState{}
	shard := pr.getShard()
	for _, p := range shard.Peers {
		if p.Role == metapb.PeerRole_Voter {
			confState.Voters = append(confState.Voters, p.ID)
		} else if p.Role == metapb.PeerRole_Learner {
			confState.Learners = append(confState.Learners, p.ID)
		}
	}
	pr.lr.SetConfState(confState)
	return nil
}

// initLogState returns a boolean flag indicating whether this is a new node.
func (pr *peerReplica) initLogState() (bool, error) {
	rs, err := pr.store.logdb.ReadRaftState(pr.shardID, pr.peer.ID)
	if errors.Is(err, logdb.ErrNoSavedLog) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	hasRaftHardState := !raft.IsEmptyHardState(rs.State)
	if hasRaftHardState {
		logger.Infof("%s initLogState, first index %d, count %d, commit %d, term %d",
			pr.id(), rs.FirstIndex, rs.EntryCount, rs.State.Commit, rs.State.Term)
		pr.lr.SetState(rs.State)
	}
	pr.lr.SetRange(rs.FirstIndex, rs.EntryCount)
	return !(rs.EntryCount > 0 || hasRaftHardState), nil
}

func (pr *peerReplica) createStateMachine(shard *bhmetapb.Shard) {
	pr.sm = &stateMachine{
		pr:          pr,
		store:       pr.store,
		peerID:      pr.peer.ID,
		executorCtx: newApplyContext(),
		dataStorage: pr.store.DataStorageByGroup(shard.Group),
	}
	pr.sm.metadataMu.shard = *shard
}

func (pr *peerReplica) getPeer(id uint64) (metapb.Peer, bool) {
	value, ok := pr.store.getPeer(id)
	if ok {
		return value, true
	}

	shard := pr.getShard()
	for _, p := range shard.Peers {
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
	pr.store.workReady.notify(pr.getShard().Group, pr.eventWorker)
}

func (pr *peerReplica) maybeCampaign() (bool, error) {
	if len(pr.getShard().Peers) <= 1 {
		// The peer campaigned when it was created, no need to do it again.
		return false, nil
	}

	err := pr.rn.Campaign()
	if err != nil {
		return false, err
	}

	return true, nil
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
	pr.pendingReads.doReadLEAppliedIndex(pr.appliedIndex, pr)
}

func (pr *peerReplica) supportSplit() bool {
	return !pr.getShard().DisableSplit
}

func (pr *peerReplica) pendingReadCount() int {
	return pr.rn.PendingReadCount()
}

func (pr *peerReplica) readyReadCount() int {
	return pr.rn.ReadyReadCount()
}

func (pr *peerReplica) resetBatch() {
	shard := pr.getShard()
	pr.batch = newBatch(uint64(pr.store.cfg.Raft.MaxEntryBytes), shard.ID, pr.peer)
}

func (pr *peerReplica) collectDownPeers() []metapb.PeerStats {
	now := time.Now()
	shard := pr.getShard()
	var downPeers []metapb.PeerStats
	for _, p := range shard.Peers {
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

// collectPendingPeers returns a list of peers that are potentially waiting for
// snapshots from the leader.
func (pr *peerReplica) collectPendingPeers() []metapb.Peer {
	return []metapb.Peer{}
}

func (pr *peerReplica) nextProposalIndex() uint64 {
	return pr.rn.NextProposalIndex()
}

func getRaftConfig(id, appliedIndex uint64, lr *LogReader, cfg *config.Config) *raft.Config {
	return &raft.Config{
		ID:              id,
		Applied:         appliedIndex,
		ElectionTick:    cfg.Raft.ElectionTimeoutTicks,
		HeartbeatTick:   cfg.Raft.HeartbeatTicks,
		MaxSizePerMsg:   uint64(cfg.Raft.MaxSizePerMsg),
		MaxInflightMsgs: cfg.Raft.MaxInflightMsgs,
		Storage:         lr,
		CheckQuorum:     true,
		PreVote:         true,
	}
}
