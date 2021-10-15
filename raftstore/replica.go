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

	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/task"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/util"
)

var dn = util.DescribeReplica

type replica struct {
	logger                *zap.Logger
	shardID               uint64
	replica               Replica
	startedC              chan struct{}
	disableCompactProtect bool
	rn                    *raft.RawNode
	stopRaftTick          bool
	leaderID              uint64
	store                 *store
	lr                    *LogReader
	replicaHeartbeatsMap  sync.Map
	prophetHeartbeatTime  uint64
	incomingProposals     *proposalBatch
	pendingReads          *readIndexQueue
	pendingProposals      *pendingProposals
	sm                    *stateMachine
	ticks                 *task.Queue
	messages              *task.Queue
	feedbacks             *task.Queue
	requests              *task.Queue
	actions               *task.Queue
	items                 []interface{}
	appliedIndex          uint64
	lastReadyIndex        uint64
	writtenKeys           uint64
	writtenBytes          uint64
	readKeys              uint64
	readBytes             uint64
	sizeDiffHint          uint64
	raftLogSizeHint       uint64
	deleteKeysHint        uint64
	// TODO: set these fields on split check
	approximateSize uint64
	approximateKeys uint64

	metrics localMetrics

	closedC   chan struct{}
	unloadedC chan struct{}
	stopOnce  sync.Once
}

// createReplica called in:
// 1. Event worker goroutine: After the split of the old shard, to create new shard.
// 2. Goroutine that calls start method of store: Load all local shards.
// 3. Prophet event loop: Create shard dynamically.
func createReplica(store *store, shard Shard, why string) (*replica, error) {
	replica := findReplica(shard, store.meta.meta.ID)
	if replica == nil {
		return nil, fmt.Errorf("no replica found on store %d in shard %+v",
			store.meta.meta.ID,
			shard)
	}

	return newReplica(store, shard, *replica, why)
}

// createReplicaWithRaftMessage the replica can be created from another node with raft membership changes, and we only
// know the shard_id and replica_id when creating this replicated replica, the shard info
// will be retrieved later after applying snapshot.
func createReplicaWithRaftMessage(store *store, msg meta.RaftMessage, replica Replica, why string) (*replica, error) {
	shard := Shard{
		ID:           msg.ShardID,
		Epoch:        msg.ShardEpoch,
		Start:        msg.Start,
		End:          msg.End,
		Group:        msg.Group,
		DisableSplit: msg.DisableSplit,
		Unique:       msg.Unique,
	}

	return newReplica(store, shard, replica, why)
}

func newReplica(store *store, shard Shard, r Replica, why string) (*replica, error) {
	l := store.logger.With(store.storeField(), log.ShardIDField(shard.ID), log.ReplicaIDField(r.ID))

	l.Info("begin to create replica",
		log.ReasonField(why),
		log.ShardField("metadata", shard))

	if r.ID == 0 {
		return nil, fmt.Errorf("invalid replica %+v", r)
	}

	maxBatchSize := uint64(store.cfg.Raft.MaxEntryBytes)
	pr := &replica{
		logger:            l,
		store:             store,
		replica:           r,
		shardID:           shard.ID,
		startedC:          make(chan struct{}),
		lr:                NewLogReader(l, shard.ID, r.ID, store.logdb),
		pendingProposals:  newPendingProposals(),
		incomingProposals: newProposalBatch(l, maxBatchSize, shard.ID, r),
		pendingReads:      &readIndexQueue{shardID: shard.ID, logger: l},
		ticks:             task.New(32),
		messages:          task.New(32),
		requests:          task.New(32),
		actions:           task.New(32),
		feedbacks:         task.New(32),
		items:             make([]interface{}, readyBatchSize),
		closedC:           make(chan struct{}),
		unloadedC:         make(chan struct{}),
	}
	storage := store.DataStorageByGroup(shard.Group)
	pr.sm = newStateMachine(l, storage, shard, r.ID, pr)
	return pr, nil
}

func (pr *replica) start() {
	pr.logger.Info("begin to start replica")

	shard := pr.getShard()
	for _, g := range pr.store.cfg.Raft.RaftLog.DisableCompactProtect {
		if shard.Group == g {
			pr.disableCompactProtect = true
			break
		}
	}

	if pr.store.aware != nil {
		pr.store.aware.Created(shard)
	}
	if err := pr.initConfState(); err != nil {
		panic(err)
	}
	if _, err := pr.initLogState(); err != nil {
		panic(err)
	}
	c := getRaftConfig(pr.replica.ID, pr.appliedIndex, pr.lr, pr.store.cfg)
	rn, err := raft.NewRawNode(c)
	if err != nil {
		pr.logger.Fatal("fail to create raft node",
			zap.Error(err))
	}
	pr.rn = rn
	close(pr.startedC)

	// TODO: is it okay to invoke pr.rn methods from this thread?
	// If this shard has only one replica and I am the one, campaign directly.
	if len(shard.Replicas) == 1 && shard.Replicas[0].ContainerID == pr.store.meta.meta.ID {
		pr.logger.Info("try to campaign",
			log.ReasonField("only self"))

		if err := pr.rn.Campaign(); err != nil {
			pr.logger.Fatal("fail to campaign",
				zap.Error(err))
		}
	} else if shard.State == metapb.ResourceState_WaittingCreate &&
		shard.Replicas[0].ContainerID == pr.store.Meta().ID {
		pr.logger.Info("try to campaign",
			log.ReasonField("first replica of dynamically created"))

		if err := pr.rn.Campaign(); err != nil {
			pr.logger.Fatal("fail to campaign",
				zap.Error(err))
		}
	}

	pr.onRaftTick(nil)
	pr.logger.Info("replica started")
}

func (pr *replica) close() {
	pr.requestRemoval()
}

func (pr *replica) closed() bool {
	select {
	case <-pr.closedC:
		return true
	default:
	}
	return false
}

func (pr *replica) unloaded() bool {
	select {
	case <-pr.unloadedC:
		return true
	default:
	}
	return false
}

func (pr *replica) confirmUnloaded() {
	close(pr.unloadedC)
}

func (pr *replica) waitUnloaded() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			pr.logger.Info("slow to be unloaded")
		case <-pr.unloadedC:
			return
		}
	}
}

func (pr *replica) requestRemoval() {
	pr.stopOnce.Do(func() {
		close(pr.closedC)
	})
}

func (pr *replica) getShard() Shard {
	return pr.sm.getShard()
}

func (pr *replica) getShardID() uint64 {
	return pr.shardID
}

// initConfState initializes the ConfState of the LogReader which will be
// applied to the raft module.
func (pr *replica) initConfState() error {
	// FIXME: this is using the latest confState, should be using the confState
	// consistent with the aoe state.
	confState := raftpb.ConfState{}
	shard := pr.getShard()
	for _, p := range shard.Replicas {
		if p.Role == metapb.ReplicaRole_Voter {
			confState.Voters = append(confState.Voters, p.ID)
		} else if p.Role == metapb.ReplicaRole_Learner {
			confState.Learners = append(confState.Learners, p.ID)
		}
	}
	pr.lr.SetConfState(confState)
	return nil
}

// initLogState returns a boolean flag indicating whether this is a new node.
func (pr *replica) initLogState() (bool, error) {
	rs, err := pr.store.logdb.ReadRaftState(pr.shardID, pr.replica.ID)
	if errors.Is(err, logdb.ErrNoSavedLog) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	hasRaftHardState := !raft.IsEmptyHardState(rs.State)
	if hasRaftHardState {
		pr.logger.Info("init log state",
			zap.Uint64("count", rs.EntryCount),
			zap.Uint64("first-index", rs.FirstIndex),
			zap.Uint64("commit-index", rs.State.Commit),
			zap.Uint64("term", rs.State.Term))
		pr.lr.SetState(rs.State)
	}
	pr.lr.SetRange(rs.FirstIndex, rs.EntryCount)
	return !(rs.EntryCount > 0 || hasRaftHardState), nil
}

func (pr *replica) getReplicaRecord(id uint64) (Replica, bool) {
	rec, ok := pr.store.getReplicaRecord(id)
	if ok {
		return rec, true
	}

	shard := pr.getShard()
	for _, rec := range shard.Replicas {
		if rec.ID == id {
			pr.store.replicaRecords.Store(id, rec)
			return rec, true
		}
	}

	return Replica{}, false
}

func (pr *replica) setLeaderReplicaID(id uint64) {
	atomic.StoreUint64(&pr.leaderID, id)
}

func (pr *replica) isLeader() bool {
	return pr.getLeaderReplicaID() == pr.replica.ID
}

func (pr *replica) getLeaderReplicaID() uint64 {
	return atomic.LoadUint64(&pr.leaderID)
}

func (pr *replica) waitStarted() {
	<-pr.startedC
}

func (pr *replica) notifyWorker() {
	pr.waitStarted()
	pr.store.workerPool.notify(pr.shardID)
}

func (pr *replica) maybeCampaign() (bool, error) {
	if len(pr.getShard().Replicas) <= 1 {
		// The replica campaigned when it was created, no need to do it again.
		return false, nil
	}
	if err := pr.rn.Campaign(); err != nil {
		return false, err
	}

	return true, nil
}

func (pr *replica) onReq(req rpc.Request, cb func(rpc.ResponseBatch)) error {
	metric.IncComandCount(format.Uint64ToString(req.CustomType))
	return pr.addRequest(newReqCtx(req, cb))
}

func (pr *replica) maybeExecRead() {
	pr.pendingReads.process(pr.appliedIndex, pr)
}

func (pr *replica) supportSplit() bool {
	return !pr.getShard().DisableSplit
}

func (pr *replica) pendingReadCount() int {
	return pr.rn.PendingReadCount()
}

func (pr *replica) readyReadCount() int {
	return pr.rn.ReadyReadCount()
}

func (pr *replica) resetIncomingProposals() {
	shard := pr.getShard()
	pr.incomingProposals = newProposalBatch(pr.logger,
		uint64(pr.store.cfg.Raft.MaxEntryBytes), shard.ID, pr.replica)
}

func (pr *replica) collectDownReplicas() []metapb.ReplicaStats {
	now := time.Now()
	shard := pr.getShard()
	var downReplicas []metapb.ReplicaStats
	for _, p := range shard.Replicas {
		if p.ID == pr.replica.ID {
			continue
		}

		if value, ok := pr.replicaHeartbeatsMap.Load(p.ID); ok {
			last := value.(time.Time)
			if now.Sub(last) >= pr.store.cfg.Replication.MaxPeerDownTime.Duration {
				state := metapb.ReplicaStats{}
				state.Replica = Replica{ID: p.ID, ContainerID: p.ContainerID}
				state.DownSeconds = uint64(now.Sub(last).Seconds())

				downReplicas = append(downReplicas, state)
			}
		}
	}
	return downReplicas
}

// collectPendingReplicas returns a list of replicas that are potentially waiting for
// snapshots from the leader.
func (pr *replica) collectPendingReplicas() []Replica {
	return []Replica{}
}

func (pr *replica) nextProposalIndex() uint64 {
	return pr.rn.NextProposalIndex()
}

func getRaftConfig(id, appliedIndex uint64, lr *LogReader, cfg *config.Config) *raft.Config {
	return &raft.Config{
		ID:                        id,
		Applied:                   appliedIndex,
		ElectionTick:              cfg.Raft.ElectionTimeoutTicks,
		HeartbeatTick:             cfg.Raft.HeartbeatTicks,
		MaxSizePerMsg:             uint64(cfg.Raft.MaxSizePerMsg),
		MaxInflightMsgs:           cfg.Raft.MaxInflightMsgs,
		Storage:                   lr,
		CheckQuorum:               true,
		PreVote:                   true,
		DisableProposalForwarding: true,
	}
}
