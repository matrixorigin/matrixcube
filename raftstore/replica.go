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
	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixcube/aware"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/errorpb"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/transport"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/util/stop"
	"github.com/matrixorigin/matrixcube/util/task"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

var dn = util.DescribeReplica

type replicaGetter interface {
	getReplica(uint64) (*replica, bool)
}

type snapshotStatus struct {
	to       uint64
	rejected bool
}

type replica struct {
	logger    *zap.Logger
	storeID   uint64
	shardID   uint64
	replicaID uint64
	replica   Replica
	group     uint64
	startedC  chan struct{}
	rn        *raft.RawNode
	leaderID  uint64
	// FIXME: decouple replica from store
	store     *store
	transport transport.Trans
	aware     aware.ShardStateAware
	logdb     logdb.LogDB
	cfg       config.Config

	lr                   *LogReader
	replicaHeartbeatsMap sync.Map
	snapshotter          *snapshotter
	incomingProposals    *proposalBatch
	pendingReads         *readIndexQueue
	pendingProposals     *pendingProposals
	readStopper          *stop.Stopper
	sm                   *stateMachine
	prophetClient        prophet.Client
	groupController      *replicaGroupController
	ticks                *task.Queue
	messages             *task.Queue
	feedbacks            *task.Queue
	snapshotStatus       *task.Queue
	requests             *task.Queue
	actions              *task.Queue
	items                []interface{}
	appliedIndex         uint64
	// pushedIndex is the log index that has been passed to the state machine to
	// be applied
	pushedIndex uint64
	stats       *replicaStats
	metrics     localMetrics

	initialized bool
	closedC     chan struct{}
	unloadedC   chan struct{}
	destroyedC  chan struct{}
	stopOnce    sync.Once

	// committedIndexes the committed value of all replicas is recorded, and this information is not
	// necessarily up-to-date.
	// this map must access in event worker
	committedIndexes map[uint64]uint64 // replica-id -> committed index(saved into logdb)
	// lastCommittedIndex last committed log
	lastCommittedIndex uint64

	destoryTaskFactory destroyReplicaTaskFactory
	destoryTaskMu      struct {
		sync.Mutex
		hasTask bool
		reason  string
	}

	tickTotalCount   uint64
	tickHandledCount uint64
}

// createReplica called in:
// 1. Event worker goroutine: After the split of the old shard, to create new shard.
// 2. Goroutine that calls start method of store: Load all local shards.
// 3. Prophet event loop: Create shard dynamically.
func newReplica(store *store, shard Shard, r Replica, reason string) (*replica, error) {
	l := store.logger.With(store.storeField(), log.ShardIDField(shard.ID), log.ReplicaIDField(r.ID))

	l.Info("begin to create replica",
		log.ReasonField(reason),
		log.ReplicaField("replica", r),
		log.ShardField("metadata", shard))

	if r.ID == 0 {
		return nil, fmt.Errorf("invalid replica %+v", r)
	}

	snapshotter := newSnapshotter(shard.ID, r.ID,
		l.Named("snapshotter"), store.GetReplicaSnapshotDir, store.logdb, store.cfg.FS)
	maxBatchSize := uint64(store.cfg.Raft.MaxEntryBytes)
	pr := &replica{
		logger:            l,
		store:             store,
		transport:         store.trans,
		logdb:             store.logdb,
		cfg:               *store.cfg,
		aware:             store.aware,
		groupController:   store.groupController,
		replica:           r,
		replicaID:         r.ID,
		shardID:           shard.ID,
		storeID:           store.Meta().ID,
		group:             shard.Group,
		startedC:          make(chan struct{}),
		stats:             newReplicaStats(),
		lr:                NewLogReader(l, shard.ID, r.ID, store.logdb),
		pendingProposals:  newPendingProposals(),
		incomingProposals: newProposalBatch(l, maxBatchSize, shard.ID, r),
		pendingReads:      &readIndexQueue{shardID: shard.ID, logger: l},
		snapshotter:       snapshotter,
		ticks:             task.New(32),
		messages:          task.New(32),
		requests:          task.New(32),
		actions:           task.New(32),
		feedbacks:         task.New(32),
		snapshotStatus:    task.New(32),
		items:             make([]interface{}, readyBatchSize),
		closedC:           make(chan struct{}),
		unloadedC:         make(chan struct{}),
		destroyedC:        make(chan struct{}),
		committedIndexes:  make(map[uint64]uint64),
	}
	// we are not guaranteed to have a prophet client in tests
	if store.pd != nil {
		pr.prophetClient = store.pd.GetClient()
	}

	storage := store.DataStorageByGroup(shard.Group)
	pr.sm = newStateMachine(l,
		storage, pr.logdb, shard, r, pr,
		func() *replicaCreator {
			return newReplicaCreator(store)
		})
	pr.destoryTaskFactory = newDefaultDestroyReplicaTaskFactory(pr.addAction,
		pr.prophetClient, defaultCheckInterval)
	return pr, nil
}

// start() should return error
func (pr *replica) start() {
	pr.logger.Info("begin to start replica")
	pr.readStopper = stop.NewStopper(fmt.Sprintf("read-stopper[%d/%d/%d]",
		pr.shardID, pr.replicaID, pr.replica.ContainerID),
		stop.WithLogger(pr.logger))

	shard := pr.getShard()
	if err := pr.snapshotter.prepareReplicaSnapshotDir(); err != nil {
		pr.logger.Fatal("failed to create replica snapshot dir",
			zap.Error(err))
	}
	// pr.initAppliedIndex must be called before pr.initConfState()
	if err := pr.initAppliedIndex(pr.sm.dataStorage); err != nil {
		pr.logger.Fatal("failed to initialize applied index",
			zap.Error(err))
	}
	if err := pr.initConfState(); err != nil {
		pr.logger.Fatal("failed to initialize confState",
			zap.Error(err))
	}
	if _, err := pr.initLogState(); err != nil {
		pr.logger.Fatal("failed to initialize log state",
			zap.Error(err))
	}
	c := getRaftConfig(pr.replicaID, pr.appliedIndex, pr.lr, &pr.cfg, pr.logger)
	rn, err := raft.NewRawNode(c)
	if err != nil {
		pr.logger.Fatal("fail to create raft node",
			zap.Error(err))
	}
	pr.rn = rn

	// We notify the Shard of the creation event before the Shard is driven by the
	// event worker, to ensure that it is always the first event.
	if pr.aware != nil {
		pr.aware.Created(shard)
	}

	pr.setStarted()
	// If this shard has only one replica and I am the one, campaign directly.
	if len(shard.Replicas) == 1 && shard.Replicas[0].ContainerID == pr.storeID {
		pr.logger.Info("try to campaign",
			log.ReasonField("only self"))
		pr.addAction(action{actionType: campaignAction})
	} else if shard.State == metapb.ResourceState_Creating &&
		shard.Replicas[0].ContainerID == pr.storeID {
		pr.logger.Info("try to campaign",
			log.ReasonField("first replica of dynamically created"))
		pr.addAction(action{actionType: campaignAction})
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

func (pr *replica) getFirstIndex() uint64 {
	return pr.sm.getFirstIndex()
}

func (pr *replica) getShardID() uint64 {
	return pr.shardID
}

// TODO: move this into the state machine, it should be invoked as a part of the
// state machine restart procedure.
// initAppliedIndex load PersistentLogIndex from datastorage, use this index to init raft rawnode.
func (pr *replica) initAppliedIndex(storage storage.DataStorage) error {
	dummySnapshot, err := pr.getLogReaderMarkerState()
	if err != nil {
		return err
	}
	index, term := dummySnapshot.Metadata.Index, dummySnapshot.Metadata.Term
	pr.sm.updateAppliedIndexTerm(index, term)
	pr.appliedIndex = index
	pr.pushedIndex = index
	pr.logger.Info("applied index loaded",
		zap.Uint64("applied-index", pr.appliedIndex))
	return nil
}

// initConfState initializes the ConfState of the LogReader which will be
// applied to the raft module.
func (pr *replica) initConfState() error {
	// FIXME: this is using the latest confState, should be using the confState
	// consistent with the aoe state.
	confState := raftpb.ConfState{}
	ss, err := pr.logdb.GetSnapshot(pr.shardID)
	if err != nil && err != logdb.ErrNoSnapshot {
		return err
	} else {
		if !raft.IsEmptySnap(ss) && ss.Metadata.Index > pr.appliedIndex {
			confState = ss.Metadata.ConfState
			pr.logger.Info("init conf state loaded from snapshot",
				zap.Uint64("snapshot-index", ss.Metadata.Index),
				zap.Uint64("applied-index", pr.appliedIndex))
		} else {
			shard := pr.getShard()
			for _, p := range shard.Replicas {
				if p.Role == metapb.ReplicaRole_Voter {
					confState.Voters = append(confState.Voters, p.ID)
				} else if p.Role == metapb.ReplicaRole_Learner {
					confState.Learners = append(confState.Learners, p.ID)
				}
			}
			pr.logger.Info("init conf state loaded from dataStorage metadata")
		}
		pr.logger.Info("init conf state loaded",
			log.ReplicaIDsField("voters", confState.Voters),
			log.ReplicaIDsField("learners", confState.Learners))
		pr.lr.SetConfState(confState)
	}
	return nil
}

func (pr *replica) getLogReaderMarkerState() (raftpb.Snapshot, error) {
	// FIXME: this is an ugly hack
	// the fundamental issue here is that we can't directly tell what is the term
	// value of the persistentLogIndex and thus couldn't establish an marker point
	// for the LogReader. getTerm() works around this issue by querying the
	// underlying LogDB (LogReader.Term() is not ready yet as we are trying to
	// correctly initialize LogReader). This in turn forces us not to cleanup
	// all applied raft logs and snapshots.
	getTerm := func(index uint64) (uint64, error) {
		if index == 0 {
			return 0, nil
		}

		snapshots, err := pr.logdb.GetAllSnapshots(pr.shardID)
		if err != nil {
			return 0, err
		}
		for _, cs := range snapshots {
			if cs.Metadata.Index == index {
				return cs.Metadata.Term, nil
			}
		}

		ents, _, err := pr.logdb.IterateEntries(nil, 0, pr.shardID, pr.replicaID,
			index, index+1, math.MaxUint64)
		if err != nil {
			return 0, err
		}
		if ents[0].Index != index {
			panic("unexpected entry")
		}
		return ents[0].Term, nil
	}

	persistentLogIndex, err := pr.sm.dataStorage.GetPersistentLogIndex(pr.shardID)
	if err != nil {
		return raftpb.Snapshot{}, err
	}
	ss, err := pr.logdb.GetSnapshot(pr.shardID)
	if err != nil && err != logdb.ErrNoSnapshot {
		return raftpb.Snapshot{}, err
	}

	if raft.IsEmptySnap(ss) || ss.Metadata.Index < persistentLogIndex {
		term, err := getTerm(persistentLogIndex)
		if err != nil {
			return raftpb.Snapshot{}, err
		}
		return raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: persistentLogIndex,
				Term:  term,
			},
		}, nil
	} else if ss.Metadata.Index >= persistentLogIndex {
		return ss, nil
	}
	panic("not suppose to reach here")
}

// initLogState returns a boolean flag indicating whether this is a new node.
func (pr *replica) initLogState() (bool, error) {
	dummySnapshot, err := pr.getLogReaderMarkerState()
	if err != nil {
		return false, err
	}
	if err := pr.lr.ApplySnapshot(dummySnapshot); err != nil {
		return false, err
	}
	rs, err := pr.logdb.ReadRaftState(pr.shardID,
		pr.replicaID, dummySnapshot.Metadata.Index)
	if errors.Is(err, logdb.ErrNoSavedLog) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	hasRaftHardState := !raft.IsEmptyHardState(rs.State)
	if hasRaftHardState {
		pr.lr.SetState(rs.State)
	}
	pr.logger.Info("init log state",
		zap.Uint64("count", rs.EntryCount),
		zap.Uint64("first-index", rs.FirstIndex),
		zap.Uint64("commit-index", rs.State.Commit),
		zap.Uint64("term", rs.State.Term))
	pr.lr.SetRange(rs.FirstIndex, rs.EntryCount)
	pr.lastCommittedIndex = rs.State.Commit
	pr.sm.setFirstIndex(rs.FirstIndex)
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
	return pr.getLeaderReplicaID() == pr.replicaID
}

func (pr *replica) getLeaderReplicaID() uint64 {
	return atomic.LoadUint64(&pr.leaderID)
}

func (pr *replica) setStarted() {
	close(pr.startedC)
}

func (pr *replica) waitStarted() {
	<-pr.startedC
}

func (pr *replica) notifyWorker() {
	pr.waitStarted()
	pr.store.workerPool.notify(pr.shardID)
}

func (pr *replica) doCampaign() error {
	return pr.rn.Campaign()
}

func (pr *replica) onReq(req rpc.Request, cb func(rpc.ResponseBatch)) error {
	metric.IncComandCount(format.Uint64ToString(req.CustomType))
	return pr.addRequest(newReqCtx(req, cb))
}

func (pr *replica) maybeExecRead() {
	pr.pendingReads.process(pr.appliedIndex, pr.execReadRequest)
}

func (pr *replica) execReadRequest(req rpc.Request) {
	// FIXME: use an externally passed context instead of `context.Background()` for future tracking.
	err := pr.readStopper.RunTask(context.Background(), func(ctx context.Context) {
		select {
		case <-ctx.Done():
			requestDoneWithReplicaRemoved(req, pr.store.shardsProxy.OnResponse, pr.shardID)
		default:
			if ce := pr.logger.Check(zap.DebugLevel, "begin to exec read requests"); ce != nil {
				ce.Write(log.RequestIDField(req.ID),
					log.RaftRequestField("request", &req))
			}

			ctx := acquireReadCtx()
			defer releaseReadCtx(ctx)

			// FIXME: pr.getShard() has a lock, it's a hot path.
			ctx.reset(pr.getShard(), storage.Request{
				CmdType: req.CustomType,
				Key:     req.Key,
				Cmd:     req.Cmd,
			})

			v, err := pr.sm.dataStorage.Read(ctx)
			if err != nil {
				// FIXME: some read failures should be tolerated.
				pr.logger.Fatal("fail to exec read batch",
					zap.Error(err))
			}

			pr.addAction(action{
				actionType: updateReadMetrics,
				readMetrics: readMetrics{
					readBytes: ctx.readBytes,
					readKeys:  1,
				},
			})

			requestDone(req, pr.store.shardsProxy.OnResponse, v)
		}
	})
	if err == stop.ErrUnavailable {
		pr.store.shardsProxy.OnResponse(rpc.ResponseBatch{Header: rpc.ResponseBatchHeader{Error: errorpb.Error{
			Message: errShardNotFound.Error(),
			ShardNotFound: &errorpb.ShardNotFound{
				ShardID: pr.shardID,
			},
		}}})
	}
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
		uint64(pr.cfg.Raft.MaxEntryBytes), shard.ID, pr.replica)
}

func (pr *replica) collectDownReplicas() []metapb.ReplicaStats {
	now := time.Now()
	shard := pr.getShard()
	var downReplicas []metapb.ReplicaStats
	for _, p := range shard.Replicas {
		if p.ID == pr.replicaID {
			continue
		}

		if value, ok := pr.replicaHeartbeatsMap.Load(p.ID); ok {
			last := value.(time.Time)
			if now.Sub(last) >= pr.cfg.Replication.MaxPeerDownTime.Duration {
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

func (pr *replica) getTickTotalCount() uint64 {
	return atomic.LoadUint64(&pr.tickTotalCount)
}

func (pr *replica) getTickHandledCount() uint64 {
	return atomic.LoadUint64(&pr.tickHandledCount)
}

func getRaftConfig(id, appliedIndex uint64, lr *LogReader, cfg *config.Config, logger *zap.Logger) *raft.Config {
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
		Logger:                    &etcdRaftLoggerAdapter{logger: logger.Sugar()},
	}
}

type etcdRaftLoggerAdapter struct {
	logger *zap.SugaredLogger
}

func (l *etcdRaftLoggerAdapter) Debug(v ...interface{}) { l.logger.Debug(v...) }
func (l *etcdRaftLoggerAdapter) Debugf(format string, v ...interface{}) {
	l.logger.Debugf(format, v...)
}
func (l *etcdRaftLoggerAdapter) Error(v ...interface{}) { l.logger.Error(v...) }
func (l *etcdRaftLoggerAdapter) Errorf(format string, v ...interface{}) {
	l.logger.Errorf(format, v...)
}
func (l *etcdRaftLoggerAdapter) Info(v ...interface{})                 { l.logger.Info(v...) }
func (l *etcdRaftLoggerAdapter) Infof(format string, v ...interface{}) { l.logger.Errorf(format, v...) }
func (l *etcdRaftLoggerAdapter) Warning(v ...interface{})              { l.logger.Warn(v...) }
func (l *etcdRaftLoggerAdapter) Warningf(format string, v ...interface{}) {
	l.logger.Warnf(format, v...)
}
func (l *etcdRaftLoggerAdapter) Fatal(v ...interface{}) { l.logger.Fatal(v...) }
func (l *etcdRaftLoggerAdapter) Fatalf(format string, v ...interface{}) {
	l.logger.Fatalf(format, v...)
}
func (l *etcdRaftLoggerAdapter) Panic(v ...interface{}) { l.logger.Panic(v...) }
func (l *etcdRaftLoggerAdapter) Panicf(format string, v ...interface{}) {
	l.logger.Panicf(format, v...)
}
