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
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	trackerPkg "go.etcd.io/etcd/raft/v3/tracker"
	"go.uber.org/zap"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/util/uuid"
)

const (
	readyBatchSize = 1024
)

type action struct {
	actionType         actionType
	snapshotCompaction snapshotCompactionDetails
	splitCheckData     splitCheckData
	targetIndex        uint64
	readMetrics        readMetrics
	epoch              Epoch
	actionCallback     func(interface{})
}

type readMetrics struct {
	readBytes uint64
	readKeys  uint64
}

type splitCheckData struct {
	keys      uint64
	size      uint64
	splitKeys [][]byte
	splitIDs  []rpcpb.SplitID
	ctx       []byte
}

type snapshotCompactionDetails struct {
	snapshot           raftpb.Snapshot
	persistentLogIndex uint64
}

type actionType int

const (
	campaignAction actionType = iota
	checkSplitAction
	checkCompactLogAction
	splitAction
	heartbeatAction
	updateReadMetrics
	checkLogCommittedAction
	checkLogAppliedAction
	logCompactionAction
	snapshotCompactionAction
	checkPendingReadsAction
)

func (pr *replica) addAdminRequest(adminType rpcpb.AdminCmdType, request protoc.PB) {
	shard := pr.getShard()
	if err := pr.addRequest(newReqCtx(rpcpb.Request{
		ID:         uuid.NewV4().Bytes(),
		Group:      shard.Group,
		ToShard:    shard.ID,
		Type:       rpcpb.Admin,
		CustomType: uint64(adminType),
		Epoch:      shard.Epoch,
		Cmd:        protoc.MustMarshal(request),
	}, nil)); err != nil {
		panic(err)
	}
}

func (pr *replica) addRequest(req reqCtx) error {
	if err := pr.requests.Put(req); err != nil {
		return err
	}
	pr.notifyWorker()
	return nil
}

// addAction adds the specified action to the actions queue so it will be
// scheduled to execute in the raft worker thread.
func (pr *replica) addAction(act action) {
	if err := pr.actions.Put(act); err != nil {
		return
	}
	pr.notifyWorker()
}

func (pr *replica) addMessage(msg metapb.RaftMessage) {
	if err := pr.messages.Put(msg); err != nil {
		pr.logger.Info("raft step stopped")
		return
	}
	pr.notifyWorker()
}

func (pr *replica) addFeedback(feedback interface{}) {
	if err := pr.feedbacks.Put(feedback); err != nil {
		pr.logger.Info("raft feedback stopped")
	}
	pr.notifyWorker()
}

func (pr *replica) addSnapshotStatus(ss snapshotStatus) {
	if err := pr.snapshotStatus.Put(ss); err != nil {
		pr.logger.Info("snapshot status stopped")
	}
	pr.notifyWorker()
}

func (pr *replica) addRaftTick() bool {
	if err := pr.ticks.Put(struct{}{}); err != nil {
		return false
	}
	atomic.AddUint64(&pr.tickTotalCount, 1)
	pr.notifyWorker()
	return true
}

func (pr *replica) onRaftTick(arg interface{}) {
	if pr.addRaftTick() {
		metric.SetRaftTickQueueMetric(pr.ticks.Len())
		w := util.DefaultTimeoutWheel()
		if _, err := w.Schedule(pr.cfg.Raft.TickInterval.Duration, pr.onRaftTick, nil); err != nil {
			panic(err)
		}
		return
	}
	pr.logger.Info("raft tick stopped")
}

func (pr *replica) addCheckPendingReads() bool {
	if err := pr.actions.Put(action{actionType: checkPendingReadsAction}); err != nil {
		return false
	}
	pr.notifyWorker()
	return true
}

func (pr *replica) onCheckPendingReads(arg interface{}) {
	// If read index message lost, the pending reads cannot be removed from the `pr.pendingReads`.
	// We periodically check whether there are read requests that need to be cleaned up. These requests
	// will not be responded to, and the client will try again.
	if pr.addCheckPendingReads() {
		w := util.DefaultTimeoutWheel()
		if _, err := w.Schedule(time.Minute, pr.onCheckPendingReads, nil); err != nil {
			panic(err)
		}
		return
	}
	pr.logger.Info("check pending reads stopped")
}

func (pr *replica) shutdown() {
	pr.metrics.flush()
	pr.actions.Dispose()
	pr.ticks.Dispose()
	pr.messages.Dispose()
	pr.feedbacks.Dispose()

	pr.notifyShutdownToPendings()

	// This replica won't be processed by the eventWorker again.
	// This means no further read requests will be started using the stopper.
	pr.readStopper.Stop()
	pr.sm.close()
	pr.logger.Info("replica shutdown completed")
}

func (pr *replica) handleEvent(wc *logdb.WorkerContext) (hasEvent bool, err error) {
	select {
	case <-pr.closedC:
		if !pr.unloaded() {
			pr.shutdown()
			pr.confirmUnloaded()
		}
		pr.logger.Debug("skip handling events on stopped replica")
		return false, nil
	default:
	}

	hasEvent, err = pr.handleInitializedState()
	if err != nil {
		return hasEvent, err
	}
	if hasEvent {
		return hasEvent, nil
	}
	if pr.handleMessage(pr.items) {
		hasEvent = true
	}
	if pr.handleTick(pr.items) {
		hasEvent = true
	}
	if pr.handleFeedback(pr.items) {
		hasEvent = true
	}
	if pr.handleSnapshotStatus(pr.items) {
		hasEvent = true
	}
	if pr.handleRequest(pr.items) {
		hasEvent = true
	}
	if pr.rn.HasReady() {
		hasEvent = true
		if err := pr.handleRaftReady(wc); err != nil {
			return hasEvent, err
		}
	}
	if newEvent, err := pr.handleAction(pr.items); err != nil {
		return hasEvent, err
	} else if newEvent {
		hasEvent = true
	}

	return hasEvent, nil
}

// apply the already received snapshot
// for safety, we have to apply the snapshot once it is received and acked. it
// would corrupt the raft state if we just ignore such snapshots.
func (pr *replica) handleInitializedState() (bool, error) {
	if pr.initialized {
		return false, nil
	}
	defer func() {
		pr.initialized = true
	}()
	pr.logger.Debug("checking initial snapshot")
	ss, err := pr.logdb.GetSnapshot(pr.shardID)
	if err == logdb.ErrNoSnapshot {
		pr.logger.Info("no initial snapshot")
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if raft.IsEmptySnap(ss) {
		// should never be empty here
		panic("unexpected empty snapshot")
	}
	index, err := pr.getPersistentLogIndex()
	if err != nil {
		return false, err
	}
	pr.logger.Info("initial snapshot available",
		zap.Uint64("persistent-index", index),
		log.SnapshotField(ss))
	if ss.Metadata.Index > index {
		pr.logger.Info("applying initial snapshot",
			log.IndexField(ss.Metadata.Index))
		if err := pr.applySnapshot(ss); err != nil {
			return false, err
		}
		pr.pushedIndex = ss.Metadata.Index
	} else {
		// snapshot is out of date, remove the disk image as we will never apply
		// the snapshot in the future. keep the logdb record as LogReader needs
		// this snapshot record to establish the starting point of the persisted
		// raft log entries.
		pr.logger.Info("skipped applying initial snapshot",
			log.IndexField(ss.Metadata.Index))
		if err := pr.removeSnapshot(ss, false); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (pr *replica) handleAction(items []interface{}) (bool, error) {
	if size := pr.actions.Len(); size == 0 {
		return false, nil
	}
	n, err := pr.actions.Get(readyBatchSize, items)
	if err != nil {
		return false, nil
	}

	for i := int64(0); i < n; i++ {
		act := items[i].(action)
		switch act.actionType {
		case checkSplitAction:
			pr.tryCheckSplit(act)
		case splitAction:
			pr.doSplit(act)
		case campaignAction:
			if err := pr.doCampaign(); err != nil {
				pr.logger.Fatal("failed to do campaign",
					zap.Error(err))
			}
		case heartbeatAction:
			pr.prophetHeartbeat()
		case updateReadMetrics:
			pr.doUpdateReadMetrics(act)
		case checkLogCommittedAction:
			pr.doCheckLogCommitted(act)
		case checkLogAppliedAction:
			pr.doCheckLogApplied(act)
		case checkCompactLogAction:
			pr.doCheckLogCompact(pr.rn.Status().Progress, pr.rn.LastIndex())
		case logCompactionAction:
			if err := pr.doLogCompaction(act.targetIndex); err != nil {
				return false, err
			}
		case snapshotCompactionAction:
			if err := pr.snapshotCompaction(act.snapshotCompaction.snapshot,
				act.snapshotCompaction.persistentLogIndex); err != nil {
				return false, err
			}
		case checkPendingReadsAction:
			pr.pendingReads.removeLost()
		}
	}

	if pr.actions.Len() > 0 {
		pr.notifyWorker()
	}
	return true, nil
}

func (pr *replica) doUpdateReadMetrics(act action) {
	pr.stats.readBytes += act.readMetrics.readBytes
	pr.stats.readKeys += act.readMetrics.readKeys
}

func (pr *replica) handleMessage(items []interface{}) bool {
	if size := pr.messages.Len(); size == 0 {
		return false
	}

	n, err := pr.messages.Get(readyBatchSize, items)
	if err != nil {
		return false
	}
	for i := int64(0); i < n; i++ {
		raftMsg := items[i].(metapb.RaftMessage)
		msg := raftMsg.Message
		pr.updateReplicasCommittedIndex(raftMsg)

		if pr.isLeader() && msg.From != 0 {
			pr.replicaHeartbeatsMap.Store(msg.From, time.Now())
		}

		if err := pr.rn.Step(msg); err != nil {
			pr.logger.Error("fail to step raft",
				zap.Error(err))
		}
	}

	size := pr.messages.Len()
	metric.SetRaftStepQueueMetric(size)
	if size > 0 {
		pr.notifyWorker()
	}
	return true
}

func (pr *replica) updateReplicasCommittedIndex(msg metapb.RaftMessage) {
	pr.committedIndexes[msg.From.ID] = msg.CommitIndex
}

func (pr *replica) handleTick(items []interface{}) bool {
	if size := pr.ticks.Len(); size == 0 {
		pr.metrics.flush()
		metric.SetRaftTickQueueMetric(size)
		return false
	}

	n, err := pr.ticks.Get(readyBatchSize, items)
	if err != nil {
		return false
	}
	for i := int64(0); i < n; i++ {
		pr.rn.Tick()
		atomic.AddUint64(&pr.tickHandledCount, 1)
	}

	return true
}

func (pr *replica) handleFeedback(items []interface{}) bool {
	if size := pr.feedbacks.Len(); size == 0 {
		return false
	}

	n, err := pr.feedbacks.Get(readyBatchSize, items)
	if err != nil {
		return false
	}
	for i := int64(0); i < n; i++ {
		if replicaID, ok := items[i].(uint64); ok {
			pr.rn.ReportUnreachable(replicaID)
		}
	}

	size := pr.feedbacks.Len()
	metric.SetRaftReportQueueMetric(size)
	if size > 0 {
		pr.notifyWorker()
	}

	return true
}

func (pr *replica) handleSnapshotStatus(items []interface{}) bool {
	if size := pr.snapshotStatus.Len(); size == 0 {
		return false
	}

	n, err := pr.snapshotStatus.Get(readyBatchSize, items)
	if err != nil {
		return false
	}
	for i := int64(0); i < n; i++ {
		if ss, ok := items[i].(snapshotStatus); ok {
			rss := raft.SnapshotFinish
			if ss.rejected {
				rss = raft.SnapshotFailure
			}
			pr.rn.ReportSnapshot(ss.to, rss)
		}
	}

	size := pr.snapshotStatus.Len()
	metric.SetRaftReportQueueMetric(size)
	if size > 0 {
		pr.notifyWorker()
	}

	return true
}

func (pr *replica) prophetHeartbeat() {
	if !pr.isLeader() {
		return
	}
	shard := pr.getShard()
	req := rpcpb.ShardHeartbeatReq{
		Term:            pr.rn.BasicStatus().Term,
		Leader:          &pr.replica,
		StoreID:         pr.storeID,
		DownReplicas:    pr.collectDownReplicas(),
		PendingReplicas: pr.collectPendingReplicas(),
		Stats:           pr.stats.heartbeatState(),
		GroupKey:        pr.groupController.getShardGroupKey(shard),
	}
	pr.logger.Debug("start send shard heartbeat")
	if err := pr.prophetClient.ShardHeartbeat(shard, req); err != nil {
		pr.logger.Error("fail to send heartbeat to prophet",
			zap.Error(err))
	}
	pr.logger.Debug("end send shard heartbeat")
}

func (pr *replica) doCheckLogCompact(progresses map[uint64]trackerPkg.Progress, lastIndex uint64) {
	if !pr.isLeader() {
		return
	}

	var minReplicatedIndex uint64
	for _, p := range progresses {
		if minReplicatedIndex == 0 {
			minReplicatedIndex = p.Match
		}
		if p.Match < minReplicatedIndex {
			minReplicatedIndex = p.Match
		}
	}
	// When an election happened or a new replica is added, replicatedIdx can be 0.
	if minReplicatedIndex > 0 {
		if lastIndex < minReplicatedIndex {
			pr.logger.Fatal("invalid replicated index",
				zap.Uint64("replicated", minReplicatedIndex),
				zap.Uint64("last", lastIndex))
		}
		metric.ObserveRaftLogLag(lastIndex - minReplicatedIndex)
	}

	compactIndex := minReplicatedIndex
	appliedIndex := pr.appliedIndex
	firstIndex := pr.getFirstIndex()
	if minReplicatedIndex < firstIndex ||
		minReplicatedIndex-firstIndex <= pr.store.cfg.Raft.RaftLog.CompactThreshold {
		pr.logger.Debug("maybe skip requesting log compaction",
			zap.Uint64("min-replicated-index", minReplicatedIndex),
			zap.Uint64("applied-index", minReplicatedIndex),
			zap.Uint64("last-index", lastIndex),
			zap.Uint64("first-index", firstIndex),
			zap.Uint64("threshold", pr.store.cfg.Raft.RaftLog.CompactThreshold))
		compactIndex = 0
	}

	// check wether to force compaction or not
	if compactIndex == 0 &&
		appliedIndex > firstIndex &&
		appliedIndex-firstIndex >= pr.feature.ForceCompactCount {
		compactIndex = appliedIndex
	} else if compactIndex == 0 &&
		pr.stats.raftLogSizeHint >= pr.feature.ForceCompactBytes {
		compactIndex = appliedIndex
	}

	if compactIndex == 0 {
		pr.logger.Debug("requesting log compaction skipped",
			zap.Uint64("min-replicated-index", minReplicatedIndex),
			zap.Uint64("applied-index", minReplicatedIndex),
			zap.Uint64("last-index", lastIndex),
			zap.Uint64("first-index", firstIndex),
			zap.Uint64("threshold", pr.store.cfg.Raft.RaftLog.CompactThreshold))
		return
	}

	if compactIndex > minReplicatedIndex {
		pr.logger.Debug("some replica lag is too large, maybe sent a snapshot later",
			zap.Uint64("lag", compactIndex-minReplicatedIndex))
	}
	compactIndex--
	if compactIndex < firstIndex {
		return
	}
	pr.logger.Info("requesting log compaction",
		log.IndexField(compactIndex))
	pr.addAdminRequest(rpcpb.AdminCompactLog, &rpcpb.CompactLogRequest{
		CompactIndex: compactIndex,
	})
}

func (pr *replica) doLogCompaction(index uint64) error {
	if index == 0 {
		return nil
	}
	pr.logger.Info("log compaction action handled",
		log.IndexField(index))
	// generate a dummy snapshot so we can run the log compaction.
	// this dummy snapshot will be used to establish the marker position of
	// the LogReader on startup.
	// such dummy snapshot will never be loaded, as its Index value is not
	// greater than data storage's persistentLogIndex value.
	term, err := pr.lr.Term(index)
	if err != nil {
		pr.logger.Error("failed to get term value",
			zap.Error(err),
			log.IndexField(index))
		if err == raft.ErrCompacted || err == raft.ErrUnavailable {
			// skip this compaction operation as we can't establish the marker
			// position.
			pr.logger.Info("skipped a compaction action",
				log.IndexField(index))
			return nil
		}
		return err
	}
	// this is a dummy snapshot meaning there is no on disk snapshot image.
	// we are not supposed to apply such dummy snapshot. the dummy flag is
	// used for debugging purposes.
	si := metapb.SnapshotInfo{
		Dummy: true,
	}
	rd := raft.Ready{
		Snapshot: raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: index,
				Term:  term,
			},
			Data: protoc.MustMarshal(&si),
		},
	}
	wc := pr.logdb.NewWorkerContext()
	defer wc.Close()
	if err := pr.logdb.SaveRaftState(pr.shardID, pr.replicaID, rd, wc); err != nil {
		return err
	}
	pr.logger.Info("dummy snapshot saved",
		log.IndexField(index))
	// update LogReader's range info to make the compacted entries invisible to
	// raft.
	if err := pr.lr.Compact(index); err != nil {
		if err != raft.ErrCompacted {
			// TODO: check whether any error should be tolerated.
			return err
		}
	}
	if err := pr.logdb.RemoveEntriesTo(pr.shardID, pr.replicaID, index); err != nil {
		return err
	}
	pr.logger.Info("compaction completed",
		log.IndexField(index))

	return nil
}

func (pr *replica) notifyShutdownToPendings() {
	// resp all stale requests in batch and queue
	pr.incomingProposals.close()

	// resp all pending proposals
	pr.pendingProposals.close()

	// resp all pending requests in batch and queue
	pr.pendingReads.close()

	requests := pr.requests.Dispose()
	for _, r := range requests {
		req := r.(reqCtx)
		if req.cb != nil {
			respStoreNotMatch(errStoreNotMatch, req.req, req.cb)
		}
	}
}
