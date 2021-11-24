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
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/util"
	"go.uber.org/zap"
)

const (
	readyBatchSize = 1024
)

type action struct {
	actionType     actionType
	splitCheckData splitCheckData
	targetIndex    uint64
	readMetrics    readMetrics
	epoch          Epoch
	actionCallback func(interface{})
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
)

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

func (pr *replica) addMessage(msg meta.RaftMessage) {
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

func (pr *replica) addAdminRequest(req rpc.AdminRequest) error {
	req.Epoch = pr.getShard().Epoch
	return pr.addRequest(newAdminReqCtx(req))
}

func (pr *replica) addRaftTick() bool {
	if err := pr.ticks.Put(struct{}{}); err != nil {
		return false
	}
	pr.notifyWorker()
	return true
}

func (pr *replica) onRaftTick(arg interface{}) {
	if pr.addRaftTick() {
		metric.SetRaftTickQueueMetric(pr.ticks.Len())
		util.DefaultTimeoutWheel().Schedule(pr.cfg.Raft.TickInterval.Duration, pr.onRaftTick, nil)
		return
	}
	pr.logger.Info("raft tick stopped")
}

func (pr *replica) shutdown() {
	pr.metrics.flush()
	pr.actions.Dispose()
	pr.ticks.Dispose()
	pr.messages.Dispose()
	pr.feedbacks.Dispose()

	// resp all stale requests in batch and queue
	for {
		if pr.incomingProposals.isEmpty() {
			break
		}
		if c, ok := pr.incomingProposals.pop(); ok {
			for _, req := range c.requestBatch.Requests {
				respStoreNotMatch(errStoreNotMatch, req, c.cb)
			}
		}
	}

	// resp all pending proposals
	pr.pendingProposals.close()

	// resp all pending requests in batch and queue
	for _, rr := range pr.pendingReads.reads {
		for _, req := range rr.batch.Requests {
			respStoreNotMatch(errStoreNotMatch, req, pr.store.shardsProxy.OnResponse)
		}
	}
	pr.pendingReads.reset()

	requests := pr.requests.Dispose()
	for _, r := range requests {
		req := r.(reqCtx)
		if req.cb != nil {
			respStoreNotMatch(errStoreNotMatch, req.req, req.cb)
		}
	}

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
		return false, nil
	default:
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
	if pr.handleRequest(pr.items) {
		hasEvent = true
	}
	if pr.rn.HasReady() {
		hasEvent = true
		if err := pr.handleRaftReady(wc); err != nil {
			return hasEvent, err
		}
	}
	if pr.handleAction(pr.items) {
		hasEvent = true
	}
	return true, nil
}

func (pr *replica) handleAction(items []interface{}) bool {
	if size := pr.actions.Len(); size == 0 {
		return false
	}

	n, err := pr.actions.Get(readyBatchSize, items)
	if err != nil {
		return false
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
		}
	}

	if pr.actions.Len() > 0 {
		pr.notifyWorker()
	}
	return true
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
		raftMsg := items[i].(meta.RaftMessage)
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

func (pr *replica) updateReplicasCommittedIndex(msg meta.RaftMessage) {
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

func (pr *replica) prophetHeartbeat() {
	if !pr.isLeader() {
		return
	}
	req := rpcpb.ResourceHeartbeatReq{
		Term:            pr.rn.BasicStatus().Term,
		Leader:          &pr.replica,
		ContainerID:     pr.storeID,
		DownReplicas:    pr.collectDownReplicas(),
		PendingReplicas: pr.collectPendingReplicas(),
		Stats:           pr.stats.heartbeatState(),
	}

	resource := NewResourceAdapterWithShard(pr.getShard())
	if err := pr.prophetClient.ResourceHeartbeat(resource, req); err != nil {
		pr.logger.Error("fail to send heartbeat to prophet",
			zap.Error(err))
	}
}

func (pr *replica) doCheckLogCompact() {
	// Leader will replicate the compact log command to followers,
	// If we use current replicated_index (like 10) as the compact index,
	// when we replicate this log, the newest replicated_index will be 11,
	// but we only compact the log to 10, not 11, at that time,
	// the first index is 10, and replicated_index is 11, with an extra log,
	// and we will do compact again with compact index 11, in cycles...
	// So we introduce a threshold, if replicated index - first index > threshold,
	// we will try to compact log.
	// raft log entries[..............................................]
	//                  ^                                       ^
	//                  |-----------------threshold------------ |
	//              first_index                         replicated_index

	var replicatedIdx uint64
	for _, p := range pr.rn.Status().Progress {
		if replicatedIdx == 0 {
			replicatedIdx = p.Match
		}

		if p.Match < replicatedIdx {
			replicatedIdx = p.Match
		}
	}

	// When an election happened or a new replica is added, replicatedIdx can be 0.
	if replicatedIdx > 0 {
		lastIdx := pr.rn.LastIndex()
		if lastIdx < replicatedIdx {
			pr.logger.Fatal("invalid replicated index",
				zap.Uint64("replicated", replicatedIdx),
				zap.Uint64("last", lastIdx))
		}

		metric.ObserveRaftLogLag(lastIdx - replicatedIdx)
	}

	var compactIdx uint64
	appliedIdx := pr.appliedIndex
	firstIdx := pr.getFirstLog()

	if replicatedIdx < firstIdx ||
		replicatedIdx-firstIdx <= pr.store.cfg.Raft.RaftLog.CompactThreshold {
		return
	}

	if appliedIdx > firstIdx &&
		appliedIdx-firstIdx >= pr.store.cfg.Raft.RaftLog.ForceCompactCount {
		compactIdx = appliedIdx
	} else if pr.stats.raftLogSizeHint >= pr.store.cfg.Raft.RaftLog.ForceCompactBytes {
		compactIdx = appliedIdx
	}

	// Have no idea why subtract 1 here, but original code did this by magic.
	if compactIdx == 0 {
		pr.logger.Fatal("unexpect compactIdx",
			zap.Uint64("compact", compactIdx))
	}

	if compactIdx > replicatedIdx {
		pr.logger.Info("some replica lag is too large, maybe sent a snapshot later",
			zap.Uint64("lag", compactIdx-replicatedIdx))
	}

	compactIdx--
	if compactIdx < firstIdx {
		// In case compactIdx == firstIdx before subtraction.
		return
	}

	pr.addAdminRequest(rpc.AdminRequest{
		CmdType: rpc.AdminCmdType_CompactLog,
		CompactLog: &rpc.CompactLogRequest{
			CompactIndex: compactIdx,
		},
	})
}
