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

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/util"
)

const (
	readyBatchSize = 1024
)

type action struct {
	actionType     actionType
	splitCheckData splitCheckData
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
	splitAction
	heartbeatAction
	updateReadMetrics
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

func (pr *replica) addMessage(msg raftpb.Message) {
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
		msg := items[i].(raftpb.Message)
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
