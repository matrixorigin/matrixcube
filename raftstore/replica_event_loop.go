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

	"github.com/matrixorigin/matrixcube/components/keys"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/util"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

const (
	readyBatch = 1024
)

type action struct {
	actionType actionType
	splitKeys  [][]byte
	splitIDs   []rpcpb.SplitID
	epoch      metapb.ResourceEpoch
}

type actionType int

const (
	checkCompactAction = actionType(0)
	doCampaignAction   = actionType(1)
	checkSplitAction   = actionType(2)
	doSplitAction      = actionType(3)
	heartbeatAction    = actionType(4)
)

func (pr *replica) addRequest(req reqCtx) error {
	if err := pr.requests.Put(req); err != nil {
		return err
	}
	pr.notifyWorker()
	return nil
}

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

func (pr *replica) addAdminRequest(req rpc.AdminRequest) error {
	return pr.addRequest(newAdminReqCtx(req))
}

func (pr *replica) onRaftTick(arg interface{}) {
	if !pr.stopRaftTick {
		if err := pr.ticks.Put(struct{}{}); err != nil {
			pr.logger.Info("raft tick stopped")
			return
		}
		metric.SetRaftTickQueueMetric(pr.ticks.Len())
		pr.notifyWorker()
	}

	util.DefaultTimeoutWheel().Schedule(pr.store.cfg.Raft.TickInterval.Duration, pr.onRaftTick, nil)
}

func (pr *replica) onStop() {
	pr.stopOnce.Do(func() {
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
					req.Key = keys.DecodeDataKey(req.Key)
					respStoreNotMatch(errStoreNotMatch, req, c.cb)
				}
			}
		}

		// resp all pending requests in batch and queue
		for _, rr := range pr.pendingReads.reads {
			for _, req := range rr.batch.Requests {
				req.Key = keys.DecodeDataKey(req.Key)
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

		pr.logger.Info("handle serve raft stopped")
	})
}

func (pr *replica) handleEvent() bool {
	if pr.events.Len() == 0 && !pr.events.IsDisposed() {
		return false
	}

	select {
	case <-pr.ctx.Done():
		pr.onStop()
		return false
	default:
	}

	if _, err := pr.events.Get(); err != nil {
		pr.logger.Info("replica stopped")
		pr.onStop()
		return false
	}

	pr.handleMessage(pr.items)
	pr.handleTick(pr.items)
	pr.cacheRaftStatus()
	pr.handleFeedback(pr.items)
	pr.handleRequest(pr.items)

	if pr.rn.HasReadySince(pr.lastReadyIndex) {
		pr.handleReady()
	}

	pr.handleAction(pr.items)
	return true
}

func (pr *replica) cacheRaftStatus() {
	pr.setLeaderPeerID(pr.rn.Status().Lead)
}

func (pr *replica) handleAction(items []interface{}) {
	if size := pr.actions.Len(); size == 0 {
		return
	}

	n, err := pr.actions.Get(readyBatch, items)
	if err != nil {
		return
	}

	for i := int64(0); i < n; i++ {
		a := items[i].(action)
		switch a.actionType {
		case checkSplitAction:
			pr.tryCheckSplit()
		case doSplitAction:
			pr.doSplit(a.splitKeys, a.splitIDs, a.epoch)
		case checkCompactAction:
			pr.doCheckCompact()
		case doCampaignAction:
			if _, err := pr.maybeCampaign(); err != nil {
				pr.logger.Fatal("tail to campaign raft",
					zap.Error(err))
			}
		case heartbeatAction:
			pr.doHeartbeat()
		}
	}

	if pr.actions.Len() > 0 {
		pr.notifyWorker()
	}
}

func (pr *replica) handleMessage(items []interface{}) {
	if size := pr.messages.Len(); size == 0 {
		return
	}

	n, err := pr.messages.Get(readyBatch, items)
	if err != nil {
		return
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
}

func (pr *replica) handleTick(items []interface{}) {
	for {
		if size := pr.ticks.Len(); size == 0 {
			pr.metrics.flush()
			metric.SetRaftTickQueueMetric(size)
			return
		}

		n, err := pr.ticks.Get(readyBatch, items)
		if err != nil {
			return
		}
		for i := int64(0); i < n; i++ {
			if !pr.stopRaftTick {
				pr.rn.Tick()
			}
		}
	}
}

func (pr *replica) handleFeedback(items []interface{}) {
	if size := pr.feedbacks.Len(); size == 0 {
		return
	}

	n, err := pr.feedbacks.Get(readyBatch, items)
	if err != nil {
		return
	}
	for i := int64(0); i < n; i++ {
		if msg, ok := items[i].(raftpb.Message); ok {
			pr.rn.ReportUnreachable(msg.To)
			if msg.Type == raftpb.MsgSnap {
				pr.rn.ReportSnapshot(msg.To, raft.SnapshotFailure)
			}
		}
	}

	size := pr.feedbacks.Len()
	metric.SetRaftReportQueueMetric(size)
	if size > 0 {
		pr.notifyWorker()
	}
}

func (pr *replica) doCheckCompact() {
}

func (pr *replica) doHeartbeat() {
	if !pr.isLeader() {
		return
	}
	req := rpcpb.ResourceHeartbeatReq{
		Term:            pr.rn.BasicStatus().Term,
		Leader:          &pr.replica,
		ContainerID:     pr.store.Meta().ID,
		DownReplicas:    pr.collectDownReplicas(),
		PendingReplicas: pr.collectPendingReplicas(),
		Stats: metapb.ResourceStats{
			WrittenBytes:    pr.writtenBytes,
			WrittenKeys:     pr.writtenKeys,
			ReadBytes:       pr.readBytes,
			ReadKeys:        pr.readKeys,
			ApproximateKeys: pr.approximateKeys,
			ApproximateSize: pr.approximateSize,
			Interval: &metapb.TimeInterval{
				Start: pr.lastHBTime,
				End:   uint64(time.Now().Unix()),
			},
		},
	}
	pr.lastHBTime = req.Stats.Interval.End
	resource := NewResourceAdapterWithShard(pr.getShard())
	// TODO: move this out of the raft worker pool thread if network IO is
	// involved
	if err := pr.store.pd.GetClient().ResourceHeartbeat(resource, req); err != nil {
		pr.logger.Error("fail to send heartbeat to prophet",
			zap.Error(err))
	}
}
