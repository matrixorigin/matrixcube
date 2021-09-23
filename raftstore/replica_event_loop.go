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
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/util"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/raft/v3/tracker"
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
	err := pr.requests.Put(req)
	if err != nil {
		return err
	}

	pr.addEvent()
	return nil
}

func (pr *replica) addAction(act action) {
	err := pr.actions.Put(act)
	if err != nil {
		return
	}

	pr.addEvent()
}

func (pr *replica) addReport(report interface{}) {
	err := pr.reports.Put(report)
	if err != nil {
		logger.Infof("shard %d raft report stopped",
			pr.shardID)
		return
	}

	pr.addEvent()
}

func (pr *replica) addEvent() (bool, error) {
	ok, err := pr.events.Offer(struct{}{})
	pr.notifyWorker()
	return ok, err
}

func (pr *replica) addApplyResult(result asyncApplyResult) {
	err := pr.applyResults.Put(result)
	if err != nil {
		logger.Infof("shard %d raft apply result stopped",
			pr.shardID)
		return
	}

	pr.addEvent()
}

func (pr *replica) step(msg raftpb.Message) {
	err := pr.steps.Put(msg)
	if err != nil {
		logger.Infof("shard %d raft step stopped",
			pr.shardID)
		return
	}

	pr.addEvent()
}

func (pr *replica) onAdmin(req *rpc.AdminRequest) error {
	r := reqCtx{}
	r.admin = req
	return pr.addRequest(r)
}

func (pr *replica) onRaftTick(arg interface{}) {
	if !pr.stopRaftTick {
		err := pr.ticks.Put(struct{}{})
		if err != nil {
			logger.Infof("shard %d raft tick stopped",
				pr.shardID)
			return
		}

		metric.SetRaftTickQueueMetric(pr.ticks.Len())
		pr.addEvent()
	}

	util.DefaultTimeoutWheel().Schedule(pr.store.cfg.Raft.TickInterval.Duration, pr.onRaftTick, nil)
}

func (pr *replica) handleEvent() bool {
	if pr.events.Len() == 0 && !pr.events.IsDisposed() {
		return false
	}

	stop := false
	select {
	case <-pr.ctx.Done():
		stop = true
	default:
	}

	_, err := pr.events.Get()
	if err != nil || stop {
		pr.stopOnce.Do(func() {
			pr.metrics.flush()
			pr.actions.Dispose()
			pr.ticks.Dispose()
			pr.steps.Dispose()
			pr.reports.Dispose()
			pr.applyResults.Dispose()

			// resp all stale requests in batch and queue
			for {
				if pr.batch.isEmpty() {
					break
				}
				if c, ok := pr.batch.pop(); ok {
					for _, req := range c.req.Requests {
						req.Key = keys.DecodeDataKey(req.Key)
						respStoreNotMatch(errStoreNotMatch, req, c.cb)
					}
				}
			}

			// resp all pending requests in batch and queue
			for _, c := range pr.pendingReads.reads {
				for _, req := range c.req.Requests {
					req.Key = keys.DecodeDataKey(req.Key)
					respStoreNotMatch(errStoreNotMatch, req, pr.store.cb)
				}
			}
			pr.pendingReads.reset()

			requests := pr.requests.Dispose()
			for _, r := range requests {
				req := r.(reqCtx)
				if req.cb != nil {
					respStoreNotMatch(errStoreNotMatch, req.req, req.cb)
				}

				pb.ReleaseRequest(req.req)
			}

			logger.Infof("shard %d handle serve raft stopped",
				pr.shardID)
		})
		return false
	}

	pr.handleStep(pr.items)
	pr.handleTick(pr.items)
	pr.cacheRaftStatus()
	pr.handleReport(pr.items)
	pr.handleApplyResult(pr.items)
	pr.handleRequest(pr.items)

	if pr.rn.HasReadySince(pr.lastReadyIndex) {
		pr.handleReady()
	}

	pr.handleAction(pr.items)
	return true
}

func (pr *replica) cacheRaftStatus() {
	st := pr.rn.Status()
	pr.setLeaderPeerID(st.Lead)
}

func (pr *replica) handleAction(items []interface{}) {
	size := pr.actions.Len()
	if size == 0 {
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
			pr.doCheckSplit()
		case doSplitAction:
			pr.doSplit(a.splitKeys, a.splitIDs, a.epoch)
		case checkCompactAction:
			pr.doCheckCompact()
		case doCampaignAction:
			_, err := pr.maybeCampaign()
			if err != nil {
				logger.Fatalf("shard %d new split campaign failed with %+v",
					pr.shardID,
					err)
			}
		case heartbeatAction:
			pr.doHeartbeat()
		}
	}

	if pr.actions.Len() > 0 {
		pr.addEvent()
	}
}

func (pr *replica) handleStep(items []interface{}) {
	size := pr.steps.Len()
	if size == 0 {
		return
	}

	n, err := pr.steps.Get(readyBatch, items)
	if err != nil {
		return
	}

	for i := int64(0); i < n; i++ {
		msg := items[i].(raftpb.Message)
		if pr.isLeader() && msg.From != 0 {
			pr.peerHeartbeatsMap.Store(msg.From, time.Now())
		}

		err := pr.rn.Step(msg)
		if err != nil {
			logger.Errorf("shard %d step failed with %+v",
				pr.shardID,
				err)
		}

		if logger.DebugEnabled() {
			if len(msg.Entries) > 0 {
				logger.Debugf("shard %d step raft", pr.shardID)
			}
		}
	}

	size = pr.steps.Len()
	metric.SetRaftStepQueueMetric(size)

	if size > 0 {
		pr.addEvent()
	}
}

func (pr *replica) handleTick(items []interface{}) {
	for {
		size := pr.ticks.Len()
		if size == 0 {
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

func (pr *replica) handleReport(items []interface{}) {
	size := pr.reports.Len()
	if size == 0 {
		return
	}

	n, err := pr.reports.Get(readyBatch, items)
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

	size = pr.reports.Len()
	metric.SetRaftReportQueueMetric(size)

	if size > 0 {
		pr.addEvent()
	}
}

func (pr *replica) doCheckSplit() {
	if !pr.isLeader() {
		return
	}

	if pr.store.runner.IsNamedWorkerBusy(splitCheckWorkerName) {
		return
	}

	for id, p := range pr.rn.Status().Progress {
		// If a peer is apply snapshot, skip split, avoid sent snapshot again in future.
		if p.State == tracker.StateSnapshot {
			logger.Infof("shard %d peer %d is applying snapshot",
				pr.shardID,
				id)
			return
		}
	}

	err := pr.startSplitCheckJob()
	if err != nil {
		logger.Fatalf("shard %d add split check job failed with %+v",
			pr.shardID,
			err)
		return
	}

	pr.sizeDiffHint = 0
}

func (pr *replica) doSplit(splitKeys [][]byte, splitIDs []rpcpb.SplitID, epoch metapb.ResourceEpoch) {
	if !pr.isLeader() {
		return
	}

	current := pr.getShard()
	if current.Epoch.Version != epoch.Version {
		logger.Infof("shard %d epoch changed, need re-check later, current=<%+v> split=<%+v>",
			pr.shardID,
			current.Epoch,
			epoch)
		return
	}

	req := &rpc.AdminRequest{
		CmdType: rpc.AdminCmdType_BatchSplit,
		Splits:  &rpc.BatchSplitRequest{},
	}

	for idx := range splitIDs {
		req.Splits.Requests = append(req.Splits.Requests, rpc.SplitRequest{
			SplitKey:   splitKeys[idx],
			NewShardID: splitIDs[idx].NewID,
			NewPeerIDs: splitIDs[idx].NewPeerIDs,
		})
	}
	pr.onAdmin(req)
}

func (pr *replica) doCheckCompact() {
}

func (pr *replica) doHeartbeat() {
	if !pr.isLeader() {
		return
	}
	req := rpcpb.ResourceHeartbeatReq{}
	req.Term = pr.rn.BasicStatus().Term
	req.Leader = &pr.peer
	req.ContainerID = pr.store.Meta().ID
	req.DownPeers = pr.collectDownPeers()
	req.PendingPeers = pr.collectPendingPeers()
	req.Stats.WrittenBytes = pr.writtenBytes
	req.Stats.WrittenKeys = pr.writtenKeys
	req.Stats.ReadBytes = pr.readBytes
	req.Stats.ReadKeys = pr.readKeys
	req.Stats.ApproximateKeys = pr.approximateKeys
	req.Stats.ApproximateSize = pr.approximateSize
	req.Stats.Interval = &metapb.TimeInterval{
		Start: pr.lastHBTime,
		End:   uint64(time.Now().Unix()),
	}
	pr.lastHBTime = req.Stats.Interval.End

	err := pr.store.pd.GetClient().ResourceHeartbeat(NewResourceAdapterWithShard(pr.getShard()), req)
	if err != nil {
		logger.Errorf("shard %d heartbeat to prophet failed with %+v",
			pr.shardID,
			err)
	}
}
