package raftstore

import (
	"time"

	"github.com/coreos/etcd/raft"
	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/beehive/metric"
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/util"
)

const (
	readyBatch = 1024
)

type action int

const (
	checkCompactAction = iota
	checkSplitAction
	doCampaignAction
)

func (pr *peerReplica) addRequest(req reqCtx) error {
	err := pr.requests.Put(req)
	if err != nil {
		return err
	}

	pr.addEvent()
	return nil
}

func (pr *peerReplica) addAction(act action) {
	err := pr.actions.Put(act)
	if err != nil {
		return
	}

	pr.addEvent()
}

func (pr *peerReplica) addReport(report interface{}) {
	err := pr.reports.Put(report)
	if err != nil {
		logger.Infof("shard %d raft report stopped",
			pr.shardID)
		return
	}

	pr.addEvent()
}

func (pr *peerReplica) addEvent() (bool, error) {
	return pr.events.Offer(struct{}{})
}

func (pr *peerReplica) addApplyResult(result asyncApplyResult) {
	err := pr.applyResults.Put(result)
	if err != nil {
		logger.Infof("shard %d raft apply result stopped",
			pr.shardID)
		return
	}

	pr.addEvent()
}

func (pr *peerReplica) step(msg etcdraftpb.Message) {
	err := pr.steps.Put(msg)
	if err != nil {
		logger.Infof("shard %d raft step stopped",
			pr.shardID)
		return
	}

	pr.addEvent()
}

func (pr *peerReplica) onAdmin(req *raftcmdpb.AdminRequest) error {
	r := reqCtx{}
	r.admin = req
	return pr.addRequest(r)
}

func (pr *peerReplica) onRaftTick(arg interface{}) {
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

	util.DefaultTimeoutWheel().Schedule(pr.store.opts.raftTickDuration, pr.onRaftTick, nil)
}

func (pr *peerReplica) doubleCheck() bool {
	if pr.ticks.Len() > 0 {
		return true
	}

	if pr.steps.Len() > 0 {
		return true
	}
	if pr.reports.Len() > 0 {
		return true
	}
	if pr.applyResults.Len() > 0 {
		return true
	}
	if pr.requests.Len() > 0 {
		return true
	}
	if pr.actions.Len() > 0 {
		return true
	}

	return !pr.ps.isApplyingSnapshot() && pr.rn.HasReadySince(pr.ps.lastReadyIndex)
}

func (pr *peerReplica) handleEvent() bool {
	if pr.events.Len() == 0 && !pr.events.IsDisposed() {
		if !pr.doubleCheck() {
			return false
		}
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
						req.Key = DecodeDataKey(req.Key)
						respStoreNotMatch(errStoreNotMatch, req, c.cb)
					}
				}
			}

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
			pr.store.prStopped()
		})
		return false
	}

	pr.handleStep(pr.items)
	pr.handleTick(pr.items)
	pr.handleReport(pr.items)
	pr.handleApplyResult(pr.items)
	pr.handleRequest(pr.items)

	if pr.rn.HasReadySince(pr.ps.lastReadyIndex) {
		pr.handleReady()
	}

	pr.handleAction(pr.items)
	return true
}

func (pr *peerReplica) handleAction(items []interface{}) {
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
		switch a {
		case checkSplitAction:
			pr.doCheckSplit()
		case checkCompactAction:
			pr.doCheckCompact()
		case doCampaignAction:
			_, err := pr.maybeCampaign()
			if err != nil {
				logger.Fatalf("shard %d new split campaign failed, newShard=<%+v> errors:\n %+v",
					pr.shardID,
					pr.ps.shard,
					err)
			}
		}
	}

	if pr.actions.Len() > 0 {
		pr.addEvent()
	}
}

func (pr *peerReplica) handleStep(items []interface{}) {
	size := pr.steps.Len()
	if size == 0 {
		return
	}

	n, err := pr.steps.Get(readyBatch, items)
	if err != nil {
		return
	}

	for i := int64(0); i < n; i++ {
		msg := items[i].(etcdraftpb.Message)
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

func (pr *peerReplica) handleTick(items []interface{}) {
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

func (pr *peerReplica) handleReport(items []interface{}) {
	size := pr.reports.Len()
	if size == 0 {
		return
	}

	n, err := pr.reports.Get(readyBatch, items)
	if err != nil {
		return
	}

	for i := int64(0); i < n; i++ {
		if msg, ok := items[i].(etcdraftpb.Message); ok {
			pr.rn.ReportUnreachable(msg.To)
			if msg.Type == etcdraftpb.MsgSnap {
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

func (pr *peerReplica) doCheckSplit() {
	for id, p := range pr.rn.Status().Progress {
		// If a peer is apply snapshot, skip split, avoid sent snapshot again in future.
		if p.State == raft.ProgressStateSnapshot {
			logger.Infof("shard %d peer %d is applying snapshot",
				pr.shardID,
				id)
			return
		}
	}

	err := pr.startSplitCheckJob()
	if err != nil {
		logger.Fatalf("shard %d add split check job failed, errors:\n %+v",
			pr.shardID,
			err)
		return
	}

	pr.sizeDiffHint = 0
}

func (pr *peerReplica) doCheckCompact() {
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

	// When an election happened or a new peer is added, replicated_idx can be 0.
	if replicatedIdx > 0 {
		lastIdx := pr.rn.LastIndex()
		if lastIdx < replicatedIdx {
			logger.Fatalf("shard %d expect last index >= replicated index, last=<%d> replicated=<%d>",
				pr.shardID,
				lastIdx,
				replicatedIdx)
		}

		metric.ObserveRaftLogLag(lastIdx - replicatedIdx)
	}

	var compactIdx uint64
	appliedIdx := pr.ps.getAppliedIndex()
	firstIdx, _ := pr.ps.FirstIndex()

	if replicatedIdx < firstIdx ||
		replicatedIdx-firstIdx <= pr.store.opts.raftThresholdCompactLog {
		return
	}

	if !pr.disableCompactProtect &&
		appliedIdx > firstIdx &&
		appliedIdx-firstIdx >= pr.store.opts.maxRaftLogCountToForceCompact {
		compactIdx = appliedIdx
	} else if !pr.disableCompactProtect &&
		pr.raftLogSizeHint >= pr.store.opts.maxRaftLogBytesToForceCompact {
		compactIdx = appliedIdx
	} else {
		compactIdx = replicatedIdx
	}

	// Have no idea why subtract 1 here, but original code did this by magic.
	if compactIdx == 0 {
		logger.Fatal("shard %d unexpect compactIdx",
			pr.shardID)
	}

	// avoid leader send snapshot to the a little lag peer.
	if !pr.disableCompactProtect {
		if compactIdx > replicatedIdx {
			if (compactIdx - replicatedIdx) <= pr.store.opts.maxRaftLogCompactProtectLag {
				compactIdx = replicatedIdx
			} else {
				logger.Infof("shard %d peer lag is too large, maybe sent a snapshot later. lag=<%d>",
					pr.shardID,
					compactIdx-replicatedIdx)
			}
		}
	}

	compactIdx--
	if compactIdx < firstIdx {
		// In case compactIdx == firstIdx before subtraction.
		return
	}

	term, _ := pr.rn.Term(compactIdx)
	req := new(raftcmdpb.AdminRequest)
	req.CmdType = raftcmdpb.CompactRaftLog
	req.Compact = &raftcmdpb.CompactRaftLogRequest{
		CompactIndex: compactIdx,
		CompactTerm:  term,
	}

	pr.onAdmin(req)
}
