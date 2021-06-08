package raftstore

import (
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/util"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/raft/tracker"
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

func (pr *peerReplica) step(msg raftpb.Message) {
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

	util.DefaultTimeoutWheel().Schedule(pr.store.cfg.Raft.TickInterval.Duration, pr.onRaftTick, nil)
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
				logger.Fatalf("shard %d new split campaign failed, newShard=<%+v> errors:\n %+v",
					pr.shardID,
					pr.ps.shard,
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

func (pr *peerReplica) doCheckSplit() {
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
		logger.Fatalf("shard %d add split check job failed, errors:\n %+v",
			pr.shardID,
			err)
		return
	}

	pr.sizeDiffHint = 0
}

func (pr *peerReplica) doSplit(splitKeys [][]byte, splitIDs []rpcpb.SplitID, epoch metapb.ResourceEpoch) {
	if !pr.isLeader() {
		return
	}

	current := pr.ps.shard
	if current.Epoch.Version != epoch.Version {
		logger.Infof("shard %d epoch changed, need re-check later, current=<%+v> split=<%+v>",
			pr.shardID,
			current.Epoch,
			epoch)
		return
	}

	req := &raftcmdpb.AdminRequest{
		CmdType: raftcmdpb.AdminCmdType_BatchSplit,
		Splits:  &raftcmdpb.BatchSplitRequest{},
	}

	for idx := range splitIDs {
		req.Splits.Requests = append(req.Splits.Requests, raftcmdpb.SplitRequest{
			SplitKey:   splitKeys[idx],
			NewShardID: splitIDs[idx].NewID,
			NewPeerIDs: splitIDs[idx].NewPeerIDs,
		})
	}
	pr.onAdmin(req)
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
		replicatedIdx-firstIdx <= pr.store.cfg.Raft.RaftLog.CompactThreshold {
		return
	}

	if !pr.disableCompactProtect &&
		appliedIdx > firstIdx &&
		appliedIdx-firstIdx >= pr.store.cfg.Raft.RaftLog.ForceCompactCount {
		compactIdx = appliedIdx
	} else if !pr.disableCompactProtect &&
		pr.raftLogSizeHint >= pr.store.cfg.Raft.RaftLog.ForceCompactBytes {
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
			if (compactIdx - replicatedIdx) <= pr.store.cfg.Raft.RaftLog.CompactProtectLag {
				compactIdx = replicatedIdx
			} else {
				logger.Infof("shard %d peer lag is too large, maybe sent a snapshot later. lag=<%d>",
					pr.shardID,
					compactIdx-replicatedIdx)
			}
		}
	}

	compactIdx--

	if pr.store.cfg.Customize.CustomAdjustCompactFuncFactory != nil {
		if fn := pr.store.cfg.Customize.CustomAdjustCompactFuncFactory(pr.ps.shard.Group); fn != nil {
			idx, err := fn(pr.ps.shard, compactIdx)
			if err != nil {
				logger.Errorf("shard %d adjust compact idx %d failed with %+v",
					pr.shardID,
					compactIdx,
					err)
				return
			}

			if idx > compactIdx {
				logger.Fatalf("shard %d adjust compact idx %d failed, invalid adjust idx %d",
					pr.shardID,
					compactIdx,
					idx,
					err)
				return
			}

			logger.Infof("shard %d compact idx %d updated to %d",
				pr.shardID,
				compactIdx,
				idx,
				err)
			compactIdx = idx
		}
	}

	if compactIdx < firstIdx {
		// In case compactIdx == firstIdx before subtraction.
		return
	}

	term, _ := pr.rn.Term(compactIdx)
	req := new(raftcmdpb.AdminRequest)
	req.CmdType = raftcmdpb.AdminCmdType_CompactLog
	req.CompactLog = &raftcmdpb.CompactLogRequest{
		CompactIndex: compactIdx,
		CompactTerm:  term,
	}

	pr.onAdmin(req)
}

func (pr *peerReplica) doHeartbeat() {
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

	err := pr.store.pd.GetClient().ResourceHeartbeat(NewResourceAdapterWithShard(pr.ps.shard), req)
	if err != nil {
		logger.Errorf("shard %d heartbeat to prophet failed with %+v",
			pr.shardID,
			err)
	}
}
