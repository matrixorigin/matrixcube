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
	"time"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
	"github.com/matrixorigin/matrixcube/util"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

var (
	// testMaxOnceCommitEntryCount how many submitted entries are processed each time. 0 is unlimited
	testMaxOnceCommitEntryCount = 0
)

type readyContext struct {
	hardState  raftpb.HardState
	raftState  bhraftpb.RaftLocalState
	applyState bhraftpb.RaftApplyState
	lastTerm   uint64
	snap       *bhraftpb.SnapshotMessage
	wb         *util.WriteBatch
}

func (ctx *readyContext) reset() {
	ctx.hardState = raftpb.HardState{}
	ctx.raftState = bhraftpb.RaftLocalState{}
	ctx.applyState = bhraftpb.RaftApplyState{}
	ctx.lastTerm = 0
	ctx.snap = nil
	ctx.wb.Reset()
}

type applySnapResult struct {
	prev    bhmetapb.Shard
	current bhmetapb.Shard
}

// handle raft ready will do these things:
// 1. append raft log
// 2. send raft message to followers
// 3. apply raft log
// 4. exec read index request
func (pr *peerReplica) handleReady() {
	// If we continue to handle all the messages, it may cause too many messages because
	// leader will send all the remaining messages to this follower, which can lead
	// to full message queue under high load.
	if pr.ps.isApplyingSnapshot() {
		logger.Debugf("shard %d still applying snapshot, skip further handling",
			pr.shardID)
		return
	}

	pr.ps.resetApplyingSnapJob()

	// wait apply committed entries complete
	if pr.rn.HasPendingSnapshot() &&
		!pr.ps.isApplyComplete() {
		logger.Debugf("shard %d apply index and committed index not match, skip applying snapshot, apply=<%d> commit=<%d>",
			pr.shardID,
			pr.ps.getAppliedIndex(),
			pr.ps.getCommittedIndex())
		return
	}

	rd := pr.rn.ReadySince(pr.ps.lastReadyIndex)
	ctx := pr.readyCtx
	ctx.reset()

	// If snapshot is received, further handling
	if !raft.IsEmptySnap(rd.Snapshot) {
		ctx.snap = &bhraftpb.SnapshotMessage{}
		protoc.MustUnmarshal(ctx.snap, rd.Snapshot.Data)
		if !pr.stopRaftTick {
			// When we apply snapshot, stop raft tick and resume until the snapshot applied
			pr.stopRaftTick = true
		}

		if !pr.store.snapshotManager.Exists(ctx.snap) {
			logger.Infof("shard %d receiving snapshot, skip further handling",
				pr.shardID)
			return
		}
	}

	ctx.hardState = pr.ps.raftLocalState.HardState
	ctx.raftState = pr.ps.raftLocalState
	ctx.applyState = pr.ps.raftApplyState
	ctx.lastTerm = pr.ps.lastTerm

	pr.handleRaftReadyAppend(ctx, &rd)
	pr.handleRaftReadyApply(ctx, &rd)
}

// ====================== append raft log methods

func (pr *peerReplica) handleRaftReadyAppend(ctx *readyContext, rd *raft.Ready) {
	start := time.Now()

	// If we become leader, send heartbeat to pd
	if rd.SoftState != nil {
		if rd.SoftState.RaftState == raft.StateLeader {
			logger.Infof("shard %d ********become leader now********",
				pr.shardID)
			pr.addAction(action{actionType: heartbeatAction})
			pr.resetBatch()
			if pr.store.aware != nil {
				pr.store.aware.BecomeLeader(pr.ps.shard)
			}
		} else {
			if pr.store.aware != nil {
				pr.store.aware.BecomeFollower(pr.ps.shard)
			}
		}
	}

	// The leader can write to disk and replicate to the followers concurrently
	// For more details, check raft thesis 10.2.1.
	if pr.isLeader() {
		pr.send(rd.Messages)
	}

	ctx.wb.Reset()
	pr.handleAppendSnapshot(ctx, rd)
	pr.handleAppendEntries(ctx, rd)

	if ctx.raftState.LastIndex > 0 && !raft.IsEmptyHardState(rd.HardState) {
		ctx.hardState = rd.HardState
	}

	pr.doSaveRaftState(ctx)
	pr.doSaveApplyState(ctx)

	err := pr.store.MetadataStorage().Write(ctx.wb, !pr.store.cfg.Raft.RaftLog.DisableSync)
	if err != nil {
		logger.Fatalf("shard %d handle raft ready failure, errors\n %+v",
			pr.shardID,
			err)
	}

	metric.ObserveRaftLogAppendDuration(start)
}

func (pr *peerReplica) handleAppendSnapshot(ctx *readyContext, rd *raft.Ready) {
	if !raft.IsEmptySnap(rd.Snapshot) {
		err := pr.doAppendSnapshot(ctx, rd.Snapshot)
		if err != nil {
			logger.Fatalf("shard %d handle raft ready failed with %+v",
				pr.ps.shard.ID,
				err)
		}

		pr.metrics.ready.snapshort++
	}
}

func (pr *peerReplica) doAppendSnapshot(ctx *readyContext, snap raftpb.Snapshot) error {
	logger.Infof("shard %d begin to apply snapshot",
		pr.shardID)

	if ctx.snap.Header.Shard.ID != pr.shardID {
		return fmt.Errorf("shard %d not match, snapShard=<%d> currShard=<%d>",
			pr.shardID,
			ctx.snap.Header.Shard.ID,
			pr.shardID)
	}

	if pr.ps.isInitialized() {
		err := pr.ps.store.clearMeta(pr.shardID, ctx.wb)
		if err != nil {
			logger.Errorf("shard %d clear meta failed with %+v",
				pr.shardID,
				err)
			return err
		}
	}

	err := pr.store.updatePeerState(ctx.snap.Header.Shard, bhraftpb.PeerState_Applying, ctx.wb)
	if err != nil {
		logger.Errorf("shard %d write peer state failed with %+v",
			pr.shardID,
			err)
		return err
	}

	lastIndex := snap.Metadata.Index
	lastTerm := snap.Metadata.Term

	ctx.raftState.LastIndex = lastIndex
	ctx.applyState.AppliedIndex = lastIndex
	ctx.lastTerm = lastTerm

	// The snapshot only contains log which index > applied index, so
	// here the truncate state's (index, term) is in snapshot metadata.
	ctx.applyState.TruncatedState.Index = lastIndex
	ctx.applyState.TruncatedState.Term = lastTerm

	logger.Infof("shard %d apply snapshot state completed, apply state %+v",
		pr.shardID,
		ctx.applyState)
	return nil
}

func (pr *peerReplica) handleAppendEntries(ctx *readyContext, rd *raft.Ready) {
	if len(rd.Entries) > 0 {
		err := pr.doAppendEntries(ctx, rd.Entries)
		if err != nil {
			logger.Fatalf("shard %d handle raft ready failed with %+v",
				pr.ps.shard.ID,
				err)
		}

		pr.metrics.ready.append++
	}
}

// doAppendEntries the given entries to the raft log using previous last index or self.last_index.
// Return the new last index for later update. After we commit in engine, we can set last_index
// to the return one.
func (pr *peerReplica) doAppendEntries(ctx *readyContext, entries []raftpb.Entry) error {
	c := len(entries)
	if c == 0 {
		return nil
	}

	prevLastIndex := ctx.raftState.LastIndex
	lastIndex := entries[c-1].Index
	lastTerm := entries[c-1].Term

	for _, e := range entries {
		d := protoc.MustMarshal(&e)
		err := ctx.wb.Set(getRaftLogKey(pr.shardID, e.Index), d)
		if err != nil {
			logger.Fatalf("shard %d append entry <%s> failed with %+v",
				pr.shardID,
				e.String(),
				err)
			return err
		}
	}

	// Delete any previously appended log entries which never committed.
	for index := lastIndex + 1; index < prevLastIndex+1; index++ {
		err := ctx.wb.Delete(getRaftLogKey(pr.shardID, index))
		if err != nil {
			logger.Fatalf("shard %d delete any previously appended log entries %d failed with %+v",
				pr.shardID,
				index,
				err)
			return err
		}
	}

	ctx.raftState.LastIndex = lastIndex
	ctx.lastTerm = lastTerm

	return nil
}

func (pr *peerReplica) doSaveRaftState(ctx *readyContext) {
	if ctx.raftState.LastIndex != pr.ps.raftLocalState.LastIndex ||
		ctx.hardState.Commit != pr.ps.raftLocalState.HardState.Commit ||
		ctx.hardState.Term != pr.ps.raftLocalState.HardState.Term ||
		ctx.hardState.Vote != pr.ps.raftLocalState.HardState.Vote {

		ctx.raftState.HardState = ctx.hardState
		err := ctx.wb.Set(getRaftLocalStateKey(pr.shardID), protoc.MustMarshal(&ctx.raftState))
		if err != nil {
			logger.Fatalf("shard %d handle raft ready failed with %+v",
				pr.ps.shard.ID,
				err)
		}
	}
}

func (pr *peerReplica) doSaveApplyState(ctx *readyContext) {
	tmp := ctx.applyState
	origin := pr.ps.raftApplyState

	if tmp.AppliedIndex != origin.AppliedIndex ||
		tmp.TruncatedState.Index != origin.TruncatedState.Index ||
		tmp.TruncatedState.Term != origin.TruncatedState.Term {
		err := ctx.wb.Set(getRaftApplyStateKey(pr.shardID), protoc.MustMarshal(&ctx.applyState))
		if err != nil {
			logger.Fatalf("shard %d handle raft ready failed with %+v",
				pr.ps.shard.ID,
				err)
		}
	}
}

// ====================== apply raft log methods

func (pr *peerReplica) handleRaftReadyApply(ctx *readyContext, rd *raft.Ready) {
	if ctx.snap != nil {
		// When apply snapshot, there is no log applied and not compacted yet.
		pr.raftLogSizeHint = 0
	}

	result := pr.doApplySnapshot(ctx, rd)
	if !pr.isLeader() {
		pr.send(rd.Messages)
	}

	if result != nil {
		pr.startRegistrationJob()
	}

	pr.applyCommittedEntries(rd)

	pr.doApplyReads(rd)

	if result != nil {
		pr.doUpdateKeyRange(result)
	}

	pr.rn.AdvanceAppend(*rd)
	if result != nil {
		// Because we only handle raft ready when not applying snapshot, so following
		// line won't be called twice for the same snapshot.
		pr.rn.AdvanceApply(pr.ps.lastReadyIndex)
	}
}

func (pr *peerReplica) doApplySnapshot(ctx *readyContext, rd *raft.Ready) *applySnapResult {
	pr.ps.raftLocalState = ctx.raftState
	pr.ps.raftLocalState.HardState = ctx.hardState
	pr.ps.raftApplyState = ctx.applyState
	pr.ps.lastTerm = ctx.lastTerm

	// If we apply snapshot ok, we should update some infos like applied index too.
	if ctx.snap == nil {
		return nil
	}

	// cleanup data before apply snap job
	if pr.ps.isInitialized() {
		err := pr.store.clearExtraData(pr.applyWorker, pr.ps.shard, ctx.snap.Header.Shard)
		if err != nil {
			// No need panic here, when applying snapshot, the deletion will be tried
			// again. But if the shard range changes, like [a, c) -> [a, b) and [b, c),
			// [b, c) will be kept in rocksdb until a covered snapshot is applied or
			// store is restarted.
			logger.Errorf("shard %d cleanup data failed with %+v",
				pr.shardID,
				err)
			return nil
		}
	}

	pr.startApplyingSnapJob()

	// remove pending snapshots for sending
	removedPeers(ctx.snap.Header.Shard, pr.ps.shard)

	prev := pr.ps.shard
	pr.ps.shard = ctx.snap.Header.Shard

	return &applySnapResult{
		prev:    prev,
		current: pr.ps.shard,
	}
}

func (pr *peerReplica) applyCommittedEntries(rd *raft.Ready) {
	if pr.ps.isApplyingSnapshot() {
		pr.ps.lastReadyIndex = pr.ps.getTruncatedIndex()
	} else {
		if testMaxOnceCommitEntryCount > 0 &&
			testMaxOnceCommitEntryCount < len(rd.CommittedEntries) {
			rd.CommittedEntries = rd.CommittedEntries[:testMaxOnceCommitEntryCount]
		}

		for _, entry := range rd.CommittedEntries {
			pr.raftLogSizeHint += uint64(len(entry.Data))
		}

		if len(rd.CommittedEntries) > 0 {
			pr.ps.lastReadyIndex = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
			err := pr.startApplyCommittedEntriesJob(pr.shardID, pr.getCurrentTerm(), rd.CommittedEntries)
			if err != nil {
				logger.Fatalf("shard %d add apply committed entries job failed with %+v",
					pr.shardID,
					err)
			}

			pr.metrics.ready.commit++
		}
	}
}

func (pr *peerReplica) doApplyReads(rd *raft.Ready) {
	if pr.readyToHandleRead() {
		for _, state := range rd.ReadStates {
			pr.pendingReads.ready(state)
		}

		if len(rd.ReadStates) > 0 {
			pr.maybeExecRead()
		}
	}

	// Note that only after handle read_states can we identify what requests are
	// actually stale.
	if rd.SoftState != nil {
		if rd.SoftState.RaftState != raft.StateLeader {
			// all uncommitted reads will be dropped silently in raft.
			for _, c := range pr.pendingReads.reads {
				c.resp(errorStaleCMDResp(c.getUUID(), pr.getCurrentTerm()))
			}
			pr.pendingReads.reset()

			// we are not leader now, so all writes in the batch is actually stale
			for i := 0; i < pr.batch.size(); i++ {
				if c, ok := pr.batch.pop(); ok {
					c.resp(errorStaleCMDResp(c.getUUID(), pr.getCurrentTerm()))
				}
			}
			pr.resetBatch()
		}
	}
}

func (pr *peerReplica) send(msgs []raftpb.Message) {
	for _, msg := range msgs {
		err := pr.sendRaftMsg(msg)
		if err != nil {
			// We don't care that the message is sent failed, so here just log this error
			logger.Debugf("shard %d send msg failure, from_peer=<%d> to_peer=<%d>, errors:\n%s",
				pr.shardID,
				msg.From,
				msg.To,
				err)
		}
		pr.metrics.ready.message++
	}
}

func (pr *peerReplica) sendRaftMsg(msg raftpb.Message) error {
	sendMsg := pb.AcquireRaftMessage()
	sendMsg.ShardID = pr.shardID
	sendMsg.ShardEpoch = pr.ps.shard.Epoch
	sendMsg.Group = pr.ps.shard.Group
	sendMsg.DisableSplit = pr.ps.shard.DisableSplit
	sendMsg.Unique = pr.ps.shard.Unique
	sendMsg.RuleGroups = pr.ps.shard.RuleGroups
	sendMsg.From = pr.peer
	sendMsg.To, _ = pr.store.getPeer(msg.To)
	if sendMsg.To.ID == 0 {
		return fmt.Errorf("can not found peer<%d>", msg.To)
	}

	// There could be two cases:
	// 1. Target peer already exists but has not established communication with leader yet
	// 2. Target peer is added newly due to member change or shard split, but it's not
	//    created yet
	// For both cases the shard start key and end key are attached in RequestVote and
	// Heartbeat message for the store of that peer to check whether to create a new peer
	// when receiving these messages, or just to wait for a pending shard split to perform
	// later.
	if pr.ps.isInitialized() &&
		(msg.Type == raftpb.MsgVote ||
			// the peer has not been known to this leader, it may exist or not.
			(msg.Type == raftpb.MsgHeartbeat && msg.Commit == 0)) {
		sendMsg.Start = pr.ps.shard.Start
		sendMsg.End = pr.ps.shard.End
	}

	sendMsg.Message = msg
	pr.store.trans.Send(sendMsg)

	switch msg.Type {
	case raftpb.MsgApp:
		pr.metrics.message.append++
	case raftpb.MsgAppResp:
		pr.metrics.message.appendResp++
	case raftpb.MsgVote:
		pr.metrics.message.vote++
	case raftpb.MsgVoteResp:
		pr.metrics.message.voteResp++
	case raftpb.MsgSnap:
		pr.rn.ReportSnapshot(msg.To, raft.SnapshotFinish)
		pr.metrics.message.snapshot++
	case raftpb.MsgHeartbeat:
		pr.metrics.message.heartbeat++
	case raftpb.MsgHeartbeatResp:
		pr.metrics.message.heartbeatResp++
	case raftpb.MsgTransferLeader:
		pr.metrics.message.transfeLeader++
	}

	return nil
}

func (pr *peerReplica) doUpdateKeyRange(result *applySnapResult) {
	logger.Infof("shard %d snapshot is applied, shard=<%+v>",
		pr.shardID,
		result.current)

	if len(result.prev.Peers) > 0 {
		logger.Infof("shard %d shard changed after apply snapshot, from=<%+v> to=<%+v>",
			pr.shardID,
			result.prev,
			result.current)
		pr.store.removeShardKeyRange(result.prev)
	}

	pr.store.updateShardKeyRange(result.current)
}
