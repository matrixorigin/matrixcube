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

	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/meta"
)

var (
	// testMaxOnceCommitEntryCount defines how many committed entries can be
	// applied each time. 0 means unlimited
	testMaxOnceCommitEntryCount = 0
	// ErrUnknownReplica indicates that the replica is unknown.
	ErrUnknownReplica = errors.New("unknown replica")
)

func (pr *replica) handleReady(wc *logdb.WorkerContext) error {
	if ce := pr.logger.Check(zap.DebugLevel, "begin handleReady"); ce != nil {
		ce.Write(log.ShardIDField(pr.shardID),
			log.ReplicaIDField(pr.replica.ID))
	}

	rd := pr.rn.ReadySince(pr.lastReadyIndex)
	pr.handleRaftState(rd)
	pr.sendRaftAppendLogMessages(rd)
	if err := pr.handleRaftReadyAppend(rd, wc); err != nil {
		return err
	}
	pr.sendRaftMessages(rd)
	rd = pr.limitNumOfEntriesToApply(rd)
	if err := pr.applyCommittedEntries(rd); err != nil {
		return err
	}
	pr.handleReadyToRead(rd)
	pr.rn.AdvanceAppend(rd)

	if ce := pr.logger.Check(zap.DebugLevel, "handleReady completed"); ce != nil {
		ce.Write(log.ShardIDField(pr.shardID),
			log.ReplicaIDField(pr.replica.ID))
	}

	return nil
}

func (pr *replica) handleRaftState(rd raft.Ready) {
	// etcd raft won't repeatedly return the same non-empty soft state
	if rd.SoftState != nil {
		// If we become leader, send heartbeat to pd
		if rd.SoftState.RaftState == raft.StateLeader {
			pr.logger.Info("********become leader now********")
			pr.prophetHeartbeat()
			pr.resetIncomingProposals()
			if pr.aware != nil {
				pr.aware.BecomeLeader(pr.getShard())
			}
		} else {
			pr.logger.Info("********become follower now********")
			if pr.aware != nil {
				pr.aware.BecomeFollower(pr.getShard())
			}
		}
	}
}

func (pr *replica) handleRaftReadyAppend(rd raft.Ready,
	wc *logdb.WorkerContext) error {
	start := time.Now()
	defer metric.ObserveRaftLogAppendDuration(start)
	return pr.handleAppendEntries(rd, wc)
}

func getEstimatedAppendSize(rd raft.Ready) int {
	sz := 0
	for _, e := range rd.Entries {
		sz += len(e.Data)
		sz += 24
	}
	return sz
}

func (pr *replica) handleAppendEntries(rd raft.Ready,
	wc *logdb.WorkerContext) error {
	if len(rd.Entries) > 0 {
		if ce := pr.logger.Check(zap.DebugLevel,
			"begin to save raft state"); ce != nil {
			ce.Write(log.ShardIDField(pr.shardID),
				log.ReplicaIDField(pr.replica.ID),
				log.IndexField(rd.Entries[0].Index),
				zap.Int("estimated-size", getEstimatedAppendSize(rd)))
		}
		pr.lr.Append(rd.Entries)
		pr.metrics.ready.append++
		err := pr.logdb.SaveRaftState(pr.shardID, pr.replica.ID, rd, wc)
		if ce := pr.logger.Check(zap.DebugLevel,
			"save raft state completed"); ce != nil {
			ce.Write(log.ShardIDField(pr.shardID),
				log.ReplicaIDField(pr.replica.ID),
				log.IndexField(rd.Entries[0].Index))
		}
		return err
	}
	return nil
}

func (pr *replica) limitNumOfEntriesToApply(rd raft.Ready) raft.Ready {
	if testMaxOnceCommitEntryCount > 0 &&
		testMaxOnceCommitEntryCount < len(rd.CommittedEntries) {
		rd.CommittedEntries = rd.CommittedEntries[:testMaxOnceCommitEntryCount]
	}
	return rd
}

func (pr *replica) applyCommittedEntries(rd raft.Ready) error {
	for _, entry := range rd.CommittedEntries {
		pr.raftLogSizeHint += uint64(len(entry.Data))
	}

	if len(rd.CommittedEntries) > 0 {
		pr.lastReadyIndex = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
		if err := pr.doApplyCommittedEntries(rd.CommittedEntries); err != nil {
			return err
		}
		pr.metrics.ready.commit++
	}
	return nil
}

func (pr *replica) handleReadyToRead(rd raft.Ready) {
	for _, state := range rd.ReadStates {
		pr.pendingReads.ready(state)
	}
	if len(rd.ReadStates) > 0 {
		pr.maybeExecRead()
	}
}

func (pr *replica) sendRaftAppendLogMessages(rd raft.Ready) {
	// MsgApp can be immediately sent to followers so leader and followers can
	// concurrently persist the logs to disk. For more details, check raft thesis
	// section 10.2.1.
	pr.send(rd.Messages, true)
}

func (pr *replica) sendRaftMessages(rd raft.Ready) {
	// send all other non-MsgApp messages
	pr.send(rd.Messages, false)
}

func isMsgApp(m raftpb.Message) bool {
	return m.Type == raftpb.MsgApp
}

func (pr *replica) send(msgs []raftpb.Message, msgAppOnly bool) {
	for _, msg := range msgs {
		if isMsgApp(msg) && msgAppOnly {
			pr.sendMessage(msg)
		} else if !isMsgApp(msg) && !msgAppOnly {
			pr.sendMessage(msg)
		}
	}
}

func (pr *replica) sendMessage(msg raftpb.Message) {
	if err := pr.sendRaftMessage(msg); err != nil {
		// We don't care such failed message transmission, just log the error
		pr.logger.Debug("fail to send msg",
			zap.Uint64("from", msg.From),
			zap.Uint64("from", msg.To),
			zap.Error(err))
	}
	pr.metrics.ready.message++
}

func (pr *replica) sendRaftMessage(msg raftpb.Message) error {
	shard := pr.getShard()
	to, ok := pr.getReplicaRecord(msg.To)
	if !ok {
		return errors.Wrapf(ErrUnknownReplica,
			"shardID %d, replicaID: %d", pr.shardID, msg.To)
	}

	m := meta.RaftMessage{
		ShardID:      pr.shardID,
		From:         pr.replica,
		To:           to,
		ShardEpoch:   shard.Epoch,
		Group:        shard.Group,
		DisableSplit: shard.DisableSplit,
		Unique:       shard.Unique,
		RuleGroups:   shard.RuleGroups,
		Message:      msg,
	}

	// There could be two cases:
	// 1. Target replica already exists but has not established communication with
	//    leader yet
	// 2. Target replica is added newly due to member change or shard split, but
	//    it has not been created yet
	// For both cases the shard start key and end key are attached in RequestVote
	// and Heartbeat message for the store of that replica to check whether to
	// create a new replica when receiving these messages, or just to wait for a
	// pending shard split to perform later.
	if len(shard.Replicas) > 0 &&
		(msg.Type == raftpb.MsgVote ||
			// the replica has not been known to this leader, it may exist or not.
			(msg.Type == raftpb.MsgHeartbeat && msg.Commit == 0)) {
		m.Start = shard.Start
		m.End = shard.End
	}

	pr.transport.Send(m)
	// FIXME: this should not be called until the snapshot is actually
	// transmitted to the target replica.
	if msg.Type == raftpb.MsgSnap {
		pr.rn.ReportSnapshot(msg.To, raft.SnapshotFinish)
	}
	pr.updateMessageMetrics(msg)
	return nil
}

func (pr *replica) updateMessageMetrics(msg raftpb.Message) {
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
		pr.metrics.message.snapshot++
	case raftpb.MsgHeartbeat:
		pr.metrics.message.heartbeat++
	case raftpb.MsgHeartbeatResp:
		pr.metrics.message.heartbeatResp++
	case raftpb.MsgTransferLeader:
		pr.metrics.message.transferLeader++
	}
}

func (pr *replica) doApplyCommittedEntries(entries []raftpb.Entry) error {
	pr.logger.Debug("begin to apply raft log",
		zap.Int("count", len(entries)))

	pr.sm.applyCommittedEntries(entries)
	if pr.sm.isRemoved() {
		// local replica is removed, keep the shard
		pr.store.destroyReplica(pr.shardID, false, "removed by config change")
	}
	return nil
}
