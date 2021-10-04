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

	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

var (
	// testMaxOnceCommitEntryCount defines how many committed entries can be
	// applied each time. 0 means unlimited
	testMaxOnceCommitEntryCount = 0
)

type applySnapResult struct {
	prev    Shard
	current Shard
}

func (pr *replica) handleReady() error {
	rd := pr.rn.ReadySince(pr.lastReadyIndex)
	pr.handleRaftState(rd)
	pr.sendRaftReplicateMessages(rd)
	if err := pr.handleRaftReadyAppend(rd); err != nil {
		return err
	}
	pr.sendRaftMessages(rd)
	rd = pr.limitNumOfEntriesToApply(rd)
	if err := pr.applyCommittedEntries(rd); err != nil {
		return err
	}
	pr.handleReadyToRead(rd)
	pr.rn.AdvanceAppend(rd)

	return nil
}

func (pr *replica) handleRaftState(rd raft.Ready) {
	// etcd raft won't repeatedly return the same non-empty soft state
	if rd.SoftState != nil {
		// If we become leader, send heartbeat to pd
		if rd.SoftState.RaftState == raft.StateLeader {
			pr.logger.Info("********become leader now********")
			pr.addAction(action{actionType: heartbeatAction})
			pr.resetIncomingProposals()
			if pr.store.aware != nil {
				pr.store.aware.BecomeLeader(pr.getShard())
			}
		} else {
			pr.logger.Info("********become follower now********")
			if pr.store.aware != nil {
				pr.store.aware.BecomeFollower(pr.getShard())
			}
		}
	}
}

func (pr *replica) handleRaftReadyAppend(rd raft.Ready) error {
	start := time.Now()
	defer metric.ObserveRaftLogAppendDuration(start)
	return pr.handleAppendEntries(rd)
}

func (pr *replica) handleAppendEntries(rd raft.Ready) error {
	if len(rd.Entries) > 0 {
		pr.lr.Append(rd.Entries)
		pr.metrics.ready.append++
		return pr.store.logdb.SaveRaftState(pr.shardID, pr.replica.ID, rd)
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

func (pr *replica) sendRaftReplicateMessages(rd raft.Ready) {
	// MsgApp can be immediately sent to followers so leader and followers can
	// concurrently persist the logs to disk. For more details, check raft thesis
	// section 10.2.1.
	pr.send(rd.Messages, true)
}

func (pr *replica) sendRaftMessages(rd raft.Ready) {
	// send all other non-MsgApp messages
	pr.send(rd.Messages, false)
}

func (pr *replica) isMsgApp(m raftpb.Message) bool {
	return m.Type == raftpb.MsgApp
}

func (pr *replica) send(msgs []raftpb.Message, msgAppOnly bool) {
	for _, msg := range msgs {
		if pr.isMsgApp(msg) && msgAppOnly {
			pr.sendMessage(msg)
		} else if !pr.isMsgApp(msg) && !msgAppOnly {
			pr.sendMessage(msg)
		}
	}
}

func (pr *replica) sendMessage(msg raftpb.Message) {
	if err := pr.sendRaftMsg(msg); err != nil {
		// We don't care such failed message transmission, just log the error
		pr.logger.Debug("fail to send msg",
			zap.Uint64("from", msg.From),
			zap.Uint64("from", msg.To),
			zap.Error(err))
	}
	pr.metrics.ready.message++
}

func (pr *replica) sendRaftMsg(msg raftpb.Message) error {
	shard := pr.getShard()
	sendMsg := meta.RaftMessage{}
	sendMsg.ShardID = pr.shardID
	sendMsg.ShardEpoch = shard.Epoch
	sendMsg.Group = shard.Group
	sendMsg.DisableSplit = shard.DisableSplit
	sendMsg.Unique = shard.Unique
	sendMsg.RuleGroups = shard.RuleGroups
	sendMsg.From = pr.replica
	sendMsg.To, _ = pr.getReplicaRecord(msg.To)
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
	if len(shard.Replicas) > 0 &&
		(msg.Type == raftpb.MsgVote ||
			// the peer has not been known to this leader, it may exist or not.
			(msg.Type == raftpb.MsgHeartbeat && msg.Commit == 0)) {
		sendMsg.Start = shard.Start
		sendMsg.End = shard.End
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
		// FIXME: this should not be called until the snapshot is actually
		// transmitted to the target replica.
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

func (pr *replica) doApplyCommittedEntries(entries []raftpb.Entry) error {
	pr.logger.Debug("begin to apply raft log",
		zap.Int("count", len(entries)))

	pr.sm.applyCommittedEntries(entries)
	if pr.sm.isRemoved() {
		pr.doApplyDestory(false)
	}
	return nil
}
