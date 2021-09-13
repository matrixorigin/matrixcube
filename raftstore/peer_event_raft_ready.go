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
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

var (
	// testMaxOnceCommitEntryCount defines how many committed entries can be
	// applied each time. 0 means unlimited
	testMaxOnceCommitEntryCount = 0
)

type applySnapResult struct {
	prev    bhmetapb.Shard
	current bhmetapb.Shard
}

func (pr *peerReplica) handleReady() error {
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

func (pr *peerReplica) handleRaftState(rd raft.Ready) {
	// etcd raft won't repeatedly return the same non-empty soft state
	if rd.SoftState != nil {
		// If we become leader, send heartbeat to pd
		if rd.SoftState.RaftState == raft.StateLeader {
			logger.Infof("%s ********become leader now********",
				pr.id())
			pr.addAction(action{actionType: heartbeatAction})
			pr.resetBatch()
			if pr.store.aware != nil {
				pr.store.aware.BecomeLeader(pr.getShard())
			}
		} else {
			logger.Infof("%s ********become follower now********",
				pr.id())
			if pr.store.aware != nil {
				pr.store.aware.BecomeFollower(pr.getShard())
			}
		}
	}
}

func (pr *peerReplica) handleRaftReadyAppend(rd raft.Ready) error {
	start := time.Now()
	defer metric.ObserveRaftLogAppendDuration(start)
	return pr.handleAppendEntries(rd)
}

func (pr *peerReplica) handleAppendEntries(rd raft.Ready) error {
	if len(rd.Entries) > 0 {
		pr.lr.Append(rd.Entries)
		pr.metrics.ready.append++
		return pr.store.logdb.SaveRaftState(pr.shardID, pr.peer.ID, rd)
	}
	return nil
}

func (pr *peerReplica) limitNumOfEntriesToApply(rd raft.Ready) raft.Ready {
	if testMaxOnceCommitEntryCount > 0 &&
		testMaxOnceCommitEntryCount < len(rd.CommittedEntries) {
		rd.CommittedEntries = rd.CommittedEntries[:testMaxOnceCommitEntryCount]
	}
	return rd
}

func (pr *peerReplica) applyCommittedEntries(rd raft.Ready) error {
	for _, entry := range rd.CommittedEntries {
		pr.raftLogSizeHint += uint64(len(entry.Data))
	}

	if len(rd.CommittedEntries) > 0 {
		pr.lastReadyIndex = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
		if err := pr.startApplyCommittedEntriesJob(rd.CommittedEntries); err != nil {
			return err
		}
		pr.metrics.ready.commit++
	}
	return nil
}

func (pr *peerReplica) handleReadyToRead(rd raft.Ready) {
	for _, state := range rd.ReadStates {
		pr.pendingReads.ready(state)
	}
	if len(rd.ReadStates) > 0 {
		pr.maybeExecRead()
	}
}

func (pr *peerReplica) sendRaftReplicateMessages(rd raft.Ready) {
	// MsgApp can be immediately sent to followers so leader and followers can
	// concurrently persist the logs to disk. For more details, check raft thesis
	// section 10.2.1.
	pr.send(rd.Messages, true)
}

func (pr *peerReplica) sendRaftMessages(rd raft.Ready) {
	// send all other non-MsgApp messages
	pr.send(rd.Messages, false)
}

func (pr *peerReplica) isMsgApp(m raftpb.Message) bool {
	return m.Type == raftpb.MsgApp
}

func (pr *peerReplica) send(msgs []raftpb.Message, msgAppOnly bool) {
	for _, msg := range msgs {
		if pr.isMsgApp(msg) && msgAppOnly {
			pr.sendMessage(msg)
		} else if !pr.isMsgApp(msg) && !msgAppOnly {
			pr.sendMessage(msg)
		}
	}
}

func (pr *peerReplica) sendMessage(msg raftpb.Message) {
	if err := pr.sendRaftMsg(msg); err != nil {
		// We don't care such failed message transmission, just log the error
		logger.Debugf("shard %d send msg failed, from_peer=<%d> to_peer=<%d>, errors:\n%s",
			pr.shardID,
			msg.From,
			msg.To,
			err)
	}
	pr.metrics.ready.message++
}

func (pr *peerReplica) sendRaftMsg(msg raftpb.Message) error {
	shard := pr.getShard()
	sendMsg := pb.AcquireRaftMessage()
	sendMsg.ShardID = pr.shardID
	sendMsg.ShardEpoch = shard.Epoch
	sendMsg.Group = shard.Group
	sendMsg.DisableSplit = shard.DisableSplit
	sendMsg.Unique = shard.Unique
	sendMsg.RuleGroups = shard.RuleGroups
	sendMsg.From = pr.peer
	sendMsg.To, _ = pr.getPeer(msg.To)
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
	if len(shard.Peers) > 0 &&
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
