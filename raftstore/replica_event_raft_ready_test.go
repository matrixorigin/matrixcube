// Copyright 2021 MatrixOrigin.
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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func getRaftMessageTypes() []raftpb.MessageType {
	return []raftpb.MessageType{
		raftpb.MsgApp,
		raftpb.MsgAppResp,
		raftpb.MsgVote,
		raftpb.MsgVoteResp,
		raftpb.MsgSnap,
		raftpb.MsgHeartbeat,
		raftpb.MsgHeartbeatResp,
		raftpb.MsgTransferLeader,
	}
}

func TestUpdateMessageMetrics(t *testing.T) {
	r := replica{}
	for _, mt := range getRaftMessageTypes() {
		m := raftpb.Message{
			Type: mt,
		}
		r.updateMessageMetrics(m)
	}

	msgMetrics := raftMessageMetrics{
		append:         1,
		appendResp:     1,
		vote:           1,
		voteResp:       1,
		snapshot:       1,
		heartbeat:      1,
		heartbeatResp:  1,
		transferLeader: 1,
	}

	assert.Equal(t, msgMetrics, r.metrics.message)
}

func TestIsMsgApp(t *testing.T) {
	for _, mt := range getRaftMessageTypes() {
		m := raftpb.Message{Type: mt}
		assert.Equal(t, mt == raftpb.MsgApp, isMsgApp(m))
	}
}

func TestSendRaftMesasgeReturnsErrUnknownReplicaWhenReplicaIsUnknown(t *testing.T) {
	r := replica{
		store: &store{},
		sm:    &stateMachine{},
	}
	_, ok := r.getReplicaRecord(1)
	require.False(t, ok)
	m := raftpb.Message{}
	err := r.sendRaftMessage(m)
	assert.True(t, errors.Is(err, ErrUnknownReplica))
}

type replicaTestTransport struct {
	messages []meta.RaftMessage
}

func (t *replicaTestTransport) Send(m meta.RaftMessage) {
	t.messages = append(t.messages, m)
}

func TestSendRaftMessageAttachsExpectedShardDetails(t *testing.T) {
	trans := &replicaTestTransport{}
	r := replica{
		replica:   Replica{ID: 100},
		store:     &store{},
		sm:        &stateMachine{},
		transport: trans,
	}
	shard := Shard{
		Start: []byte("start-key"),
		End:   []byte("end-key"),
		Group: 300,
		Replicas: []Replica{
			{
				ID: 100,
			},
		},
	}
	r.sm.metadataMu.shard = shard
	toReplica := Replica{ID: 200}
	r.store.replicaRecords.Store(toReplica.ID, toReplica)
	m := raftpb.Message{
		From: r.replica.ID,
		To:   toReplica.ID,
		Type: raftpb.MsgVote,
	}
	r.sendRaftMessage(m)
	require.Equal(t, 1, len(trans.messages))
	sentMsg := trans.messages[0]
	assert.Equal(t, shard.Start, sentMsg.Start)
	assert.Equal(t, shard.End, sentMsg.End)
	assert.Equal(t, r.replica, sentMsg.From)
	assert.Equal(t, toReplica, sentMsg.To)
	assert.Equal(t, m, sentMsg.Message)
	assert.Equal(t, shard.Group, sentMsg.Group)

	assert.Equal(t, uint64(1), r.metrics.message.vote)
}

func testSendMessages(t *testing.T, appendOnly bool) {
	trans := &replicaTestTransport{}
	r := replica{
		replica:   Replica{ID: 100},
		store:     &store{},
		sm:        &stateMachine{},
		transport: trans,
	}
	shard := Shard{
		Start: []byte("start-key"),
		End:   []byte("end-key"),
		Group: 300,
		Replicas: []Replica{
			{
				ID: 100,
			},
		},
	}
	r.sm.metadataMu.shard = shard
	toReplica := Replica{ID: 200}
	r.store.replicaRecords.Store(toReplica.ID, toReplica)
	m1 := raftpb.Message{Type: raftpb.MsgVote, To: toReplica.ID, From: r.replica.ID}
	m2 := raftpb.Message{Type: raftpb.MsgApp, To: toReplica.ID, From: r.replica.ID}
	m3 := raftpb.Message{Type: raftpb.MsgAppResp, To: toReplica.ID, From: r.replica.ID}
	rd := raft.Ready{Messages: []raftpb.Message{m1, m2, m3}}
	if appendOnly {
		r.sendRaftAppendLogMessages(rd)
		require.Equal(t, 1, len(trans.messages))
		assert.Equal(t, raftpb.MsgApp, trans.messages[0].Message.Type)
		assert.Equal(t, uint64(1), r.metrics.message.append)
	} else {
		r.sendRaftMessages(rd)
		require.Equal(t, 2, len(trans.messages))
		assert.Equal(t, raftpb.MsgVote, trans.messages[0].Message.Type)
		assert.Equal(t, raftpb.MsgAppResp, trans.messages[1].Message.Type)
		assert.Equal(t, uint64(1), r.metrics.message.vote)
		assert.Equal(t, uint64(0), r.metrics.message.append)
		assert.Equal(t, uint64(1), r.metrics.message.appendResp)
	}
}

func TestSendAppendOnlyMessages(t *testing.T) {
	testSendMessages(t, true)
}

func TestSendOtherMessages(t *testing.T) {
	testSendMessages(t, false)
}
