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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/util/fileutil"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/vfs"
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
	defer leaktest.AfterTest(t)()
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

func (t *replicaTestTransport) Send(m meta.RaftMessage) bool {
	t.messages = append(t.messages, m)
	return true
}

func (t *replicaTestTransport) SendSnapshot(m meta.RaftMessage) bool {
	t.messages = append(t.messages, m)
	return true
}

func (t *replicaTestTransport) Start() error {
	return nil
}

func (t *replicaTestTransport) Close() error {
	return nil
}

func (t *replicaTestTransport) SetFilter(func(meta.RaftMessage) bool) {
	panic("not implemented")
}

func (t *replicaTestTransport) SendingSnapshotCount() uint64 {
	return 0
}

func TestSendRaftMessageAttachsExpectedShardDetails(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
	defer leaktest.AfterTest(t)()
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

func TestIssue386(t *testing.T) {
	// See https://github.com/matrixorigin/matrixcube/issues/386
	// Missing save hardstate to logdb if append entries is empty.
	s, closer := newTestStore(t)
	defer closer()

	pr := newTestReplica(Shard{ID: 1}, Replica{ID: 2}, s)

	assert.NoError(t, pr.processReady(raft.Ready{
		Entries: []raftpb.Entry{{Index: 1, Term: 1, Data: []byte("test")}},
	}, s.logdb.NewWorkerContext()))

	assert.NoError(t, pr.processReady(raft.Ready{
		HardState: raftpb.HardState{Commit: 1},
	}, s.logdb.NewWorkerContext()))

	rs, err := s.logdb.ReadRaftState(1, 2, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), rs.State.Commit)
}

func TestApplyReceivedSnapshot(t *testing.T) {
	fn := func(t *testing.T, r *replica, fs vfs.FS) {
		ss, created, err := r.createSnapshot()
		if err != nil {
			t.Fatalf("failed to create snapshot %v", err)
		}
		assert.Equal(t, uint64(100), ss.Metadata.Index)
		assert.True(t, created)

		// reset the data storage
		dsMem := mem.NewStorage()
		base := kv.NewBaseStorage(dsMem, fs)
		ds := kv.NewKVDataStorage(base, nil)
		defer ds.Close()
		replicaRec := Replica{ID: 1, ContainerID: 100}
		shard := Shard{ID: 1, Replicas: []Replica{replicaRec}}
		r.sm = newStateMachine(r.logger, ds, r.logdb, shard, replicaRec, nil, nil)

		rd := raft.Ready{Snapshot: ss}

		assert.NoError(t, r.processReady(rd, r.logdb.NewWorkerContext()))
		assert.Equal(t, ss.Metadata.Index, r.sm.metadataMu.index)
		assert.Equal(t, ss.Metadata.Term, r.sm.metadataMu.term)
		assert.Equal(t, shard, r.sm.metadataMu.shard)

		sms, err := r.sm.dataStorage.GetInitialStates()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(sms))
		assert.Equal(t, shard, sms[0].Metadata.Shard)

		env := r.snapshotter.getRecoverSnapshotEnv(ss)
		exist, err := fileutil.Exist(env.GetFinalDir(), fs)
		assert.NoError(t, err)
		assert.True(t, exist)
		_, err = r.logdb.GetSnapshot(1)
		assert.NoError(t, err)
	}
	fs := vfs.GetTestFS()
	runReplicaSnapshotTest(t, fn, fs)
}

func TestEntriesToApply(t *testing.T) {
	tests := []struct {
		inputIndex   uint64
		inputLength  uint64
		crash        bool
		resultIndex  uint64
		resultLength uint64
		pushedIndex  uint64
	}{
		{1, 5, true, 0, 0, 10},
		{1, 10, false, 0, 0, 10},
		{1, 11, false, 11, 1, 10},
		{1, 20, false, 11, 10, 10},
		{10, 6, false, 11, 5, 10},
		{11, 5, false, 11, 5, 10},
		{12, 5, true, 0, 0, 10},
	}
	for idx, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r == nil && tt.crash {
					t.Fatalf("%d, didn't panic", idx)
				}
			}()
			inputs := make([]raftpb.Entry, 0)
			for i := tt.inputIndex; i < tt.inputIndex+tt.inputLength; i++ {
				inputs = append(inputs, raftpb.Entry{Index: i})
			}
			r := &replica{pushedIndex: tt.pushedIndex, logger: log.GetPanicZapLogger()}
			results := r.entriesToApply(inputs)
			if uint64(len(results)) != tt.resultLength {
				t.Errorf("%d, result len %d, want %d", idx, len(results), tt.resultLength)
			}
			if len(results) > 0 {
				if results[0].Index != tt.resultIndex {
					t.Errorf("%d, first result index %d, want %d",
						idx, results[0].Index, tt.resultIndex)
				}
			}
		}()
	}
}
