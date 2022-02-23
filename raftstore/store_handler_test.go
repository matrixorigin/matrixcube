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

	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/util/stop"
	"github.com/matrixorigin/matrixcube/util/task"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

func TestTryToCreateReplicate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := []struct {
		name       string
		pr         *replica
		start, end []byte
		msg        metapb.RaftMessage
		ok         bool
		checkCache bool
	}{
		{
			name: "normal",
			pr:   &replica{shardID: 1, replica: Replica{ID: 1}},
			msg:  metapb.RaftMessage{To: Replica{ID: 1}, ShardID: 1},
			ok:   true,
		},

		{
			name: "msg stale",
			pr:   &replica{shardID: 1, replica: Replica{ID: 2}},
			msg:  metapb.RaftMessage{To: Replica{ID: 1}, ShardID: 1},
			ok:   false,
		},
		{
			name: "current stale",
			pr:   &replica{shardID: 1, replica: Replica{ID: 1}},
			msg:  metapb.RaftMessage{To: Replica{ID: 2}, ShardID: 1},
			ok:   false,
		},
		{
			name: "not create raft message type",
			msg:  metapb.RaftMessage{To: Replica{ID: 2}, ShardID: 1, Message: raftpb.Message{Type: raftpb.MsgApp}},
			ok:   false,
		},
		{
			name:       "create raft message type, has overlap",
			pr:         &replica{shardID: 2, replica: Replica{ID: 1}},
			start:      []byte("a"),
			end:        []byte("c"),
			msg:        metapb.RaftMessage{To: Replica{ID: 2}, ShardID: 1, Message: raftpb.Message{Type: raftpb.MsgVote}, Start: []byte("b"), End: []byte("c")},
			ok:         false,
			checkCache: true,
		},
		{
			name:  "create",
			pr:    &replica{shardID: 2, replica: Replica{ID: 1}},
			start: []byte("a"),
			end:   []byte("b"),
			msg:   metapb.RaftMessage{To: Replica{ID: 2}, ShardID: 1, Message: raftpb.Message{Type: raftpb.MsgVote}, Start: []byte("b"), End: []byte("c")},
			ok:    true,
		},
	}

	for idx, c := range cases {
		func() {
			s, cancel := newTestStore(t)
			defer cancel()
			s.DataStorageByGroup(0).GetInitialStates()
			if c.pr != nil {
				c.pr.startedC = make(chan struct{})
				c.pr.closedC = make(chan struct{})
				c.pr.store = s
				c.pr.replicaID = c.pr.replica.ID
				c.pr.logger = s.logger
				c.pr.sm = newStateMachine(c.pr.logger, s.DataStorageByGroup(0), nil, Shard{ID: c.pr.shardID, Start: c.start, End: c.end, Replicas: []Replica{c.pr.replica}}, c.pr.replica, nil, nil)
				close(c.pr.startedC)
				s.addReplica(c.pr)
				s.updateShardKeyRange(c.pr.getShard().Group, c.pr.getShard())
			}

			c.msg.From = Replica{ID: 100, StoreID: 1000}
			assert.Equal(t, c.ok, s.tryToCreateReplicate(c.msg), "index %d", idx)
			if c.checkCache {
				msg, ok := s.removeDroppedVoteMsg(c.msg.ShardID)
				assert.True(t, ok)
				assert.Equal(t, c.msg, msg)
			}
		}()
	}
}

func TestHandleDestroyReplicaMessage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := Replica{ID: 1}
	s, cancel := newTestStore(t)
	defer cancel()
	pr := &replica{
		shardID:           1,
		replica:           r,
		startedC:          make(chan struct{}),
		closedC:           make(chan struct{}),
		destroyedC:        make(chan struct{}),
		unloadedC:         make(chan struct{}),
		store:             s,
		logger:            s.logger,
		ticks:             task.New(32),
		messages:          task.New(32),
		requests:          task.New(32),
		actions:           task.New(32),
		feedbacks:         task.New(32),
		pendingProposals:  newPendingProposals(),
		incomingProposals: newProposalBatch(s.logger, 10, 1, r),
		pendingReads:      &readIndexQueue{shardID: 1, logger: s.logger},
		readStopper:       stop.NewStopper("TestHandleDestroyReplicaMessage"),
	}
	pr.sm = newStateMachine(pr.logger,
		s.DataStorageByGroup(0), nil, Shard{ID: pr.shardID, Replicas: []Replica{pr.replica}}, pr.replica, nil, nil)
	s.vacuumCleaner.start()
	defer s.vacuumCleaner.close()
	close(pr.startedC)
	s.addReplica(pr)

	assert.NotNil(t, s.getReplica(1, false))
	s.handleDestroyReplicaMessage(metapb.RaftMessage{IsTombstone: true, ShardID: 1, ShardEpoch: Epoch{Version: 1}})
	for {
		if pr.closed() {
			break
		}
	}
	pr.handleEvent(nil)

	pr.waitDestroyed()
	assert.Nil(t, s.getReplica(1, false))
}

func TestIsRaftMsgValid(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := &store{meta: &metapb.Store{ID: 1}, logger: zap.L()}
	assert.True(t, s.isRaftMsgValid(metapb.RaftMessage{To: Replica{StoreID: 1}}))
	assert.False(t, s.isRaftMsgValid(metapb.RaftMessage{To: Replica{StoreID: 2}}))
}

func TestHasRangeConflict(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := &store{}
	s.updateShardKeyRange(0, Shard{ID: 1})

	conflict, ok := s.hasRangeConflict(0, []byte{1}, nil)
	assert.True(t, ok)
	assert.Equal(t, Shard{ID: 1}, conflict)

	conflict, ok = s.hasRangeConflict(0, nil, []byte{2})
	assert.True(t, ok)
	assert.Equal(t, Shard{ID: 1}, conflict)

	conflict, ok = s.hasRangeConflict(0, []byte{1}, []byte{2})
	assert.True(t, ok)
	assert.Equal(t, Shard{ID: 1}, conflict)
}
