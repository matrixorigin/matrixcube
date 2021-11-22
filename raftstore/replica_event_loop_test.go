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

	cpebble "github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/kv/pebble"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/util/task"
	"github.com/matrixorigin/matrixcube/vfs"
)

func getTestStorage() storage.KVStorage {
	fs := vfs.NewMemFS()
	opts := &cpebble.Options{
		FS: vfs.NewPebbleFS(fs),
	}
	st, err := pebble.NewStorage("test-data", nil, opts)
	if err != nil {
		panic(err)
	}
	return st
}

// TODO: we need this here largely because it is pretty difficult to write unit
// tests for the replica type when it has an injected store instance in it.
func getCloseableReplica() (*replica, func()) {
	l := log.GetDefaultZapLogger()
	r := Replica{}
	shardID := uint64(1)
	kv := getTestStorage()
	ldb := logdb.NewKVLogDB(kv, log.GetDefaultZapLogger())
	c := &raft.Config{
		ID:              1,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         NewLogReader(l, 1, 1, ldb),
		MaxInflightMsgs: 100,
		CheckQuorum:     true,
		PreVote:         true,
	}
	rn, err := raft.NewRawNode(c)
	if err != nil {
		panic(err)
	}
	return &replica{
		logger:            l,
		replica:           r,
		shardID:           shardID,
		rn:                rn,
		pendingProposals:  newPendingProposals(),
		incomingProposals: newProposalBatch(l, 0, shardID, r),
		pendingReads:      &readIndexQueue{shardID: shardID, logger: l},
		ticks:             task.New(32),
		messages:          task.New(32),
		requests:          task.New(32),
		actions:           task.New(32),
		feedbacks:         task.New(32),
		snapshotStatus:    task.New(32),
		items:             make([]interface{}, 1024),
		startedC:          make(chan struct{}),
		closedC:           make(chan struct{}),
		unloadedC:         make(chan struct{}),
		sm:                &stateMachine{},
	}, func() { kv.Close() }
}

func TestReplicaCanBeClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r, closer := getCloseableReplica()
	defer r.close()
	defer closer()
	hasEvent, err := r.handleEvent(nil)
	assert.NoError(t, err)
	assert.True(t, hasEvent)
}
