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

	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3"
)

func TestReadIndexQueueReset(t *testing.T) {
	q := newReadIndexQueue(1, nil)
	q.reads = append(q.reads, readyRead{})
	q.readyCount = 1
	q.lastReadyIdx = 1
	q.reset()
	assert.Equal(t, 0, q.readyCount)
	assert.Equal(t, 0, q.lastReadyIdx)
	assert.Empty(t, q.reads)
}

func TestReadIndexQueueAppend(t *testing.T) {
	q := newReadIndexQueue(1, nil)
	q.append(newTestBatch("1", "k1", 1, rpc.CmdType_Write, 0, nil))
	assert.Equal(t, 1, len(q.reads))
	assert.Equal(t, 0, q.readyCount)
	assert.Equal(t, 0, q.lastReadyIdx)
}

func TestReadIndexQueueReadyWithOrder(t *testing.T) {
	q := newReadIndexQueue(1, nil)
	q.append(newTestBatch("1", "k1", 1, rpc.CmdType_Write, 0, nil))
	q.append(newTestBatch("2", "k2", 1, rpc.CmdType_Write, 0, nil))
	assert.Equal(t, 2, len(q.reads))

	q.ready(raft.ReadState{
		Index:      1,
		RequestCtx: q.reads[0].batch.getRequestID(),
	})
	assert.Equal(t, 1, q.readyCount)
	assert.Equal(t, 0, q.lastReadyIdx)
	assert.Equal(t, uint64(1), q.reads[0].index)

	q.ready(raft.ReadState{
		Index:      1,
		RequestCtx: q.reads[1].batch.getRequestID(),
	})
	assert.Equal(t, 2, q.readyCount)
	assert.Equal(t, 1, q.lastReadyIdx)
	assert.Equal(t, uint64(1), q.reads[0].index)
}

func TestReadIndexQueueReadyWithDisorder(t *testing.T) {
	q := newReadIndexQueue(1, nil)
	q.append(newTestBatch("1", "k1", 1, rpc.CmdType_Write, 0, nil))
	q.append(newTestBatch("2", "k2", 1, rpc.CmdType_Write, 0, nil))
	assert.Equal(t, 2, len(q.reads))

	q.ready(raft.ReadState{
		Index:      1,
		RequestCtx: q.reads[1].batch.getRequestID(),
	})
	assert.Equal(t, 1, q.readyCount)
	assert.Equal(t, 1, q.lastReadyIdx)
	assert.Equal(t, uint64(1), q.reads[1].index)

	q.ready(raft.ReadState{
		Index:      1,
		RequestCtx: q.reads[0].batch.getRequestID(),
	})
	assert.Equal(t, 2, q.readyCount)
	assert.Equal(t, 0, q.lastReadyIdx)
	assert.Equal(t, uint64(1), q.reads[0].index)
}

func TestReadIndexQueueRemoveLostWithNoLost(t *testing.T) {
	q := newReadIndexQueue(1, nil)
	assert.False(t, q.removeLost())

	q.append(newTestBatch("1", "k1", 1, rpc.CmdType_Write, 0, nil))
	q.append(newTestBatch("2", "k2", 1, rpc.CmdType_Write, 0, nil))
	q.append(newTestBatch("3", "k2", 1, rpc.CmdType_Write, 0, nil))
	assert.False(t, q.removeLost())

	q.ready(raft.ReadState{
		Index:      1,
		RequestCtx: q.reads[0].batch.getRequestID(),
	})
	assert.False(t, q.removeLost())

	q.ready(raft.ReadState{
		Index:      1,
		RequestCtx: q.reads[1].batch.getRequestID(),
	})
	assert.False(t, q.removeLost())

	q.ready(raft.ReadState{
		Index:      1,
		RequestCtx: q.reads[2].batch.getRequestID(),
	})
	assert.False(t, q.removeLost())
}

func TestReadIndexQueueRemoveLostWithLost(t *testing.T) {
	q := newReadIndexQueue(1, nil)

	q.append(newTestBatch("1", "k1", 1, rpc.CmdType_Write, 0, nil))
	q.append(newTestBatch("2", "k2", 1, rpc.CmdType_Write, 0, nil))
	q.append(newTestBatch("3", "k2", 1, rpc.CmdType_Write, 0, nil))
	id1 := q.reads[1].batch.getRequestID()
	id2 := q.reads[2].batch.getRequestID()

	q.ready(raft.ReadState{
		Index:      1,
		RequestCtx: q.reads[1].batch.getRequestID(),
	})
	q.ready(raft.ReadState{
		Index:      1,
		RequestCtx: q.reads[2].batch.getRequestID(),
	})

	assert.True(t, q.removeLost())
	assert.Equal(t, 2, len(q.reads))
	assert.Equal(t, 2, q.readyCount)
	assert.Equal(t, 1, q.lastReadyIdx)
	assert.Equal(t, uint64(1), q.reads[0].index)
	assert.Equal(t, id1, q.reads[0].batch.getRequestID())
	assert.Equal(t, uint64(1), q.reads[1].index)
	assert.Equal(t, id2, q.reads[1].batch.getRequestID())
}

func TestReadIndexQueueProcessWithEmpty(t *testing.T) {
	q := newReadIndexQueue(1, nil)
	assert.False(t, q.process(1, nil))
}

func TestReadIndexQueueProcessWithNoReady(t *testing.T) {
	q := newReadIndexQueue(1, nil)
	q.append(newTestBatch("1", "k1", 1, rpc.CmdType_Write, 0, nil))
	assert.False(t, q.process(1, nil))
}

func TestReadIndexQueueProcessWithReadyNotApplied(t *testing.T) {
	q := newReadIndexQueue(1, nil)
	q.append(newTestBatch("1", "k1", 1, rpc.CmdType_Write, 0, nil))
	q.append(newTestBatch("2", "k2", 1, rpc.CmdType_Write, 0, nil))
	q.ready(raft.ReadState{
		Index:      2,
		RequestCtx: q.reads[0].batch.getRequestID(),
	})
	assert.False(t, q.process(1, nil))

	q.ready(raft.ReadState{
		Index:      2,
		RequestCtx: q.reads[1].batch.getRequestID(),
	})
	assert.False(t, q.process(1, nil))

	assert.Equal(t, 2, len(q.reads))
	assert.Equal(t, 2, q.readyCount)
	assert.Equal(t, 1, q.lastReadyIdx)
}

func TestReadIndexQueueProcessWithReadyApplied(t *testing.T) {
	q := newReadIndexQueue(1, nil)
	q.append(newTestBatch("1", "k1", 1, rpc.CmdType_Write, 0, nil))
	q.append(newTestBatch("2", "k2", 1, rpc.CmdType_Write, 0, nil))
	q.append(newTestBatch("3", "k3", 1, rpc.CmdType_Write, 0, nil))

	q.ready(raft.ReadState{
		Index:      1,
		RequestCtx: q.reads[0].batch.getRequestID(),
	})
	q.ready(raft.ReadState{
		Index:      2,
		RequestCtx: q.reads[1].batch.getRequestID(),
	})
	q.ready(raft.ReadState{
		Index:      3,
		RequestCtx: q.reads[2].batch.getRequestID(),
	})

	n := 0
	assert.True(t, q.process(2, func(req rpc.Request) { n++ }))

	assert.Equal(t, 2, n)
	assert.Equal(t, 1, len(q.reads))
	assert.Equal(t, 1, q.readyCount)
	assert.Equal(t, 0, q.lastReadyIdx)
}
