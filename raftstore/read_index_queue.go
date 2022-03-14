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
	"bytes"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"go.etcd.io/etcd/raft/v3"
	"go.uber.org/zap"
)

type requestExecutor func(req rpcpb.Request)

type readyRead struct {
	batch batch
	index uint64
}

type readIndexQueue struct {
	logger       *zap.Logger
	shardID      uint64
	reads        []readyRead
	readyCount   int
	lastReadyIdx int
}

func newReadIndexQueue(shardID uint64, logger *zap.Logger) *readIndexQueue {
	return &readIndexQueue{
		shardID: shardID,
		logger:  log.Adjust(logger),
	}
}

func (q *readIndexQueue) reset() {
	q.reads = q.reads[:0]
	q.readyCount = 0
	q.lastReadyIdx = 0
}

func (q *readIndexQueue) close() {
	for _, rr := range q.reads {
		rr.batch.respShardNotFound(q.shardID)
	}
	q.reset()
}

func (q *readIndexQueue) leaderChanged(newLeader Replica) {
	for _, rr := range q.reads {
		rr.batch.respNotLeader(q.shardID, newLeader)
	}
	q.reset()
}

func (q *readIndexQueue) append(c batch) {
	q.reads = append(q.reads, readyRead{
		batch: c,
	})
}

func (q *readIndexQueue) ready(state raft.ReadState) {
	if ce := q.logger.Check(zap.DebugLevel, "read index ready"); ce != nil {
		ce.Write(log.IndexField(state.Index),
			log.HexField("batch-id", state.RequestCtx))
	}

	for idx := range q.reads {
		if bytes.Equal(q.reads[idx].batch.requestBatch.Header.ID, state.RequestCtx) {
			q.reads[idx].index = state.Index
			q.readyCount++
			q.lastReadyIdx = idx
			return
		}
	}
}

func (q *readIndexQueue) process(appliedIndex uint64, exector requestExecutor) bool {
	if len(q.reads) == 0 || q.readyCount == 0 {
		return false
	}

	handled := false
	newReads := q.reads[:0] // avoid alloc new slice
	for idx := range q.reads {
		if q.reads[idx].index > 0 && q.reads[idx].index <= appliedIndex {
			handled = true
			for _, req := range q.reads[idx].batch.requestBatch.Requests {
				exector(req)
			}
			q.readyCount--
		} else {
			newReads = append(newReads, q.reads[idx])
			if q.reads[idx].index > 0 {
				q.lastReadyIdx = len(newReads) - 1
			}
		}
	}

	q.reads = newReads
	return handled
}

func (q *readIndexQueue) removeLost() bool {
	if q.readyCount == 0 ||
		len(q.reads[:q.lastReadyIdx+1]) == q.readyCount {
		return false
	}

	// Our request is added to the queue in a FIFO manner, which means that
	// the ReadIndex request closer to the end of the queue is sent later.
	// So all read requests that are not set to ready before `lastReadyIdx`
	// need to be cleaned up.
	newReads := q.reads[:0]
	for idx := range q.reads[:q.lastReadyIdx] {
		if q.reads[idx].index > 0 {
			newReads = append(newReads, q.reads[idx])
		}
	}

	old := q.lastReadyIdx
	q.lastReadyIdx = len(newReads)
	newReads = append(newReads, q.reads[old:]...)
	q.reads = newReads
	return true
}
