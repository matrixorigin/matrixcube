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

	"go.etcd.io/etcd/raft/v3"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/rpc"
)

type requestExecutor func(req rpc.Request)

type readyRead struct {
	batch batch
	index uint64
}

type readIndexQueue struct {
	logger  *zap.Logger
	shardID uint64
	reads   []readyRead
}

func (q *readIndexQueue) reset() {
	q.reads = q.reads[:0]
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
		}
	}
}

func (q *readIndexQueue) process(appliedIndex uint64, exector requestExecutor) {
	if len(q.reads) == 0 {
		return
	}

	newReady := q.reads[:0] // avoid alloc new slice
	for _, ready := range q.reads {
		if ready.index > 0 && ready.index <= appliedIndex {
			for _, req := range ready.batch.requestBatch.Requests {
				exector(req)
			}
		} else {
			newReady = append(newReady, ready)
		}
	}

	q.reads = newReady
}
