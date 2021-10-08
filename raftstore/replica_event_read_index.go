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
	"github.com/fagongzi/util/protoc"
	"go.etcd.io/etcd/raft/v3"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/storage"
)

type readyRead struct {
	batch rpc.RequestBatch
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

func (q *readIndexQueue) ready(state raft.ReadState) {
	var batch rpc.RequestBatch
	protoc.MustUnmarshal(&batch, state.RequestCtx)
	q.reads = append(q.reads, readyRead{
		batch: batch,
		index: state.Index,
	})
}

func (q *readIndexQueue) process(appliedIndex uint64, pr *replica) {
	if len(q.reads) == 0 {
		return
	}

	newReady := q.reads[:0] // avoid alloc new slice
	shard := pr.getShard()
	readCtx := newReadContext()
	ds := pr.store.DataStorageByGroup(shard.Group)

	// TODO (lni):
	// multiple read requests issued to the data storage, but we won't get
	// anything back until the completion of the slowest one.
	for _, ready := range q.reads {
		if ready.index > 0 && ready.index <= appliedIndex {
			resp := rpc.ResponseBatch{}
			for _, req := range ready.batch.Requests {
				rr := storage.Request{
					CmdType: req.CustomType,
					Key:     req.Key,
					Cmd:     req.Cmd,
				}
				readCtx.reset(shard, rr)
				v, err := ds.Read(readCtx)
				if err != nil {
					// FIXME: some read failures should be tolerated.
					q.logger.Fatal("fail to exec read batch",
						zap.Error(err))
				}
				pr.readBytes += readCtx.readBytes
				pr.readKeys += 1
				r := rpc.Response{Value: v}
				resp.Responses = append(resp.Responses, r)
			}
			// FIXME: it is strange to require a batch{} to be created/used to send
			// responses. why sending responses is coupled with the batch type?
			// FIXME: input parameter logger is missing
			b := newBatch(nil, ready.batch, pr.store.shardsProxy.OnResponse, 0, 0)
			b.resp(resp)
		} else {
			newReady = append(newReady, ready)
		}
	}

	q.reads = newReady
}
