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
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"go.etcd.io/etcd/raft/v3"
	"go.uber.org/zap"
)

type readyRead struct {
	req   rpc.RequestBatch
	index uint64
}

type readIndexQueue struct {
	shardID uint64
	reads   []readyRead
}

func (q *readIndexQueue) reset() {
	q.reads = q.reads[:0]
}

func (q *readIndexQueue) ready(state raft.ReadState) {
	var req rpc.RequestBatch
	protoc.MustUnmarshal(&req, state.RequestCtx)
	q.reads = append(q.reads, readyRead{
		req:   req,
		index: state.Index,
	})
}

func (q *readIndexQueue) process(appliedIndex uint64, pr *replica) {
	if len(q.reads) == 0 {
		return
	}

	newReady := q.reads[:0] // avoid alloc new slice
	pr.readCtx.reset(pr.getShard())
	for _, r := range q.reads {
		if r.index > 0 && r.index <= appliedIndex {
			c := batch{
				req: r.req,
				cb:  pr.store.cb,
				tp:  read,
			}
			pr.readCtx.appendRequestByCmd(c)
		} else {
			newReady = append(newReady, r)
		}
	}

	// TODO (lni):
	// multiple read requests issued to the data storage, but we won't get
	// anything back until the completion of the slowest one.
	q.reads = newReady
	if pr.readCtx.hasRequest() {
		ds := pr.store.DataStorageByGroup(pr.getShard().Group)
		if err := ds.GetCommandExecutor().ExecuteRead(pr.readCtx); err != nil {
			pr.logger.Fatal("fail to exec read batch",
				zap.Error(err))
		}

		pr.readBytes += pr.readCtx.readBytes
		pr.readKeys += uint64(len(pr.readCtx.cmds))
		idx := 0
		for _, c := range pr.readCtx.cmds {
			resp := rpc.ResponseBatch{}
			for i := 0; i < len(c.req.Requests); i++ {
				r := rpc.Response{}
				r.Value = pr.readCtx.responses[idx]
				resp.Responses = append(resp.Responses, r)
				idx++
			}
			c.resp(resp)
		}
	}
}
