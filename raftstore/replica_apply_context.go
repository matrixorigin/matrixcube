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
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/storage"
)

type applyContext struct {
	writeCtx    *executeContext
	req         rpc.RequestBatch
	entry       raftpb.Entry
	adminResult *adminResult
	metrics     applyMetrics
}

func newApplyContext(base storage.BaseStorage) *applyContext {
	return &applyContext{
		writeCtx: newExecuteContext(base),
		req:      rpc.RequestBatch{},
	}
}

func (ctx *applyContext) close() {
	if ctx != nil && ctx.writeCtx != nil {
		ctx.writeCtx.close()
	}
}

func (ctx *applyContext) reset(shard Shard, entry raftpb.Entry) {
	ctx.writeCtx.reset(shard)
	ctx.req = rpc.RequestBatch{}
	ctx.entry = entry
	ctx.adminResult = nil
	ctx.metrics = applyMetrics{}
}
