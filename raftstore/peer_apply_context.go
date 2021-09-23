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

	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/pb/rpc"
)

type applyContext struct {
	writeCtx    *executeContext
	req         *rpc.RequestBatch
	entry       raftpb.Entry
	adminResult *adminExecResult
	metrics     applyMetrics
}

func newApplyContext() *applyContext {
	return &applyContext{
		writeCtx: newExecuteContext(),
		req:      &rpc.RequestBatch{},
	}
}

func (ctx *applyContext) reset(shard meta.Shard, entry raftpb.Entry) {
	ctx.writeCtx.reset(shard)
	ctx.req.Reset()
	ctx.entry = entry
	ctx.adminResult = nil
	ctx.metrics = applyMetrics{}
}

type asyncApplyResult struct {
	shardID uint64
	result  *adminExecResult
	metrics applyMetrics
	index   uint64
}

func (res *asyncApplyResult) hasSplitExecResult() bool {
	return nil != res.result && res.result.splitResult != nil
}

type adminExecResult struct {
	adminType        rpc.AdminCmdType
	changePeerResult *changePeerResult
	splitResult      *splitResult
}

type changePeerResult struct {
	index      uint64
	confChange raftpb.ConfChangeV2
	changes    []rpc.ConfigChangeRequest
	shard      meta.Shard
}

type splitResult struct {
	derived meta.Shard
	shards  []meta.Shard
}
