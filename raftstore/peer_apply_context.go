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
	"github.com/fagongzi/goetty/buf"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/util"
)

type applyContext struct {
	pr         *peerReplica
	raftWB     *util.WriteBatch
	dataWB     *util.WriteBatch
	attrs      map[string]interface{}
	buf        *buf.ByteBuf
	applyState bhraftpb.RaftApplyState
	req        *raftcmdpb.RaftCMDRequest
	index      uint64
	term       uint64
	offset     int
	batchSize  int
	metrics    applyMetrics
}

var _ command.Context = (*applyContext)(nil)

func newApplyContext(pr *peerReplica) *applyContext {
	return &applyContext{
		raftWB: util.NewWriteBatch(),
		dataWB: util.NewWriteBatch(),
		buf:    buf.NewByteBuf(512),
		attrs:  make(map[string]interface{}),
		pr:     pr,
	}
}

func (ctx *applyContext) reset() {
	ctx.raftWB.Reset()
	ctx.dataWB.Reset()
	for key := range ctx.attrs {
		delete(ctx.attrs, key)
	}
	ctx.applyState = bhraftpb.RaftApplyState{}
	ctx.req = nil
	ctx.index = 0
	ctx.term = 0
	ctx.offset = 0
	ctx.batchSize = 0
	ctx.metrics = applyMetrics{}
}

func (ctx *applyContext) WriteBatch() *util.WriteBatch {
	return ctx.dataWB
}
func (ctx *applyContext) Attrs() map[string]interface{} {
	return ctx.attrs
}

func (ctx *applyContext) ByteBuf() *buf.ByteBuf {
	return ctx.buf
}

func (ctx *applyContext) LogIndex() uint64 {
	return ctx.index
}

func (ctx *applyContext) Offset() int {
	return ctx.offset
}

func (ctx *applyContext) BatchSize() int {
	return ctx.batchSize
}

func (ctx *applyContext) DataStorage() storage.DataStorage {
	return ctx.pr.store.DataStorageByGroup(ctx.pr.getShard().Group, ctx.pr.shardID)
}

func (ctx *applyContext) StoreID() uint64 {
	return ctx.pr.store.Meta().ID
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
	adminType        raftcmdpb.AdminCmdType
	changePeerResult *changePeerResult
	splitResult      *splitResult
}

type changePeerResult struct {
	index      uint64
	confChange raftpb.ConfChangeV2
	changes    []raftcmdpb.ChangePeerRequest
	shard      bhmetapb.Shard
}

type splitResult struct {
	derived bhmetapb.Shard
	shards  []bhmetapb.Shard
}
