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
	"sync"

	"github.com/matrixorigin/matrixcube/util/buf"

	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/storage"
)

// FIXME: both writeContext and readContext hold a buf.ByteBuf which is
// unbounded in size after use. given we can have many replicas on each
// machine, the total memory consumption can thus be huge. we need the
// buf.ByteBuf to have the ability to shrink when realizing that its
// capacity is significantly higher than recent demands. for
// writeContext, another optimization is to have it for each worker
// rather than one for each state machine.
type writeContext struct {
	shard        Shard
	wb           storage.Resetable
	buf          *buf.ByteBuf
	batch        storage.Batch
	responses    [][]byte
	writtenBytes uint64
	diffBytes    int64
}

var _ storage.WriteContext = (*writeContext)(nil)

func newWriteContext(base storage.BaseStorage) *writeContext {
	return &writeContext{
		buf: buf.NewByteBuf(128),
		wb:  base.NewWriteBatch(),
	}
}

func (ctx *writeContext) close() {
	ctx.buf.Release()
}

func (ctx *writeContext) hasRequest() bool {
	return len(ctx.batch.Requests) > 0
}

func (ctx *writeContext) WriteBatch() storage.Resetable {
	return ctx.wb
}

func (ctx *writeContext) ByteBuf() *buf.ByteBuf {
	return ctx.buf
}

func (ctx *writeContext) Shard() Shard {
	return ctx.shard
}

func (ctx *writeContext) Batch() storage.Batch {
	return ctx.batch
}

func (ctx *writeContext) AppendResponse(resp []byte) {
	ctx.responses = append(ctx.responses, resp)
}

func (ctx *writeContext) SetWrittenBytes(value uint64) {
	ctx.writtenBytes = value
}

func (ctx *writeContext) SetDiffBytes(value int64) {
	ctx.diffBytes = value
}

func (ctx *writeContext) initialize(shard Shard, index uint64, batch rpcpb.RequestBatch) {
	ctx.buf.Clear()
	ctx.shard = shard
	ctx.batch = storage.Batch{Index: index}
	ctx.responses = ctx.responses[:0]
	ctx.writtenBytes = 0
	ctx.diffBytes = 0

	for _, r := range batch.Requests {
		ctx.batch.Requests = append(ctx.batch.Requests, storage.Request{
			CmdType: r.CustomType,
			Key:     r.Key,
			Cmd:     r.Cmd,
		})
	}
}

type readContext struct {
	shard     Shard
	buf       *buf.ByteBuf
	request   storage.Request
	readBytes uint64
}

var _ storage.ReadContext = (*readContext)(nil)

var (
	readCtxPool = sync.Pool{
		New: func() interface{} {
			return newReadContext()
		},
	}
)

func acquireReadCtx() *readContext {
	return readCtxPool.Get().(*readContext)
}

func releaseReadCtx(ctx *readContext) {
	readCtxPool.Put(ctx)
}

func newReadContext() *readContext {
	return &readContext{
		buf: buf.NewByteBuf(128),
	}
}

func (ctx *readContext) ByteBuf() *buf.ByteBuf {
	return ctx.buf
}

func (ctx *readContext) Shard() Shard {
	return ctx.shard
}

func (ctx *readContext) Request() storage.Request {
	return ctx.request
}

func (ctx *readContext) SetReadBytes(value uint64) {
	ctx.readBytes = value
}

func (ctx *readContext) reset(shard Shard, req storage.Request) {
	ctx.shard = shard
	ctx.request = req
	ctx.buf.Clear()
	ctx.readBytes = 0
}
