package raftstore

import (
	"github.com/fagongzi/goetty/buf"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/storage"
)

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

func (ctx *writeContext) setRequestBatch(batch rpc.RequestBatch) {
	ctx.batch = storage.Batch{}
	for _, r := range batch.Requests {
		ctx.batch.Requests = append(ctx.batch.Requests, storage.Request{
			CmdType: r.CustomType,
			Key:     r.Key,
			Cmd:     r.Cmd,
		})
	}
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

func (ctx *writeContext) reset(shard Shard) {
	ctx.buf.Clear()
	ctx.shard = shard
	ctx.batch = storage.Batch{}
	ctx.responses = ctx.responses[:0]
	ctx.writtenBytes = 0
	ctx.diffBytes = 0
}

type readContext struct {
	shard     Shard
	buf       *buf.ByteBuf
	request   storage.Request
	readBytes uint64
}

var _ storage.ReadContext = (*readContext)(nil)

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

type applyContext struct {
	writeCtx    *writeContext
	req         rpc.RequestBatch
	entry       raftpb.Entry
	adminResult *adminResult
	metrics     applyMetrics
}

func newApplyContext(base storage.BaseStorage) *applyContext {
	return &applyContext{
		writeCtx: newWriteContext(base),
		req:      rpc.RequestBatch{},
	}
}

func (ctx *applyContext) close() {
	ctx.writeCtx.close()
}

func (ctx *applyContext) initialize(shard Shard, entry raftpb.Entry) {
	ctx.writeCtx.reset(shard)
	ctx.req = rpc.RequestBatch{}
	ctx.entry = entry
	ctx.adminResult = nil
	ctx.metrics = applyMetrics{}
}
