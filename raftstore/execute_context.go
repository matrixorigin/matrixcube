package raftstore

import (
	"github.com/fagongzi/goetty/buf"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/storage"
)

type executeContext struct {
	shard        Shard
	wb           storage.Resetable
	buf          *buf.ByteBuf
	batches      []batch
	requests     []storage.Batch
	responses    [][]byte
	writtenBytes uint64
	diffBytes    int64
	readBytes    uint64
}

func newExecuteContext(base storage.BaseStorage) *executeContext {
	return &executeContext{
		buf: buf.NewByteBuf(128),
		wb:  base.NewWriteBatch(),
	}
}

func (ctx *executeContext) close() {
	ctx.buf.Release()
}

func (ctx *executeContext) appendRequestBatch(req rpc.RequestBatch) {
	ctx.appendBatch(batch{requestBatch: req})
}

func (ctx *executeContext) appendBatch(c batch) {
	b := storage.Batch{}
	for _, req := range c.requestBatch.Requests {
		b.Requests = append(b.Requests, storage.Request{
			CmdType: req.CustemType,
			Key:     req.Key,
			Cmd:     req.Cmd,
		})
	}

	ctx.requests = append(ctx.requests, b)
	ctx.batches = append(ctx.batches, c)
}

func (ctx *executeContext) hasRequest() bool {
	return len(ctx.batches) > 0
}

func (ctx *executeContext) WriteBatch() storage.Resetable {
	return ctx.wb
}

func (ctx *executeContext) ByteBuf() *buf.ByteBuf {
	return ctx.buf
}

func (ctx *executeContext) Shard() Shard {
	return ctx.shard
}

func (ctx *executeContext) Batches() []storage.Batch {
	return ctx.requests
}

func (ctx *executeContext) AppendResponse(resp []byte) {
	ctx.responses = append(ctx.responses, resp)
}

func (ctx *executeContext) SetWrittenBytes(value uint64) {
	ctx.writtenBytes = value
}

func (ctx *executeContext) SetReadBytes(value uint64) {
	ctx.readBytes = value
}

func (ctx *executeContext) SetDiffBytes(value int64) {
	ctx.diffBytes = value
}

func (ctx *executeContext) reset(shard Shard) {
	ctx.buf.Clear()
	ctx.shard = shard
	ctx.batches = ctx.batches[:0]
	ctx.requests = ctx.requests[:0]
	ctx.responses = ctx.responses[:0]
	ctx.writtenBytes = 0
	ctx.diffBytes = 0
	ctx.readBytes = 0
}

// TODO: this implies that we can't have more than one batch in the
// executeContext
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
	ctx.writeCtx.close()
}

func (ctx *applyContext) initialize(shard Shard, entry raftpb.Entry) {
	ctx.writeCtx.reset(shard)
	ctx.req = rpc.RequestBatch{}
	ctx.entry = entry
	ctx.adminResult = nil
	ctx.metrics = applyMetrics{}
}
