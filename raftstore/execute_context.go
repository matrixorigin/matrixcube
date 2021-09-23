package raftstore

import (
	"github.com/fagongzi/goetty/buf"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/storage"
)

type executeContext struct {
	shard        meta.Shard
	buf          *buf.ByteBuf
	cmds         []batch
	requests     []storage.LogRequest
	responses    [][]byte
	writtenBytes uint64
	diffBytes    int64
	readBytes    uint64
}

func newExecuteContext() *executeContext {
	return &executeContext{
		buf: buf.NewByteBuf(128),
	}
}

func (ctx *executeContext) close() {
	ctx.buf.Release()
}

func (ctx *executeContext) appendRequest(req *rpc.RequestBatch) {
	ctx.appendRequestByCmd(batch{req: req})
}

func (ctx *executeContext) appendRequestByCmd(c batch) {
	lq := storage.LogRequest{}
	for _, req := range c.req.Requests {
		lq.Requests = append(lq.Requests, storage.CustomCmd{
			CmdType: req.CustemType,
			Key:     req.Key,
			Cmd:     req.Cmd,
		})
	}

	ctx.requests = append(ctx.requests, lq)
	ctx.cmds = append(ctx.cmds, c)
}

func (ctx *executeContext) hasRequest() bool {
	return len(ctx.requests) > 0
}

func (ctx *executeContext) ByteBuf() *buf.ByteBuf {
	return ctx.buf
}

func (ctx *executeContext) Shard() meta.Shard {
	return ctx.shard
}

func (ctx *executeContext) Requests() []storage.LogRequest {
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

func (ctx *executeContext) reset(shard meta.Shard) {
	ctx.buf.Clear()
	ctx.shard = shard
	ctx.cmds = ctx.cmds[:0]
	ctx.requests = ctx.requests[:0]
	ctx.responses = ctx.responses[:0]
	ctx.writtenBytes = 0
	ctx.diffBytes = 0
	ctx.readBytes = 0
}
