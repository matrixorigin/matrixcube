package raftstore

import (
	"sync"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/fagongzi/goetty"
)

var (
	raftMessagePool      sync.Pool
	bufPool              sync.Pool
	cmdPool              sync.Pool
	reqCtxPool           sync.Pool
	entryPool            sync.Pool
	asyncApplyResultPool sync.Pool
	readyContextPool     sync.Pool
	applyContextPool     sync.Pool
)

func acquireBuf() *goetty.ByteBuf {
	value := bufPool.Get()
	if value == nil {
		return goetty.NewByteBuf(64)
	}

	buf := value.(*goetty.ByteBuf)
	buf.Resume(64)

	return buf
}

func releaseBuf(value *goetty.ByteBuf) {
	value.Clear()
	value.Release()
	bufPool.Put(value)
}

func acquireCmd() *cmd {
	v := cmdPool.Get()
	if v == nil {
		return &cmd{}
	}

	return v.(*cmd)
}

func releaseCmd(c *cmd) {
	c.reset()
	cmdPool.Put(c)
}

func acquireReqCtx() *reqCtx {
	v := reqCtxPool.Get()
	if v == nil {
		return &reqCtx{}
	}

	return v.(*reqCtx)
}

func releaseReqCtx(req *reqCtx) {
	req.reset()
	reqCtxPool.Put(req)
}

func acquireEntry() *raftpb.Entry {
	v := entryPool.Get()
	if v == nil {
		return &raftpb.Entry{}
	}

	return v.(*raftpb.Entry)
}

func releaseEntry(ent *raftpb.Entry) {
	ent.Reset()
	entryPool.Put(ent)
}

func acquireAsyncApplyResult() *asyncApplyResult {
	v := asyncApplyResultPool.Get()
	if v == nil {
		return &asyncApplyResult{}
	}

	return v.(*asyncApplyResult)
}

func releaseAsyncApplyResult(res *asyncApplyResult) {
	res.reset()
	asyncApplyResultPool.Put(res)
}

func acquireReadyContext() *readyContext {
	v := readyContextPool.Get()
	if v == nil {
		return &readyContext{}
	}

	return v.(*readyContext)
}

func releaseReadyContext(ctx *readyContext) {
	ctx.reset()
	readyContextPool.Put(ctx)
}

func acquireApplyContext() *applyContext {
	v := applyContextPool.Get()
	if v == nil {
		return newApplyContext()
	}

	return v.(*applyContext)
}

func releaseApplyContext(ctx *applyContext) {
	ctx.reset()
	applyContextPool.Put(ctx)
}
