package raftstore

import (
	"sync"

	"github.com/deepfabric/beehive/util"
	"github.com/fagongzi/goetty"
)

var (
	bufPool              sync.Pool
	asyncApplyResultPool sync.Pool
	readyContextPool     sync.Pool
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
		return &readyContext{
			wb: util.NewWriteBatch(),
		}
	}

	return v.(*readyContext)
}

func releaseReadyContext(ctx *readyContext) {
	ctx.reset()
	readyContextPool.Put(ctx)
}
