package raftstore

import (
	"sync"

	"github.com/fagongzi/goetty/buf"
)

var (
	bufPool sync.Pool
)

func acquireBuf() *buf.ByteBuf {
	value := bufPool.Get()
	if value == nil {
		return buf.NewByteBuf(64)
	}

	buf := value.(*buf.ByteBuf)
	buf.Resume(64)

	return buf
}

func releaseBuf(value *buf.ByteBuf) {
	value.Clear()
	value.Release()
	bufPool.Put(value)
}
