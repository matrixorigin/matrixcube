package raftstore

import (
	"sync"

	"github.com/fagongzi/goetty"
)

var (
	bufPool sync.Pool
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
