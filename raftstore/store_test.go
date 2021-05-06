package raftstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStartAndStop(t *testing.T) {
	s, closer := newTestStore()
	defer closer()

	s.Start()

	time.Sleep(time.Second)
	cnt := 0
	s.foreachPR(func(pr *peerReplica) bool {
		cnt++
		return true
	})
	assert.Equal(t, 1, cnt)
}

func TestClusterStartAndStop(t *testing.T) {
	c := newTestClusterStore(t)
	defer c.stop()

	c.start()

	time.Sleep(time.Second * 10)
	for i := 0; i < 3; i++ {
		assert.Equal(t, 1, c.getPRCount(i))
	}
}
