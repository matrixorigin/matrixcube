package raftstore

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
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
	c := newTestClusterStore(t, nil)
	defer c.stop()

	c.start()

	time.Sleep(time.Second * 10)
	for i := 0; i < 3; i++ {
		assert.Equal(t, 1, c.getPRCount(i))
	}
}

func TestAddAndRemoveShard(t *testing.T) {
	c := newTestClusterStore(t, func() []bhmetapb.Shard { return []bhmetapb.Shard{{Start: []byte("a"), End: []byte("b")}} })
	defer c.stop()

	c.start()

	time.Sleep(time.Second * 2)
	err := c.stores[0].pd.GetClient().AsyncAddResources(NewResourceAdapterWithShard(bhmetapb.Shard{Start: []byte("b"), End: []byte("c"), Unique: "abc"}))
	assert.NoError(t, err)

	time.Sleep(time.Second * 10)
	for i := 0; i < 3; i++ {
		assert.Equal(t, 2, c.getPRCount(i))
	}
	res, err := c.stores[0].pd.GetStorage().GetResource(9)
	assert.NoError(t, err)
	assert.Equal(t, metapb.ResourceState_Running, res.State())

	assert.NoError(t, c.stores[0].pd.GetClient().AsyncRemoveResources(9))
	time.Sleep(time.Second * 2)

	for i := 0; i < 3; i++ {
		assert.Equal(t, 1, c.getPRCount(i))
	}
	res, err = c.stores[0].pd.GetStorage().GetResource(9)
	assert.NoError(t, err)
	assert.Equal(t, metapb.ResourceState_Removed, res.State())
}
