package test

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestAddShardWithMultiGroups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := raftstore.NewSingleTestClusterStore(t, raftstore.WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
		cfg.ShardGroups = 2
		cfg.Prophet.Replication.Groups = []uint64{0, 1}
		cfg.Customize.CustomInitShardsFactory = func() []raftstore.Shard {
			return []raftstore.Shard{{Start: []byte("a"), End: []byte("b")}, {Group: 1, Start: []byte("a"), End: []byte("b")}}
		}
	}))
	defer c.Stop()

	c.Start()
	c.WaitShardByCountPerNode(2, testWaitTimeout)

	err := c.GetProphet().GetClient().AsyncAddResources(raftstore.NewResourceAdapterWithShard(raftstore.Shard{Start: []byte("b"), End: []byte("c"), Unique: "abc", Group: 1}))
	assert.NoError(t, err)
	c.WaitShardByCountPerNode(3, testWaitTimeout)
}

func TestSpeedupAddShard(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := raftstore.NewTestClusterStore(t, raftstore.WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
		cfg.Raft.TickInterval = typeutil.NewDuration(time.Second * 2)
		cfg.Customize.CustomInitShardsFactory = func() []raftstore.Shard { return []raftstore.Shard{{Start: []byte("a"), End: []byte("b")}} }
	}))
	defer c.Stop()

	c.Start()
	c.WaitShardByCountPerNode(1, testWaitTimeout)

	err := c.GetProphet().GetClient().AsyncAddResources(raftstore.NewResourceAdapterWithShard(raftstore.Shard{Start: []byte("b"), End: []byte("c"), Unique: "abc"}))
	assert.NoError(t, err)

	c.WaitShardByCountPerNode(2, testWaitTimeout)
	c.CheckShardCount(2)

	id := c.GetShardByIndex(0, 1).ID
	c.WaitShardStateChangedTo(id, metapb.ResourceState_Running, testWaitTimeout)
}
