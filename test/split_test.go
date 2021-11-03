package test

import (
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestSplitWithSingleCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := raftstore.NewSingleTestClusterStore(t,
		raftstore.DiskTestCluster,
		raftstore.OldTestCluster,
		raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(4)
			cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(2)
		}))

	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)

	kv := c.CreateTestKVClient(0)
	defer kv.Close()

	sid := c.GetShardByIndex(0, 0).ID
	kv.Set("k1", "v1", testWaitTimeout)
	kv.Set("k2", "v2", testWaitTimeout)

	c.WaitShardSplitByCount(sid, 1, testWaitTimeout)
	c.WaitRemovedByShardID(sid, testWaitTimeout)

	c.Restart()
	checkSplitWithStore(t, c, 0, sid)
}

func TestSplitWithMultiNodesCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := raftstore.NewTestClusterStore(t,
		raftstore.DiskTestCluster,
		raftstore.OldTestCluster,
		raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(4)
			cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(2)
		}))

	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)

	kv := c.CreateTestKVClient(0)
	defer kv.Close()

	sid := c.GetShardByIndex(0, 0).ID
	kv.Set("k1", "v1", testWaitTimeout)
	kv.Set("k2", "v2", testWaitTimeout)

	c.WaitShardSplitByCount(sid, 1, testWaitTimeout)
	c.WaitRemovedByShardID(sid, testWaitTimeout)

	// new shards created
	c.WaitShardByCountPerNode(2, testWaitTimeout)

	c.Restart()
	c.EveryStore(func(index int, store raftstore.Store) {
		checkSplitWithStore(t, c, index, sid)
	})
}

func checkSplitWithStore(t *testing.T, c raftstore.TestRaftCluster, index int, parentShardID uint64) {
	kv := c.CreateTestKVClient(index)
	defer kv.Close()

	// check new shards range
	assert.Empty(t, c.GetShardByIndex(index, 0).Start, "index %d", index)
	assert.Equal(t, []byte("k2"), c.GetShardByIndex(index, 0).End, "index %d", index)
	assert.Empty(t, c.GetShardByIndex(index, 1).End, "index %d", index)
	assert.Equal(t, []byte("k2"), c.GetShardByIndex(index, 1).Start, "index %d", index)

	// read
	v, err := kv.Get("k1", testWaitTimeout)
	assert.NoError(t, err, "index %d", index)
	assert.Equal(t, "v1", string(v), "index %d", index)

	v, err = kv.Get("k2", testWaitTimeout)
	assert.NoError(t, err, "index %d", index)
	assert.Equal(t, "v2", string(v), "index %d", index)
}
