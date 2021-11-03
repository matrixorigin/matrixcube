package test

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := raftstore.NewSingleTestClusterStore(t, raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
		cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(4)
		cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(2)
	}))
	defer c.Stop()

	c.Start()
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

	// check new shards range
	assert.Empty(t, c.GetShardByIndex(0, 0).Start)
	assert.Equal(t, []byte("k2"), c.GetShardByIndex(0, 0).End)
	assert.Equal(t, []byte("k2"), c.GetShardByIndex(0, 1).Start)
	assert.Empty(t, c.GetShardByIndex(0, 1).End)

	// read
	testWaitTimeout = time.Second * 5
	v, err := kv.Get("k1", testWaitTimeout)
	assert.NoError(t, err)
	assert.Equal(t, "v1", string(v))

	v, err = kv.Get("k2", testWaitTimeout)
	assert.NoError(t, err)
	assert.Equal(t, "v2", string(v))
}
