// Copyright 2021 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless assertd by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package raftstore

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

var (
	testWaitTimeout = time.Minute
)

func TestSplitWithSingleCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()
	c := NewSingleTestClusterStore(t,
		DiskTestCluster,
		OldTestCluster,
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(4)
			cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(2)
		}))

	c.Start()
	defer c.Stop()

	sid := prepareSplit(t, c)
	c.Restart()
	checkSplitWithStore(t, c, 0, sid)

	v, err := c.GetStore(0).Prophet().GetStorage().GetResource(sid)
	assert.NoError(t, err)
	assert.Nil(t, v)
}

func TestSplitWithMultiNodesCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()
	c := NewTestClusterStore(t,
		DiskTestCluster,
		OldTestCluster,
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(4)
			cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(2)
		}))

	c.Start()
	defer c.Stop()

	sid := prepareSplit(t, c)
	c.Restart()
	c.EveryStore(func(index int, store Store) {
		checkSplitWithStore(t, c, index, sid)
	})

	v, err := c.GetStore(0).Prophet().GetStorage().GetResource(sid)
	assert.NoError(t, err)
	assert.Nil(t, v)
}

func prepareSplit(t *testing.T, c TestRaftCluster) uint64 {
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
	return sid
}

func checkSplitWithStore(t *testing.T, c TestRaftCluster, index int, parentShardID uint64) {
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

	// check router
	shard, _ := c.GetStore(index).GetRouter().SelectShard(0, []byte("k1"))
	assert.NotEqual(t, parentShardID, shard.ID)
	shard, _ = c.GetStore(index).GetRouter().SelectShard(0, []byte("k2"))
	assert.NotEqual(t, parentShardID, shard.ID)
}
