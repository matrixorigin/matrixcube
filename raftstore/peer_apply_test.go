package raftstore

/*
import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

// TODO: re-enable this test

func TestIssue97(t *testing.T) {
	// We fogot reset raftWB in `delegate` every time as below. So a new commitedEntries will has it's prev
	// for idx, entry := range commitedEntries {
	//  ...
	//    if idx > 0 {
	// 	      d.ctx.reset()
	// 	      req.Reset()
	//    }
	// }
	// So when `commitedEntries` is currently executed, it will include the previous residual data.

	// Steps to reproduce this issue:
	// 1. write data to shard1, then shard1 split to shard1, shard2
	// 2. write data to shard2, then shard2 split to shard2, shard3
	// 3. At this point, shard2's init state(shard init state and raft applied state) is in the shard1's raftWB,
	//    and shard2 has changed its raft applied index and write to disk.
	// 4. write data to shard1, so shard2's init state which inthe shard1's raftWB will execute with current write.
	// 5. write data to shard2, restart the cluster between shard2 save last log index and shard2 save applied state
	// 6. shard2 has wrong initial state, and the split req will be executed again, and a new shard3 will be generated,
	//    So shard3 duplicated, cluster crash.

	defer leaktest.AfterTest(t)()
	c := NewSingleTestClusterStore(t,
		DisableScheduleTestCluster,
		DiskTestCluster,
		SetCMDTestClusterHandler,
		GetCMDTestClusterHandler,
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(20)
			cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(10)
		}))
	defer c.Stop()

	c.Start()
	c.WaitLeadersByCount(1, testWaitTimeout)

	kv := c.CreateTestKVClient(0)
	defer kv.Close()

	testWaitTimeout = time.Second * 10

	// step1 write to shard2 => split to 2 shards [nil, key2), [key2, nil)
	assert.NoError(t, kv.Set("key1", "value11", testWaitTimeout))
	assert.NoError(t, kv.Set("key2", "value22", testWaitTimeout))

	c.WaitLeadersByCount(2, testWaitTimeout)
	assert.Empty(t, c.GetShardByIndex(0, 0).Start)
	assert.Equal(t, "key2", string(c.GetShardByIndex(0, 0).End))
	assert.Equal(t, "key2", string(c.GetShardByIndex(0, 1).Start))
	assert.Empty(t, c.GetShardByIndex(0, 1).End)

	// step2 write to shard2 => split to 3 shards [nil, key2), [key2, key3), [key3, nil)
	assert.NoError(t, kv.Set("key3", "value33", testWaitTimeout))

	c.WaitLeadersByCount(3, testWaitTimeout)
	assert.Empty(t, c.GetShardByIndex(0, 0).Start)
	assert.Equal(t, "key2", string(c.GetShardByIndex(0, 0).End))
	assert.Equal(t, "key2", string(c.GetShardByIndex(0, 1).Start))
	assert.Equal(t, "key3", string(c.GetShardByIndex(0, 1).End))
	assert.Equal(t, "key3", string(c.GetShardByIndex(0, 2).Start))
	assert.Empty(t, c.GetShardByIndex(0, 2).End)

	// step3 write to shard1, rewrite shard2's state
	assert.NoError(t, kv.Set("a", "1", testWaitTimeout))

	// step4 write data to shard2 and only change shard2's raft local state
	c.GetStore(0).GetConfig().Test.Shards[c.GetShardByID(0, 5).ID] = &config.TestShardConfig{
		SkipSaveRaftApplyState: true,
	}
	v, _ := c.GetStore(0).(*store).delegates.Load(c.GetShardByID(0, 5).ID)
	d := v.(*applyDelegate)
	d.ctx.raftWB.Reset()
	assert.NoError(t, kv.Set("key2", "1", testWaitTimeout))

	c.Restart()
	c.WaitLeadersByCount(3, testWaitTimeout)

	kv2 := c.CreateTestKVClient(0)
	defer kv2.Close()
	_, err := kv2.Get("key2", testWaitTimeout)
	assert.NoError(t, err)
}*/
