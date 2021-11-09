package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

var (
	testWaitTimeout = time.Minute
)

func TestAppliedRules(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := raftstore.NewTestClusterStore(t, raftstore.WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
		cfg.Customize.CustomInitShardsFactory = func() []raftstore.Shard { return []raftstore.Shard{{Start: []byte("a"), End: []byte("b")}} }
	}))
	defer c.Stop()

	c.Start()
	c.WaitShardByCountPerNode(1, testWaitTimeout)

	assert.NoError(t, c.GetProphet().GetClient().PutPlacementRule(rpcpb.PlacementRule{
		GroupID: "g1",
		ID:      "id1",
		Count:   3,
		LabelConstraints: []rpcpb.LabelConstraint{
			{
				Key:    "c",
				Op:     rpcpb.In,
				Values: []string{"0", "1"},
			},
		},
	}))
	res := raftstore.NewResourceAdapterWithShard(raftstore.Shard{Start: []byte("b"), End: []byte("c"), Unique: "abc", RuleGroups: []string{"g1"}})
	err := c.GetProphet().GetClient().AsyncAddResourcesWithLeastPeers([]metadata.Resource{res}, []int{2})
	assert.NoError(t, err)

	c.WaitShardByCounts([]int{2, 2, 1}, testWaitTimeout)
}

func TestReadAndWriteAndRestart(t *testing.T) {
	c := raftstore.NewSingleTestClusterStore(t,
		raftstore.WithTestClusterLogLevel(zapcore.DebugLevel),
		raftstore.DiskTestCluster)
	defer c.Stop()

	c.Start()
	c.WaitLeadersByCount(1, testWaitTimeout)

	kv := c.CreateTestKVClient(0)
	defer kv.Close()

	for i := 0; i < 1; i++ {
		assert.NoError(t, kv.Set(fmt.Sprintf("k-%d", i), fmt.Sprintf("v-%d", i), testWaitTimeout))
	}

	c.Restart()
	c.WaitLeadersByCount(1, testWaitTimeout)

	kv2 := c.CreateTestKVClient(0)
	defer kv2.Close()

	for i := 0; i < 1; i++ {
		v, err := kv2.Get(fmt.Sprintf("k-%d", i), testWaitTimeout)
		assert.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("v-%d", i), v)
	}
}

func TestReadAndWriteAndRestartWithNodes(t *testing.T) {
	c := raftstore.NewTestClusterStore(t,
		raftstore.WithTestClusterNodeCount(3),
		raftstore.DiskTestCluster)
	defer c.Stop()

	c.Start()
	c.WaitLeadersByCount(1, testWaitTimeout)

	kv := c.CreateTestKVClient(0)
	defer kv.Close()

	for i := 0; i < 100; i++ {
		assert.NoError(t, kv.Set(fmt.Sprintf("k-%d", i), fmt.Sprintf("v-%d", i), testWaitTimeout))
	}

	c.Restart()
	c.WaitLeadersByCount(1, testWaitTimeout)

	for i := 0; i < 3; i++ {
		func(i int) {
			kv := c.CreateTestKVClient(i)
			defer kv.Close()

			for i := 0; i < 100; i++ {
				v, err := kv.Get(fmt.Sprintf("k-%d", i), testWaitTimeout)
				assert.NoError(t, err)
				assert.Equal(t, fmt.Sprintf("v-%d", i), v)
			}
		}(i)
	}
}
