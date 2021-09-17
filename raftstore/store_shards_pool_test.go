package raftstore

import (
	"fmt"
	"strings"
	"testing"

	"github.com/fagongzi/util/protoc"
	pconfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

type testResourcesAware struct {
	aware  pconfig.ResourcesAware
	adjust func(*core.CachedResource) *core.CachedResource
}

func (tra *testResourcesAware) ForeachWaittingCreateResources(do func(res metadata.Resource)) {
	tra.aware.ForeachWaittingCreateResources(do)
}
func (tra *testResourcesAware) ForeachResources(group uint64, fn func(res metadata.Resource)) {
	tra.aware.ForeachResources(group, fn)
}

func (tra *testResourcesAware) GetResource(resourceID uint64) *core.CachedResource {
	res := tra.aware.GetResource(resourceID)
	if tra.adjust != nil && res != nil {
		return tra.adjust(res)
	}

	return res
}

func TestShardPool(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
		cfg.Customize.CustomInitShardsFactory = func() []bhmetapb.Shard { return []bhmetapb.Shard{{Start: []byte("a"), End: []byte("b")}} }
	}))
	defer c.Stop()

	c.Start()

	p, err := c.GetStore(0).CreateResourcePool(metapb.ResourcePool{Group: 0, Capacity: 2, RangePrefix: []byte("b")})
	assert.NoError(t, err)
	assert.NotNil(t, p)

	// create 2th shards
	c.WaitShardByCountPerNode(3, testWaitTimeout)
	c.WaitLeadersByCount(3, testWaitTimeout)

	allocated, err := p.Alloc(0, []byte("propose1"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), allocated.AllocatedAt)
	assert.Equal(t, []byte("propose1"), allocated.Purpose)
	allocated, err = p.Alloc(0, []byte("propose1"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), allocated.AllocatedAt)
	assert.Equal(t, []byte("propose1"), allocated.Purpose)
	c.WaitShardStateChangedTo(allocated.ShardID, metapb.ResourceState_Running, testWaitTimeout)

	// create 3th shards
	c.WaitShardByCountPerNode(4, testWaitTimeout)
	c.WaitLeadersByCount(4, testWaitTimeout)

	allocated, err = p.Alloc(0, []byte("propose2"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), allocated.AllocatedAt)
	assert.Equal(t, []byte("propose2"), allocated.Purpose)
	c.WaitShardStateChangedTo(allocated.ShardID, metapb.ResourceState_Running, testWaitTimeout)

	// create 4th shards
	c.WaitShardByCountPerNode(5, testWaitTimeout)
	c.WaitLeadersByCount(5, testWaitTimeout)

	v, err := c.GetProphet().GetStorage().GetJobData(metapb.Job{Type: metapb.JobType_CreateResourcePool})
	assert.NoError(t, err)
	sp := &bhmetapb.ShardsPool{}
	protoc.MustUnmarshal(sp, v)
	assert.True(t, sp.Pools[0].Seq >= 3)
	assert.Equal(t, uint64(2), sp.Pools[0].AllocatedOffset)
	assert.Equal(t, 2, len(sp.Pools[0].AllocatedShards))

	// ensure the 4th shard is saved into prophet storage
	c.WaitShardStateChangedTo(c.GetShardByIndex(0, 4).ID, metapb.ResourceState_Running, testWaitTimeout)
	c.EveryStore(func(i int, ss Store) {
		s := ss.(*store)
		if s.shardPool.isStartedLocked() {
			tra := &testResourcesAware{aware: s.pd.GetBasicCluster(), adjust: func(res *core.CachedResource) *core.CachedResource {
				v := res.Clone(core.SetWrittenKeys(1))
				return v
			}}
			s.shardPool.gcAllocating(s.pd.GetStorage(), tra)
		}
	})

	v, err = c.GetProphet().GetStorage().GetJobData(metapb.Job{Type: metapb.JobType_CreateResourcePool})
	assert.NoError(t, err)
	sp = &bhmetapb.ShardsPool{}
	protoc.MustUnmarshal(sp, v)
	assert.Equal(t, uint64(4), sp.Pools[0].Seq)
	assert.Equal(t, uint64(2), sp.Pools[0].AllocatedOffset)
	assert.Equal(t, 0, len(sp.Pools[0].AllocatedShards))
}

func TestShardPoolWithFactory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewSingleTestClusterStore(t,
		WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
			cfg.Customize.CustomInitShardsFactory = func() []bhmetapb.Shard { return []bhmetapb.Shard{{Start: []byte("a"), End: []byte("b")}} }
			cfg.Customize.CustomShardPoolShardFactory = func(g uint64, start, end []byte, unique string, offsetInPool uint64) bhmetapb.Shard {
				return bhmetapb.Shard{
					Group:  g,
					Start:  []byte(fmt.Sprintf("b-%d", offsetInPool)),
					End:    []byte(fmt.Sprintf("b-%d", offsetInPool+1)),
					Unique: fmt.Sprintf("b-%d", offsetInPool),
				}
			}
		}))
	defer c.Stop()

	c.Start()

	p, err := c.GetStore(0).CreateResourcePool(metapb.ResourcePool{Group: 0, Capacity: 2, RangePrefix: []byte("b")})
	assert.NoError(t, err)
	assert.NotNil(t, p)

	c.WaitLeadersByCount(3, testWaitTimeout)
	kv := c.CreateTestKVClient(0)
	defer kv.Close()

	assert.NoError(t, kv.Set("b-1", "b1", testWaitTimeout))
	assert.NoError(t, kv.Set("b-2", "b2", testWaitTimeout))

	v, err := kv.Get("b-1", testWaitTimeout)
	assert.NoError(t, err)
	assert.Equal(t, "b1", v)

	v, err = kv.Get("b-2", testWaitTimeout)
	assert.NoError(t, err)
	assert.Equal(t, "b2", v)
}

func TestIssue192(t *testing.T) {
	defer leaktest.AfterTest(t)()

	wc := make(chan struct{})
	c := NewSingleTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
		cfg.Customize.CustomInitShardsFactory = func() []bhmetapb.Shard { return []bhmetapb.Shard{{Start: []byte("a"), End: []byte("b")}} }
		cfg.Test.ShardPoolCreateWaitC = wc
	}))
	defer c.Stop()

	c.Start()

	p, err := c.GetStore(0).CreateResourcePool(metapb.ResourcePool{Group: 0, Capacity: 1, RangePrefix: []byte("b")})
	assert.NoError(t, err)
	assert.NotNil(t, p)

	_, err = p.Alloc(0, []byte("purpose"))
	assert.Error(t, err)
	assert.False(t, strings.Contains(err.Error(), "timeout"))
}
