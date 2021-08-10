package raftstore

import (
	"testing"
	"time"

	"github.com/fagongzi/util/protoc"
	pconfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
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
	if tra.adjust != nil {
		return tra.adjust(res)
	}

	return res
}

func TestShardPool(t *testing.T) {
	c := NewTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
		cfg.Customize.CustomInitShardsFactory = func() []bhmetapb.Shard { return []bhmetapb.Shard{{Start: []byte("a"), End: []byte("b")}} }
	}))
	defer c.Stop()

	c.Start()
	c.WaitShardByCount(t, 1, time.Second*10)

	p, err := c.stores[0].CreateResourcePool(metapb.ResourcePool{Group: 0, Capacity: 2, RangePrefix: []byte("b")})
	assert.NoError(t, err)
	assert.NotNil(t, p)

	// create 2 shards
	c.WaitShardByCount(t, 3, time.Second*10)

	allocated, err := p.Alloc(0, []byte("propose1"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), allocated.AllocatedAt)
	assert.Equal(t, []byte("propose1"), allocated.Purpose)
	allocated, err = p.Alloc(0, []byte("propose1"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), allocated.AllocatedAt)
	assert.Equal(t, []byte("propose1"), allocated.Purpose)
	c.WaitShardStateChangedTo(t, allocated.ShardID, metapb.ResourceState_Running, 10*time.Second)

	// create 3 shards
	c.WaitShardByCount(t, 4, time.Second*10)

	allocated, err = p.Alloc(0, []byte("propose2"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), allocated.AllocatedAt)
	assert.Equal(t, []byte("propose2"), allocated.Purpose)
	c.WaitShardStateChangedTo(t, allocated.ShardID, metapb.ResourceState_Running, 10*time.Second)

	// create 4 shards
	c.WaitShardByCount(t, 5, time.Second*10)

	store := c.GetProphet().GetStorage()
	v, err := store.GetJobData(metapb.Job{Type: metapb.JobType_CreateResourcePool})
	assert.NoError(t, err)
	sp := &bhmetapb.ShardsPool{}
	protoc.MustUnmarshal(sp, v)
	assert.Equal(t, uint64(4), sp.Pools[0].Seq)
	assert.Equal(t, uint64(2), sp.Pools[0].AllocatedOffset)
	assert.Equal(t, 2, len(sp.Pools[0].AllocatedShards))

	tra := &testResourcesAware{aware: c.GetProphet().GetBasicCluster(), adjust: func(res *core.CachedResource) *core.CachedResource {
		v := res.Clone(core.SetWrittenKeys(1))
		return v
	}}
	for _, s := range c.stores {
		if s.shardPool.isStartedLocked() {
			s.shardPool.gcAllocating(store, tra)
		}
	}

	v, err = store.GetJobData(metapb.Job{Type: metapb.JobType_CreateResourcePool})
	assert.NoError(t, err)
	sp = &bhmetapb.ShardsPool{}
	protoc.MustUnmarshal(sp, v)
	assert.Equal(t, uint64(4), sp.Pools[0].Seq)
	assert.Equal(t, uint64(2), sp.Pools[0].AllocatedOffset)
	assert.Equal(t, 0, len(sp.Pools[0].AllocatedShards))
}
