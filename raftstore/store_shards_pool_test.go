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

	p, err := c.stores[0].CreateResourcePool(metapb.ResourcePool{Group: 0, Capacity: 2, RangePrefix: []byte("b")})
	assert.NoError(t, err)
	assert.NotNil(t, p)

	// create 2th shards
	c.WaitShardByCount(t, 3, testWaitTimeout)

	allocated, err := p.Alloc(0, []byte("propose1"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), allocated.AllocatedAt)
	assert.Equal(t, []byte("propose1"), allocated.Purpose)
	allocated, err = p.Alloc(0, []byte("propose1"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), allocated.AllocatedAt)
	assert.Equal(t, []byte("propose1"), allocated.Purpose)
	c.WaitShardStateChangedTo(t, allocated.ShardID, metapb.ResourceState_Running, testWaitTimeout)

	// create 3th shards
	c.WaitShardByCount(t, 4, testWaitTimeout)

	allocated, err = p.Alloc(0, []byte("propose2"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), allocated.AllocatedAt)
	assert.Equal(t, []byte("propose2"), allocated.Purpose)
	c.WaitShardStateChangedTo(t, allocated.ShardID, metapb.ResourceState_Running, testWaitTimeout)

	// create 4th shards
	c.WaitLeadersByCount(t, 5, testWaitTimeout)

	store := c.GetProphet().GetStorage()
	v, err := store.GetJobData(metapb.Job{Type: metapb.JobType_CreateResourcePool})
	assert.NoError(t, err)
	sp := &bhmetapb.ShardsPool{}
	protoc.MustUnmarshal(sp, v)
	assert.True(t, sp.Pools[0].Seq >= 3)
	assert.Equal(t, uint64(2), sp.Pools[0].AllocatedOffset)
	assert.Equal(t, 2, len(sp.Pools[0].AllocatedShards))

	// ensure the 4th shard is saved into prophet storage
	c.WaitShardStateChangedTo(t, c.GetShardByIndex(4).ID, metapb.ResourceState_Running, testWaitTimeout)
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

func TestShardPoolWithFactory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewSingleTestClusterStore(t,
		SetCMDTestClusterHandler,
		GetCMDTestClusterHandler,
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

	p, err := c.stores[0].CreateResourcePool(metapb.ResourcePool{Group: 0, Capacity: 2, RangePrefix: []byte("b")})
	assert.NoError(t, err)
	assert.NotNil(t, p)

	c.WaitLeadersByCount(t, 3, testWaitTimeout)

	resp, err := sendTestReqs(c.stores[0], testWaitTimeout, nil, nil,
		createTestWriteReq("w1", "b-1", "b1"),
		createTestWriteReq("w2", "b-2", "b2"))
	assert.NoError(t, err)
	assert.Equal(t, "OK", string(resp["w1"].Responses[0].Value))
	assert.Equal(t, "OK", string(resp["w2"].Responses[0].Value))

	resp, err = sendTestReqs(c.stores[0], testWaitTimeout, nil, nil,
		createTestReadReq("r1", "b-1"),
		createTestReadReq("r2", "b-2"))
	assert.NoError(t, err)
	assert.Equal(t, "b1", string(resp["r1"].Responses[0].Value))
	assert.Equal(t, "b2", string(resp["r2"].Responses[0].Value))
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

	p, err := c.stores[0].CreateResourcePool(metapb.ResourcePool{Group: 0, Capacity: 1, RangePrefix: []byte("b")})
	assert.NoError(t, err)
	assert.NotNil(t, p)

	_, err = p.Alloc(0, []byte("purpose"))
	assert.Error(t, err)
	assert.False(t, strings.Contains(err.Error(), "timeout"))
}
