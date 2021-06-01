package raftstore

import (
	"bytes"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/stretchr/testify/assert"
)

func TestClusterStartAndStop(t *testing.T) {
	c := NewTestClusterStore(t, "", nil, nil, nil)
	defer c.Stop()

	c.Start()

	c.WaitShardByCount(t, 1, time.Second*10)
	c.CheckShardCount(t, 1)
}

func TestAddAndRemoveShard(t *testing.T) {
	c := NewTestClusterStore(t, "", func(cfg *config.Config) {
		cfg.Customize.CustomInitShardsFactory = func() []bhmetapb.Shard { return []bhmetapb.Shard{{Start: []byte("a"), End: []byte("b")}} }
	}, nil, nil)
	defer c.Stop()

	c.Start()
	c.WaitShardByCount(t, 1, time.Second*10)

	err := c.GetProphet().GetClient().AsyncAddResources(NewResourceAdapterWithShard(bhmetapb.Shard{Start: []byte("b"), End: []byte("c"), Unique: "abc"}))
	assert.NoError(t, err)

	c.WaitShardByCount(t, 2, time.Second*10)
	c.CheckShardCount(t, 2)

	id := c.GetShardByIndex(1).ID
	c.WaitShardStateChangedTo(t, id, metapb.ResourceState_Running, time.Second*10)

	assert.NoError(t, c.GetProphet().GetClient().AsyncRemoveResources(id))
	c.WaitRemovedByShardID(t, id, time.Second*10)
	c.CheckShardCount(t, 1)
	c.WaitShardStateChangedTo(t, id, metapb.ResourceState_Removed, time.Second*10)
}

func TestAppliedRules(t *testing.T) {
	c := NewTestClusterStore(t, "", func(cfg *config.Config) {
		cfg.Customize.CustomInitShardsFactory = func() []bhmetapb.Shard { return []bhmetapb.Shard{{Start: []byte("a"), End: []byte("b")}} }
	}, nil, nil)
	defer c.Stop()

	c.Start()
	c.WaitShardByCount(t, 1, time.Second*10)

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
	res := NewResourceAdapterWithShard(bhmetapb.Shard{Start: []byte("b"), End: []byte("c"), Unique: "abc", RuleGroups: []string{"g1"}})
	err := c.GetProphet().GetClient().AsyncAddResourcesWithLeastPeers([]metadata.Resource{res}, []int{2})
	assert.NoError(t, err)

	c.WaitShardByCounts(t, [3]int{2, 2, 1}, time.Second*10)
}

func TestSplit(t *testing.T) {
	c := NewTestClusterStore(t, "", nil, nil, nil)
	defer c.Stop()

	c.Start()
	c.WaitShardByCount(t, 1, time.Second*10)

	c.Set(EncodeDataKey(0, []byte("key1")), []byte("value11"))
	c.Set(EncodeDataKey(0, []byte("key2")), []byte("value22"))
	c.Set(EncodeDataKey(0, []byte("key3")), []byte("value33"))

	c.WaitShardByCount(t, 3, time.Second*10)
	c.CheckShardRange(t, 0, nil, []byte("key2"))
	c.CheckShardRange(t, 1, []byte("key2"), []byte("key3"))
	c.CheckShardRange(t, 2, []byte("key3"), nil)
}

func TestCustomSplit(t *testing.T) {
	target := EncodeDataKey(0, []byte("key2"))
	c := NewTestClusterStore(t, "", func(cfg *config.Config) {
		cfg.Customize.CustomSplitCheckFunc = func(shard bhmetapb.Shard) (uint64, uint64, [][]byte, error) {
			store := cfg.Storage.DataStorageFactory(shard.Group, shard.ID)
			endGroup := shard.Group
			if len(shard.End) == 0 {
				endGroup++
			}
			size := uint64(0)
			keys := uint64(0)
			hasTarget := false
			store.Scan(EncodeDataKey(shard.Group, shard.Start), EncodeDataKey(endGroup, shard.End), func(key, value []byte) (bool, error) {
				size += uint64(len(key) + len(value))
				keys++
				if bytes.Equal(key, target) {
					hasTarget = true
				}
				return true, nil
			}, false)

			if len(shard.End) == 0 && len(shard.Start) == 0 && hasTarget {
				return size, keys, [][]byte{target}, nil
			}

			return size, keys, nil, nil
		}
	}, nil, nil)
	defer c.Stop()

	c.Start()
	c.WaitShardByCount(t, 1, time.Second*10)

	c.Set(EncodeDataKey(0, []byte("key1")), []byte("value11"))
	c.Set(EncodeDataKey(0, []byte("key2")), []byte("value22"))
	c.Set(EncodeDataKey(0, []byte("key3")), []byte("value33"))

	c.WaitShardByCount(t, 2, time.Second*10)
	c.CheckShardRange(t, 0, nil, []byte("key2"))
	c.CheckShardRange(t, 1, []byte("key2"), nil)
}
