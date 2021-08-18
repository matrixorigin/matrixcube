// Copyright 2020 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package raftstore

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/stretchr/testify/assert"
)

func TestClusterStartAndStop(t *testing.T) {
	c := NewTestClusterStore(t, WithTestClusterLogLevel("info"))
	defer c.Stop()

	c.Start()

	c.WaitShardByCount(t, 1, time.Second*10)
	c.CheckShardCount(t, 1)
}

func TestIssue123(t *testing.T) {
	c := NewSingleTestClusterStore(t,
		SetCMDTestClusterHandler,
		WithTestClusterLogLevel("info"),
		WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
			cfg.Customize.CustomInitShardsFactory = func() []bhmetapb.Shard { return []bhmetapb.Shard{{Start: []byte("a"), End: []byte("b")}} }
		}))
	defer c.Stop()

	c.Start()
	c.WaitShardByCount(t, 1, time.Second*10)

	p, err := c.stores[0].CreateResourcePool(metapb.ResourcePool{
		RangePrefix: []byte("b"),
		Capacity:    20,
	})
	assert.NoError(t, err)
	assert.NotNil(t, p)

	c.WaitShardByCount(t, 21, time.Second)

	for i := 0; i < 20; i++ {
		s, err := p.Alloc(0, []byte(fmt.Sprintf("%d", i)))
		assert.NoError(t, err)

		id := fmt.Sprintf("w%d", i)
		resp, err := sendTestReqs(c.stores[0], time.Second*10, nil, nil, createTestWriteReq(id, string(c.GetShardByID(s.ShardID).Start), id))
		assert.NoError(t, err)
		assert.Equal(t, "OK", string(resp[id].Responses[0].Value))
	}
}

func TestAddShardWithMultiGroups(t *testing.T) {
	c := NewTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
		cfg.ShardGroups = 2
		cfg.Prophet.Replication.Groups = []uint64{0, 1}
		cfg.Customize.CustomInitShardsFactory = func() []bhmetapb.Shard {
			return []bhmetapb.Shard{{Start: []byte("a"), End: []byte("b")}, {Group: 1, Start: []byte("a"), End: []byte("b")}}
		}
	}))
	defer c.Stop()

	c.Start()
	c.WaitShardByCount(t, 2, time.Second*10)

	err := c.GetProphet().GetClient().AsyncAddResources(NewResourceAdapterWithShard(bhmetapb.Shard{Start: []byte("b"), End: []byte("c"), Unique: "abc", Group: 1}))
	assert.NoError(t, err)
	c.WaitShardByCount(t, 3, time.Second*10)
}

func TestAppliedRules(t *testing.T) {
	c := NewTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
		cfg.Customize.CustomInitShardsFactory = func() []bhmetapb.Shard { return []bhmetapb.Shard{{Start: []byte("a"), End: []byte("b")}} }
	}))
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
	c := NewSingleTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
		cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(20)
		cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(10)
	}))
	defer c.Stop()

	c.Start()
	c.WaitShardByCount(t, 1, time.Second*10)

	c.set(EncodeDataKey(0, []byte("key1")), []byte("value11"))
	c.set(EncodeDataKey(0, []byte("key2")), []byte("value22"))
	c.set(EncodeDataKey(0, []byte("key3")), []byte("value33"))

	c.WaitShardByCount(t, 3, time.Second*10)
	c.CheckShardRange(t, 0, nil, []byte("key2"))
	c.CheckShardRange(t, 1, []byte("key2"), []byte("key3"))
	c.CheckShardRange(t, 2, []byte("key3"), nil)
}

func TestCustomSplit(t *testing.T) {
	target := EncodeDataKey(0, []byte("key2"))
	c := NewSingleTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
		cfg.Customize.CustomSplitCheckFuncFactory = func(group uint64) func(shard bhmetapb.Shard) (uint64, uint64, [][]byte, error) {
			return func(shard bhmetapb.Shard) (uint64, uint64, [][]byte, error) {
				store := cfg.Storage.DataStorageFactory(shard.Group, shard.ID).(storage.KVStorage)
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
		}
	}))
	defer c.Stop()

	c.Start()
	c.WaitShardByCount(t, 1, time.Second*10)

	c.set(EncodeDataKey(0, []byte("key1")), []byte("value11"))
	c.set(EncodeDataKey(0, []byte("key2")), []byte("value22"))
	c.set(EncodeDataKey(0, []byte("key3")), []byte("value33"))

	c.WaitShardByCount(t, 2, time.Second*10)
	c.CheckShardRange(t, 0, nil, []byte("key2"))
	c.CheckShardRange(t, 1, []byte("key2"), nil)
}

func TestSpeedupAddShard(t *testing.T) {
	c := NewTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
		cfg.Raft.TickInterval = typeutil.NewDuration(time.Second * 2)
		cfg.Customize.CustomInitShardsFactory = func() []bhmetapb.Shard { return []bhmetapb.Shard{{Start: []byte("a"), End: []byte("b")}} }
	}))
	defer c.Stop()

	c.Start()
	c.WaitShardByCount(t, 1, time.Second*10)

	err := c.GetProphet().GetClient().AsyncAddResources(NewResourceAdapterWithShard(bhmetapb.Shard{Start: []byte("b"), End: []byte("c"), Unique: "abc"}))
	assert.NoError(t, err)

	c.WaitShardByCount(t, 2, time.Second*10)
	c.CheckShardCount(t, 2)

	id := c.GetShardByIndex(1).ID
	c.WaitShardStateChangedTo(t, id, metapb.ResourceState_Running, time.Second*5)
}

func createTestWriteReq(id, k, v string) *raftcmdpb.Request {
	req := pb.AcquireRequest()
	req.ID = []byte(id)
	req.CustemType = 1
	req.Type = raftcmdpb.CMDType_Write
	req.Key = []byte(k)
	req.Cmd = []byte(v)
	return req
}

func createTestReadReq(id, k string) *raftcmdpb.Request {
	req := pb.AcquireRequest()
	req.ID = []byte(id)
	req.CustemType = 2
	req.Type = raftcmdpb.CMDType_Read
	req.Key = []byte(k)
	return req
}

func sendTestReqs(s Store, timeout time.Duration, waiterC chan string, waiters map[string]string, reqs ...*raftcmdpb.Request) (map[string]*raftcmdpb.RaftCMDResponse, error) {
	if waiters == nil {
		waiters = make(map[string]string)
	}

	resps := make(map[string]*raftcmdpb.RaftCMDResponse)
	c := make(chan *raftcmdpb.RaftCMDResponse, len(reqs))
	defer close(c)

	cb := func(resp *raftcmdpb.RaftCMDResponse) {
		c <- resp
	}

	for _, req := range reqs {
		if v, ok := waiters[string(req.ID)]; ok {
		OUTER:
			for {
				select {
				case <-time.After(timeout):
					return nil, errors.New("timeout error")
				case c := <-waiterC:
					if c == v {
						break OUTER
					}
				}
			}
		}
		err := s.(*store).onRequestWithCB(req, cb)
		if err != nil {
			return nil, err
		}
	}

	for {
		select {
		case <-time.After(timeout):
			return nil, errors.New("timeout error")
		case resp := <-c:
			if len(resp.Responses) <= 1 {
				resps[string(resp.Responses[0].ID)] = resp
			} else {
				for i := range resp.Responses {
					r := &raftcmdpb.RaftCMDResponse{}
					protoc.MustUnmarshal(r, protoc.MustMarshal(resp))
					r.Responses = resp.Responses[i : i+1]
					resps[string(r.Responses[0].ID)] = r
				}
			}

			if len(resps) == len(reqs) {
				return resps, nil
			}
		}
	}
}
