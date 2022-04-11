// Copyright 2022 MatrixOrigin.
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

package client

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestKVSetAndGetAndDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := raftstore.NewSingleTestClusterStore(t)
	c.Start()
	defer c.Stop()

	s := NewClient(Cfg{Store: c.GetStore(0)})
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Stop())
	}()

	kv := NewKVClient(s, 0, rpcpb.SelectLeader)
	defer kv.Close()

	k1 := []byte("k1")
	v1 := []byte("v1")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	f := kv.Set(ctx, k1, v1)
	defer f.Close()
	assert.NoError(t, f.GetError())

	f2 := kv.Get(ctx, k1)
	defer f2.Close()
	resp, err := f2.GetKVGetResponse()
	assert.NoError(t, err)
	assert.Equal(t, v1, resp.Value)

	f3 := kv.Delete(ctx, k1)
	defer f3.Close()
	assert.NoError(t, f3.GetError())

	f4 := kv.Get(ctx, k1)
	defer f4.Close()
	resp, err = f4.GetKVGetResponse()
	assert.NoError(t, err)
	assert.Empty(t, resp.Value)
}

func TestKVBatchSetAndBatchGet(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := raftstore.NewSingleTestClusterStore(t)
	c.Start()
	defer c.Stop()

	s := NewClient(Cfg{Store: c.GetStore(0)})
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Stop())
	}()

	kv := NewKVClient(s, 0, rpcpb.SelectLeader)
	defer kv.Close()

	k1 := []byte("k1")
	v1 := []byte("v1")
	k2 := []byte("k2")
	v2 := []byte("v2")
	k3 := []byte("k3")
	v3 := []byte("v3")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	f := kv.BatchSet(ctx, [][]byte{k1, k2, k3}, [][]byte{v1, v2, v3})
	defer f.Close()
	assert.NoError(t, f.GetError())

	f2 := kv.BatchGet(ctx, [][]byte{k3, k2, k1})
	defer f2.Close()
	resp, err := f2.GetKVBatchGetResponse()
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{v1, v2, v3}, resp.Values)
}

func TestKVBatchGetWithMultiShards(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := raftstore.NewSingleTestClusterStore(t, raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
		cfg.Customize.CustomInitShardsFactory = func() []metapb.Shard {
			return []metapb.Shard{
				{Start: []byte("k1"), End: []byte("k2")},
				{Start: []byte("k2"), End: []byte("k3")},
				{Start: []byte("k3"), End: []byte("k4")},
				{Start: []byte("k4"), End: []byte("k5")},
			}
		}
	}))
	c.Start()
	defer c.Stop()

	s := NewClient(Cfg{Store: c.GetStore(0)})
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Stop())
	}()

	kv := NewKVClient(s, 0, rpcpb.SelectLeader)
	defer kv.Close()

	k1 := []byte("k1")
	v1 := []byte("v1")
	k2 := []byte("k2")
	v2 := []byte("v2")
	k3 := []byte("k3")
	v3 := []byte("v3")
	k4 := []byte("k4")
	v4 := []byte("v5")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	f := kv.Set(ctx, k1, v1)
	assert.NoError(t, f.GetError())
	f.Close()
	f = kv.Set(ctx, k2, v2)
	assert.NoError(t, f.GetError())
	f.Close()
	f = kv.Set(ctx, k3, v3)
	assert.NoError(t, f.GetError())
	f.Close()
	f = kv.Set(ctx, k4, v4)
	assert.NoError(t, f.GetError())
	f.Close()

	f = kv.BatchGet(ctx, [][]byte{k3, k2, k1, k4})
	resp, err := f.GetKVBatchGetResponse()
	f.Close()
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{v1, v2, v3, v4}, resp.Values)

	f = kv.BatchGet(ctx, [][]byte{k3, k1})
	resp, err = f.GetKVBatchGetResponse()
	f.Close()
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{v1, v3}, resp.Values)
}

func TestKVBatchGetWithNoRetryError(t *testing.T) {
	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "10/11,20/21,30/31")
	addTestShard(router, 2, "100/101,200/201,300/301")
	addTestShard(router, 3, "1000/1001,200/2001,3000/3001")

	k1 := format.Uint64ToBytes(1)
	k2 := format.Uint64ToBytes(2)
	k3 := format.Uint64ToBytes(3)
	n := 0
	var lock sync.Mutex
	s := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
		lock.Lock()
		defer lock.Unlock()

		if n == 0 {
			n++
			return rpcpb.ResponseBatch{}, raftstore.NewShardUnavailableErr(r.ToShard)
		} else if n == 1 {
			n++
			return rpcpb.ResponseBatch{}, raftstore.ErrKeysNotInShard
		}

		var req rpcpb.KVBatchGetRequest
		protoc.MustUnmarshal(&req, r.Cmd)

		return rpcpb.ResponseBatch{Responses: []rpcpb.Response{
			{ID: r.ID, CustomType: r.CustomType, Type: r.Type, Value: protoc.MustMarshal(&rpcpb.KVBatchGetResponse{
				Values: req.Keys,
			})},
		}}, nil
	})
	defer func() {
		assert.NoError(t, s.Stop())
	}()

	kv := NewKVClient(s, 0, rpcpb.SelectLeader)
	defer kv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	f := kv.BatchGet(ctx, [][]byte{k3, k2, k1})
	resp, err := f.GetKVBatchGetResponse()
	f.Close()
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{k1, k2, k3}, resp.Values)
}

func TestKVScan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := raftstore.NewSingleTestClusterStore(t, raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
		cfg.Customize.CustomInitShardsFactory = func() []metapb.Shard {
			return []metapb.Shard{
				{Start: []byte("k1"), End: []byte("k2")},
				{Start: []byte("k2"), End: []byte("k3")},
				{Start: []byte("k3"), End: []byte("k4")},
				{Start: []byte("k4"), End: []byte("k5")},
				{Start: []byte("k5"), End: nil},
			}
		}
	}))
	c.Start()
	defer c.Stop()

	s := NewClient(Cfg{Store: c.GetStore(0)})
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Stop())
	}()

	kv := NewKVClient(s, 0, rpcpb.SelectLeader)
	defer kv.Close()

	k1 := []byte("k1")
	v1 := []byte("v1")
	k2 := []byte("k2")
	v2 := []byte("v2")
	k3 := []byte("k3")
	v3 := []byte("v3")
	k4 := []byte("k4")
	v4 := []byte("v4")
	k5 := []byte("k5")
	v5 := []byte("v5")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	f := kv.Set(ctx, k1, v1)
	assert.NoError(t, f.GetError())
	f.Close()
	f = kv.Set(ctx, k2, v2)
	assert.NoError(t, f.GetError())
	f.Close()
	f = kv.Set(ctx, k3, v3)
	assert.NoError(t, f.GetError())
	f.Close()
	f = kv.Set(ctx, k4, v4)
	assert.NoError(t, f.GetError())
	f.Close()
	f = kv.Set(ctx, k5, v5)
	assert.NoError(t, f.GetError())
	f.Close()

	cases := []struct {
		start, end   []byte
		options      []ScanOption
		expectKeys   [][]byte
		expectValues [][]byte
	}{
		{
			start:        k1,
			end:          []byte("k5"),
			expectKeys:   [][]byte{k1, k2, k3, k4},
			expectValues: [][]byte{nil, nil, nil, nil},
		},
		{
			start:        k1,
			end:          []byte("k5"),
			options:      []ScanOption{ScanWithValue()},
			expectKeys:   [][]byte{k1, k2, k3, k4},
			expectValues: [][]byte{v1, v2, v3, v4},
		},
		{
			start:        k1,
			end:          []byte("k6"),
			options:      []ScanOption{ScanWithValue(), ScanWithLimit(1)},
			expectKeys:   [][]byte{k1, k2, k3, k4, k5},
			expectValues: [][]byte{v1, v2, v3, v4, v5},
		},
	}

	for _, c := range cases {
		var keys [][]byte
		var values [][]byte
		kv.Scan(ctx, c.start, c.end, func(key, value []byte) (bool, error) {
			keys = append(keys, key)
			values = append(values, value)
			return true, nil
		}, c.options...)
		assert.Equal(t, c.expectKeys, keys)
		assert.Equal(t, c.expectValues, values)
	}
}

func TestKVParallelScan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := raftstore.NewSingleTestClusterStore(t, raftstore.WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
		cfg.Customize.CustomInitShardsFactory = func() []metapb.Shard {
			return []metapb.Shard{
				{Start: []byte("k1"), End: []byte("k2")},
				{Start: []byte("k2"), End: []byte("k3")},
				{Start: []byte("k3"), End: []byte("k4")},
				{Start: []byte("k4"), End: []byte("k5")},
				{Start: []byte("k5"), End: nil},
			}
		}
	}))
	c.Start()
	defer c.Stop()

	s := NewClient(Cfg{Store: c.GetStore(0)})
	assert.NoError(t, s.Start())
	defer func() {
		assert.NoError(t, s.Stop())
	}()

	kv := NewKVClient(s, 0, rpcpb.SelectLeader)
	defer kv.Close()

	k1 := []byte("k1")
	v1 := []byte("v1")
	k2 := []byte("k2")
	v2 := []byte("v2")
	k3 := []byte("k3")
	v3 := []byte("v3")
	k4 := []byte("k4")
	v4 := []byte("v4")
	k5 := []byte("k5")
	v5 := []byte("v5")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	f := kv.Set(ctx, k1, v1)
	assert.NoError(t, f.GetError())
	f.Close()
	f = kv.Set(ctx, k2, v2)
	assert.NoError(t, f.GetError())
	f.Close()
	f = kv.Set(ctx, k3, v3)
	assert.NoError(t, f.GetError())
	f.Close()
	f = kv.Set(ctx, k4, v4)
	assert.NoError(t, f.GetError())
	f.Close()
	f = kv.Set(ctx, k5, v5)
	assert.NoError(t, f.GetError())
	f.Close()

	cases := []struct {
		start, end   []byte
		options      []ScanOption
		expectKeys   [][]byte
		expectValues [][]byte
	}{
		{
			start:        k1,
			end:          []byte("k5"),
			expectKeys:   [][]byte{k1, k2, k3, k4},
			expectValues: [][]byte{nil, nil, nil, nil},
		},
		{
			start:        k1,
			end:          []byte("k5"),
			options:      []ScanOption{ScanWithValue()},
			expectKeys:   [][]byte{k1, k2, k3, k4},
			expectValues: [][]byte{v1, v2, v3, v4},
		},
		{
			start:        k1,
			end:          []byte("k6"),
			options:      []ScanOption{ScanWithValue(), ScanWithLimit(1)},
			expectKeys:   [][]byte{k1, k2, k3, k4, k5},
			expectValues: [][]byte{v1, v2, v3, v4, v5},
		},
	}

	for _, c := range cases {
		var keys [][]byte
		var values [][]byte
		var lock sync.Mutex
		kv.ParallelScan(ctx, c.start, c.end, func(key, value []byte) (bool, error) {
			lock.Lock()
			defer lock.Unlock()
			keys = append(keys, key)
			values = append(values, value)
			return true, nil
		}, c.options...)
		sort.Slice(keys, func(i, j int) bool {
			return bytes.Compare(keys[i], keys[j]) < 0
		})
		sort.Slice(values, func(i, j int) bool {
			return bytes.Compare(values[i], values[j]) < 0
		})
		assert.Equal(t, c.expectKeys, keys)
		assert.Equal(t, c.expectValues, values)
	}
}

func addTestShard(router raftstore.Router, shardID uint64, shardInfo string) {
	b := raftstore.NewTestDataBuilder()
	s := b.CreateShard(shardID, shardInfo)
	for _, r := range s.Replicas {
		router.UpdateStore(metapb.Store{ID: r.StoreID, ClientAddress: "test-cli"})
	}
	router.UpdateShard(s)
	router.UpdateLeader(s.ID, s.Replicas[0].ID)
}

func newTestRaftstoreClient(router raftstore.Router, handler func(rpcpb.Request) (rpcpb.ResponseBatch, error)) Client {
	sp, _ := raftstore.NewMockShardsProxy(router, handler)
	c := NewClientWithOptions(CreateWithShardsProxy(sp))
	if err := c.Start(); err != nil {
		panic(err)
	}
	return c
}
