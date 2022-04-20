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

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/raftstore"
	keysutil "github.com/matrixorigin/matrixcube/util/keys"
	"github.com/matrixorigin/matrixcube/util/stop"
)

// ScanOption scan options
type ScanOption func(*rpcpb.KVScanRequest)

type ScanHandler func(key, value []byte) (bool, error)

// ScanWithValue scan with value
func ScanWithValue() ScanOption {
	return func(req *rpcpb.KVScanRequest) {
		req.WithValue = true
	}
}

// ScanWithLimit set maximum count of scanned data
func ScanWithLimit(value uint64) ScanOption {
	return func(req *rpcpb.KVScanRequest) {
		req.Limit = value
	}
}

// ScanWithLimitBytes set maximum bytes of scanned data
func ScanWithLimitBytes(value uint64) ScanOption {
	return func(req *rpcpb.KVScanRequest) {
		req.LimitBytes = value
	}
}

// KVClient KV client, which provides basic Key-Value operations. Note that only write operations
// for a single shard are supported, because if the data to be written is distributed over multiple
// shards, atomic writing is not guaranteed.
type KVClient interface {
	// Set set key-value to the underlying storage engine. Use Future.GetError to check result.
	Set(ctx context.Context, key, value []byte) *Future
	// BatchSet set multiple key-value pairs to the underlying storage engine, these Keys must
	// belong to the same Shard. Use Future.GetError to check result.
	BatchSet(ctx context.Context, keys, values [][]byte) *Future
	// Delete delete the key from the underlying storage engine. Use Future.GetError to check
	// result.
	Delete(ctx context.Context, key []byte) *Future
	// BatchDelete delete the keys from the underlying storage engine, these Keys must belong
	// to the same ShardUse Future.GetError to check result.
	BatchDelete(ctx context.Context, keys [][]byte) *Future
	// RangeDelete delete keys in range [start, end) from the underlying storage engine, these
	// Keys must belong to the same ShardUse Future.GetError to check result.
	RangeDelete(ctx context.Context, start, end []byte) *Future
	// Get get the value of the key, use Future.GetKVGetResponse to get response
	Get(ctx context.Context, key []byte) *Future
	// BatchGet silimlar to Get, but perform with multi-keys
	BatchGet(ctx context.Context, keys [][]byte) *Future
	// Scan scan the keys in the range [start, end)
	Scan(ctx context.Context, start, end []byte, handler ScanHandler, options ...ScanOption) error
	// ScanCount returns the count of keys in the range [start, end)
	ScanCount(ctx context.Context, start, end []byte) (uint64, error)
	// ParallelScan similar to Scan, but perform scan in shards parallelly. Since scan is parallel,
	// there is no guarantee that the ScanHandler's processing of the Key is sequential.
	ParallelScan(ctx context.Context, start, end []byte, handler ScanHandler, options ...ScanOption) error
	// Close close the client
	Close() error
}

type kvClient struct {
	shardGroup uint64
	policy     rpcpb.ReplicaSelectPolicy
	cli        Client
	stopper    *stop.Stopper
}

func NewKVClient(cli Client, shardGroup uint64, policy rpcpb.ReplicaSelectPolicy) KVClient {
	return &kvClient{
		shardGroup: shardGroup,
		cli:        cli,
		policy:     policy,
		stopper:    stop.NewStopper("kv-client"),
	}
}

func (c *kvClient) Close() error {
	c.stopper.Stop()
	return nil
}

func (c *kvClient) Set(ctx context.Context, key, value []byte) *Future {
	return c.cli.Write(ctx,
		uint64(rpcpb.CmdKVSet),
		protoc.MustMarshal(&rpcpb.KVSetRequest{Key: key, Value: value}),
		WithReplicaSelectPolicy(c.policy),
		WithRouteKey(key),
		WithShardGroup(c.shardGroup))
}

func (c *kvClient) BatchSet(ctx context.Context, keys, values [][]byte) *Future {
	cmd := protoc.MustMarshal(&rpcpb.KVBatchSetRequest{
		Keys:   keys,
		Values: values,
	})
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	return c.cli.Write(ctx,
		uint64(rpcpb.CmdKVBatchSet),
		cmd,
		WithReplicaSelectPolicy(c.policy),
		WithRouteKey(keys[0]),
		WithKeysRange(keys[0], keysutil.NextKey(keys[len(keys)-1], nil)),
		WithShardGroup(c.shardGroup))
}

func (c *kvClient) Delete(ctx context.Context, key []byte) *Future {
	return c.cli.Write(ctx,
		uint64(rpcpb.CmdKVDelete),
		protoc.MustMarshal(&rpcpb.KVDeleteRequest{Key: key}),
		WithReplicaSelectPolicy(c.policy),
		WithRouteKey(key),
		WithShardGroup(c.shardGroup))
}

func (c *kvClient) BatchDelete(ctx context.Context, keys [][]byte) *Future {
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	cmd := protoc.MustMarshal(&rpcpb.KVBatchDeleteRequest{
		Keys: keys,
	})

	return c.cli.Write(ctx,
		uint64(rpcpb.CmdKVDelete),
		cmd,
		WithReplicaSelectPolicy(c.policy),
		WithRouteKey(keys[0]),
		WithKeysRange(keys[0], keysutil.NextKey(keys[len(keys)-1], nil)),
		WithShardGroup(c.shardGroup))
}

func (c *kvClient) RangeDelete(ctx context.Context, start, end []byte) *Future {
	return c.cli.Read(ctx,
		uint64(rpcpb.CmdKVRangeDelete),
		protoc.MustMarshal(&rpcpb.KVRangeDeleteRequest{Start: start, End: end}),
		WithReplicaSelectPolicy(c.policy),
		WithRouteKey(start),
		WithShardGroup(c.shardGroup))
}

func (c *kvClient) Get(ctx context.Context, key []byte) *Future {
	return c.cli.Read(ctx,
		uint64(rpcpb.CmdKVGet),
		protoc.MustMarshal(&rpcpb.KVGetRequest{Key: key}),
		WithReplicaSelectPolicy(c.policy),
		WithRouteKey(key),
		WithShardGroup(c.shardGroup))
}

func (c *kvClient) BatchGet(ctx context.Context, keys [][]byte) *Future {
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	tree := keysutil.NewKeyTree(32)
	tree.AddMany(keys)

	m := make(map[uint64][][]byte)
	var shards []uint64
	end := keysutil.NextKey(tree.Max(), nil)
	allocated := 0
	c.cli.Router().AscendRange(c.shardGroup,
		tree.Min(), end,
		c.policy,
		func(shard raftstore.Shard, replicaStore metapb.Store) bool {
			if allocated == tree.Len() {
				return false
			}
			var v [][]byte
			endKey := shard.End
			if len(endKey) == 0 {
				endKey = end
			}
			tree.AscendRange(shard.Start, endKey, func(key []byte) bool {
				v = append(v, key)
				allocated++
				return true
			})
			if len(v) > 0 {
				m[shard.ID] = v
				shards = append(shards, shard.ID)
			}
			return true
		})

	f := newFuture(ctx)
	err := c.stopper.RunTask(ctx, func(ctx context.Context) {
		bc := batchGetCtx{
			values: make(map[uint64][][]byte, len(m)),
		}
		for k := range m {
			shardID := k
			bc.wg.Add(1)
			err := c.stopper.RunTask(ctx, func(ctx context.Context) {
				defer bc.wg.Done()

				subKeys := m[shardID]
				var opts []Option
				opts = append(opts, WithShard(shardID))
				opts = append(opts, WithReplicaSelectPolicy(c.policy))
				if len(subKeys) > 1 {
					opts = append(opts, WithKeysRange(subKeys[0], keysutil.NextKey(subKeys[len(subKeys)-1], nil)))
				}

				for {
					sf := c.cli.Read(ctx,
						uint64(rpcpb.CmdKVBatchGet),
						protoc.MustMarshal(&rpcpb.KVBatchGetRequest{
							Keys: m[shardID],
						}),
						opts...)
					resp, err := sf.GetKVBatchGetResponse()
					sf.Close()
					if err == nil {
						bc.done(shardID, resp.Values)
						return
					}

					// retry
					if raftstore.IsShardUnavailableErr(err) ||
						err == raftstore.ErrKeysNotInShard {
						sf = c.BatchGet(ctx, subKeys)
						resp, err = sf.GetKVBatchGetResponse()
						sf.Close()
					}
					if err == nil {
						bc.done(shardID, resp.Values)
						return
					}
					bc.doneWithError(err)
				}
			})
			if err != nil {
				bc.doneWithError(err)
				bc.wg.Done()
			}
		}

		bc.wg.Wait()
		if bc.err != nil {
			f.done(nil, nil, bc.err)
			return
		}

		values := make([][]byte, 0, len(keys))
		for _, sid := range shards {
			values = append(values, bc.values[sid]...)
		}
		f.kvBatchGetDone(values)
	})
	if err != nil {
		f.done(nil, nil, err)
	}
	return f
}

func (c *kvClient) Scan(ctx context.Context,
	start, end []byte,
	handler ScanHandler,
	options ...ScanOption) error {
	for {
		req := rpcpb.KVScanRequest{
			Start: start,
			End:   end,
		}
		for _, opt := range options {
			opt(&req)
		}

		f := c.cli.Read(ctx, uint64(rpcpb.CmdKVScan), protoc.MustMarshal(&req),
			WithReplicaSelectPolicy(c.policy),
			WithRouteKey(start),
			WithShardGroup(c.shardGroup))
		resp, err := f.GetKVScanResponse()
		f.Close()
		if err != nil {
			return err
		}

		for i := uint64(0); i < resp.Count; i++ {
			var k, v []byte
			k = resp.Keys[i]
			if len(resp.Values) > 0 {
				v = resp.Values[i]
			}
			next, err := handler(k, v)
			if err != nil {
				return err
			}
			if !next {
				return nil
			}
		}

		if !resp.Completed {
			start = keysutil.NextKey(resp.Keys[resp.Count-1], nil)
		} else {
			start = resp.ShardEnd
		}

		// start >= end, completed
		if len(start) == 0 ||
			bytes.Compare(start, end) >= 0 {
			return nil
		}

	}
}

func (c *kvClient) ScanCount(ctx context.Context, start, end []byte) (uint64, error) {
	n := uint64(0)
	for {
		req := rpcpb.KVScanRequest{
			Start:     start,
			End:       end,
			OnlyCount: true,
		}

		f := c.cli.Read(ctx, uint64(rpcpb.CmdKVScan), protoc.MustMarshal(&req),
			WithReplicaSelectPolicy(c.policy),
			WithRouteKey(start),
			WithShardGroup(c.shardGroup))
		resp, err := f.GetKVScanResponse()
		f.Close()
		if err != nil {
			return 0, err
		}

		if !resp.Completed {
			panic("scan count can not completed is false")
		}

		n += resp.Count
		start = resp.ShardEnd

		// start >= end, completed
		if len(start) == 0 ||
			bytes.Compare(start, end) >= 0 {
			return n, nil
		}
	}
}

func (c *kvClient) ParallelScan(ctx context.Context,
	start, end []byte,
	handler ScanHandler,
	options ...ScanOption) error {
	var ranges []keyRange
	c.cli.Router().AscendRange(c.shardGroup,
		start, end,
		c.policy,
		func(shard raftstore.Shard, replicaStore metapb.Store) bool {
			if len(ranges) == 0 {
				ranges = append(ranges, keyRange{start: start, end: shard.End})
			} else {
				ranges = append(ranges, keyRange{start: ranges[len(ranges)-1].end, end: shard.End})
			}

			return true
		})
	if len(ranges) == 1 {
		return c.Scan(ctx, start, end, handler, options...)
	}

	var wg sync.WaitGroup
	var err error
	var lock sync.Mutex
	for idx := range ranges {
		wg.Add(1)
		i := idx
		e := c.stopper.RunTask(ctx, func(ctx context.Context) {
			defer wg.Done()

			if e := c.Scan(ctx, ranges[i].start, ranges[i].end, handler, options...); e != nil {
				lock.Lock()
				if err == nil {
					err = e
				}
				lock.Unlock()
			}
		})
		if e != nil {
			lock.Lock()
			if err == nil {
				err = e
			}
			lock.Unlock()
			wg.Done()
		}
	}
	wg.Wait()
	return err
}

type keyRange struct {
	start []byte
	end   []byte
}

type batchGetCtx struct {
	sync.RWMutex

	wg     sync.WaitGroup
	err    error
	values map[uint64][][]byte
}

func (bc *batchGetCtx) doneWithError(err error) {
	bc.Lock()
	defer bc.Unlock()
	bc.err = err
}

func (bc *batchGetCtx) done(shard uint64, values [][]byte) {
	bc.Lock()
	defer bc.Unlock()
	bc.values[shard] = values
}
