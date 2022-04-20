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
	raftstoreClient "github.com/matrixorigin/matrixcube/client"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/util/buf"
	keysutil "github.com/matrixorigin/matrixcube/util/keys"
	"github.com/matrixorigin/matrixcube/util/stop"
)

// TxnKVClient txn kv client, run kv option in transcation.
type TxnKVClient interface {
	// Exec exec many operations in a transaction. if commit is
	// not explicitly called, the transaction will be committed
	// automatically. If an error is returned, the transaction
	// is automatically rolled back.
	Exec(ctx context.Context, fn KVTxnFunc) error
}

// KVTxnFunc use KVTxn to execute all transaction operations
type KVTxnFunc func(KVTxn) error

// KVTxn kv txn
type KVTxn interface {
	// Set add kv set request into txn operations
	Set(key, value []byte) KVTxn
	// BatchSet add kv batch set request into txn operations
	BatchSet(keys, values [][]byte) KVTxn
	// Delete add kv delete request into txn operations
	Delete(key []byte) KVTxn
	// BatchDelete add batch delete request into txn operations
	BatchDelete(keys [][]byte) KVTxn
	// RangeDelete add range delete request into txn operations
	RangeDelete(start, end []byte) KVTxn
	// Exec execute buffered txn operations
	Exec(ctx context.Context) error
	// Rollback rollback current txn
	Rollback(ctx context.Context) error
	// Commit commit current txn.
	Commit(ctx context.Context) error

	Get(ctx context.Context, key []byte) ([]byte, error)
	// BatchGet silimlar to Get, but perform with multi-keys
	BatchGet(ctx context.Context, keys [][]byte) ([][]byte, error)
	// Scan scan the keys in the range [start, end)
	Scan(ctx context.Context, start, end []byte, handler raftstoreClient.ScanHandler, options ...raftstoreClient.ScanOption) error
	// ScanCount returns the count of keys in the range [start, end)
	ScanCount(ctx context.Context, start, end []byte) (uint64, error)
	// ParallelScan similar to Scan, but perform scan in shards parallelly. Since scan is parallel,
	// there is no guarantee that the ScanHandler's processing of the Key is sequential.
	ParallelScan(ctx context.Context, start, end []byte, handler raftstoreClient.ScanHandler, options ...raftstoreClient.ScanOption) error
}

type txnKVClient struct {
	txnClient  TxnClient
	router     raftstore.Router
	shardGroup uint64
	txnOptions []TxnOption
	stopper    *stop.Stopper
}

// NewTxnKVClient returns a txn kv client. Support kv operations
func NewTxnKVClient(txnClient TxnClient, shardGroup uint64, router raftstore.Router, txnOptions ...TxnOption) TxnKVClient {
	return &txnKVClient{
		txnClient:  txnClient,
		shardGroup: shardGroup,
		router:     router,
		txnOptions: txnOptions,
		stopper:    stop.NewStopper("txn-kv-client"),
	}
}

func (c *txnKVClient) Exec(ctx context.Context, fn KVTxnFunc) error {
	operator := c.txnClient.NewTxn(c.txnOptions...)

	txn := &kvTxn{
		operator:   operator,
		router:     c.router,
		shardGroup: c.shardGroup,
		stopper:    c.stopper,
	}
	if err := fn(txn); err != nil {
		if !txn.completed {
			return txn.Rollback(ctx)
		}
		return err
	}

	if !txn.completed {
		return txn.Commit(ctx)
	}
	return txn.err
}

type kvTxn struct {
	writeTxn   bool
	shardGroup uint64
	operator   TxnOperator
	router     raftstore.Router
	completed  bool
	err        error
	ops        []txnpb.TxnOperation
	stopper    *stop.Stopper

	reuse struct {
		set          rpcpb.KVSetRequest
		delete       rpcpb.KVDeleteRequest
		batchDelete  rpcpb.KVBatchDeleteRequest
		rangeDelete  rpcpb.KVRangeDeleteRequest
		batchSet     rpcpb.KVBatchSetRequest
		get          rpcpb.KVGetRequest
		getResp      rpcpb.KVGetResponse
		batchGet     rpcpb.KVBatchGetRequest
		batchGetResp rpcpb.KVBatchGetResponse
		scanReq      rpcpb.KVScanRequest
		scanResp     rpcpb.KVScanResponse
	}

	lazy struct {
		buffer *buf.ByteBuf
	}
}

func (txn *kvTxn) Set(key, value []byte) KVTxn {
	if txn.completed {
		return txn
	}

	txn.writeTxn = true
	txn.reuse.set.Reset()
	txn.reuse.set.Key = key
	txn.reuse.set.Value = value

	txn.ops = append(txn.ops, txnpb.TxnOperation{
		Impacted:     txnpb.KeySet{PointKeys: [][]byte{key}, Sorted: true},
		Op:           uint32(rpcpb.CmdKVSet),
		Payload:      protoc.MustMarshal(&txn.reuse.set),
		ImpactedType: txnpb.ImpactedType_WriteImpacted,
		ShardGroup:   txn.shardGroup,
	})
	return txn
}

func (txn *kvTxn) BatchSet(keys, values [][]byte) KVTxn {
	if txn.completed {
		return txn
	}

	txn.writeTxn = true
	txn.routeBatch(keys, values, func(shardID uint64, keys, values [][]byte) {
		txn.reuse.batchSet.Reset()
		txn.reuse.batchSet.Keys = keys
		txn.reuse.batchSet.Values = values
		txn.ops = append(txn.ops, txnpb.TxnOperation{
			Impacted:     txnpb.KeySet{PointKeys: keys, Sorted: true},
			Op:           uint32(rpcpb.CmdKVBatchSet),
			Payload:      protoc.MustMarshal(&txn.reuse.batchSet),
			ImpactedType: txnpb.ImpactedType_WriteImpacted,
			ShardGroup:   txn.shardGroup,
			ToShard:      shardID,
		})
	})
	return txn
}

func (txn *kvTxn) Delete(key []byte) KVTxn {
	if txn.completed {
		return txn
	}

	txn.writeTxn = true
	txn.reuse.delete.Reset()
	txn.reuse.delete.Key = key

	txn.ops = append(txn.ops, txnpb.TxnOperation{
		Impacted:     txnpb.KeySet{PointKeys: [][]byte{key}, Sorted: true},
		Op:           uint32(rpcpb.CmdKVDelete),
		Payload:      protoc.MustMarshal(&txn.reuse.delete),
		ImpactedType: txnpb.ImpactedType_WriteImpacted,
		ShardGroup:   txn.shardGroup,
	})
	return txn
}

func (txn *kvTxn) BatchDelete(keys [][]byte) KVTxn {
	if txn.completed {
		return txn
	}

	txn.writeTxn = true
	txn.routeBatch(keys, nil, func(shardID uint64, keys, values [][]byte) {
		txn.reuse.batchDelete.Reset()
		txn.reuse.batchDelete.Keys = keys

		txn.ops = append(txn.ops, txnpb.TxnOperation{
			Impacted:     txnpb.KeySet{PointKeys: keys},
			Op:           uint32(rpcpb.CmdKVBatchDelete),
			Payload:      protoc.MustMarshal(&txn.reuse.batchDelete),
			ImpactedType: txnpb.ImpactedType_WriteImpacted,
			ShardGroup:   txn.shardGroup,
		})
	})
	return txn
}

func (txn *kvTxn) RangeDelete(start, end []byte) KVTxn {
	if txn.completed {
		return txn
	}

	txn.writeTxn = true
	txn.routeRange(start, end, func(shardID uint64, start, end []byte) {
		txn.reuse.rangeDelete.Reset()
		txn.reuse.rangeDelete.Start = start
		txn.reuse.rangeDelete.End = end

		txn.ops = append(txn.ops, txnpb.TxnOperation{
			Impacted:     txnpb.KeySet{Ranges: []txnpb.KeyRange{{Start: start, End: end}}},
			Op:           uint32(rpcpb.CmdKVRangeDelete),
			Payload:      protoc.MustMarshal(&txn.reuse.rangeDelete),
			ImpactedType: txnpb.ImpactedType_WriteImpacted,
			ShardGroup:   txn.shardGroup,
			ToShard:      shardID,
		})
	})
	return txn
}

func (txn *kvTxn) Exec(ctx context.Context) error {
	if txn.completed {
		return txn.err
	}
	if len(txn.ops) == 0 {
		panic("empty txn operations")
	}

	if err := txn.operator.Write(ctx, txn.ops); err != nil {
		txn.err = txn.Rollback(ctx)
	}
	txn.ops = txn.ops[:0]
	return txn.err
}

func (txn *kvTxn) Rollback(ctx context.Context) error {
	if txn.completed {
		return txn.err
	}

	txn.completed = true
	txn.err = txn.operator.Rollback(ctx)
	return txn.err
}

func (txn *kvTxn) Commit(ctx context.Context) error {
	if txn.completed {
		return txn.err
	}

	txn.completed = true
	txn.err = txn.operator.Commit(ctx)
	return txn.err
}

func (txn *kvTxn) Get(ctx context.Context, key []byte) ([]byte, error) {
	if txn.completed {
		return nil, txn.err
	}

	txn.reuse.get.Key = key
	txn.ops = txn.ops[:0]
	txn.ops = append(txn.ops, txnpb.TxnOperation{
		Impacted:     txnpb.KeySet{PointKeys: [][]byte{key}},
		Op:           uint32(rpcpb.CmdKVGet),
		Payload:      protoc.MustMarshal(&txn.reuse.get),
		ImpactedType: txnpb.ImpactedType_ReadImpacted,
		ShardGroup:   txn.shardGroup,
	})
	resp, err := txn.operator.Read(ctx, txn.ops)
	if err != nil {
		return nil, txn.Rollback(ctx)
	}

	txn.reuse.getResp.Reset()
	protoc.MustUnmarshal(&txn.reuse.getResp, resp[0])
	return txn.reuse.getResp.Value, nil
}

func (txn *kvTxn) BatchGet(ctx context.Context, keys [][]byte) ([][]byte, error) {
	if txn.completed {
		return nil, txn.err
	}

	txn.ops = txn.ops[:0]
	txn.routeBatch(keys, nil, func(shardID uint64, keys, values [][]byte) {
		txn.reuse.batchGet.Reset()
		txn.reuse.batchGet.Keys = keys
		txn.ops = append(txn.ops, txnpb.TxnOperation{
			Impacted:     txnpb.KeySet{PointKeys: keys},
			Op:           uint32(rpcpb.CmdKVBatchGet),
			Payload:      protoc.MustMarshal(&txn.reuse.batchGet),
			ImpactedType: txnpb.ImpactedType_ReadImpacted,
			ShardGroup:   txn.shardGroup,
			ToShard:      shardID,
		})
	})

	resps, err := txn.operator.Read(ctx, txn.ops)
	if err != nil {
		return nil, txn.Rollback(ctx)
	}

	// requests not splitted
	if len(resps) == 1 {
		txn.reuse.batchGetResp.Reset()
		protoc.MustUnmarshal(&txn.reuse.batchGetResp, resps[0])
		return txn.reuse.batchGetResp.Values, nil
	}

	values := make([][]byte, len(keys))
	for _, resp := range resps {
		txn.reuse.batchGetResp.Reset()
		protoc.MustUnmarshal(&txn.reuse.batchGetResp, resp)
		for idx, value := range txn.reuse.batchGetResp.Values {
			values[txn.reuse.batchGetResp.Indexes[idx]] = value
		}
	}
	return values, nil
}

func (txn *kvTxn) Scan(ctx context.Context, start, end []byte,
	handler raftstoreClient.ScanHandler,
	options ...raftstoreClient.ScanOption) error {
	if txn.completed {
		return txn.err
	}

	txn.reuse.scanReq.Reset()
	for _, opt := range options {
		opt(&txn.reuse.scanReq)
	}

	scanStart := start
	for {
		txn.reuse.scanReq.Start = scanStart
		txn.reuse.scanReq.End = end
		txn.ops = txn.ops[:0]
		txn.ops = append(txn.ops, txnpb.TxnOperation{
			Impacted:     txnpb.KeySet{Ranges: []txnpb.KeyRange{{Start: scanStart, End: end}}},
			Op:           uint32(rpcpb.CmdKVScan),
			Payload:      protoc.MustMarshal(&txn.reuse.scanReq),
			ImpactedType: txnpb.ImpactedType_ReadImpacted,
			ShardGroup:   txn.shardGroup,
		})

		resp, err := txn.operator.Read(ctx, txn.ops)
		if err != nil {
			return txn.Rollback(ctx)
		}
		txn.reuse.scanResp.Reset()
		protoc.MustUnmarshal(&txn.reuse.scanResp, resp[0])

		for i := uint64(0); i < txn.reuse.scanResp.Count; i++ {
			var k, v []byte
			k = txn.reuse.scanResp.Keys[i]
			if len(txn.reuse.scanResp.Values) > 0 {
				v = txn.reuse.scanResp.Values[i]
			}
			next, err := handler(k, v)
			if err != nil {
				return err
			}
			if !next {
				return nil
			}
		}

		if !txn.reuse.scanResp.Completed {
			scanStart = keysutil.NextKey(txn.reuse.scanResp.Keys[txn.reuse.scanResp.Count-1], nil)
		} else {
			scanStart = txn.reuse.scanResp.ShardEnd
		}

		// scanStart >= end, completed
		if len(scanStart) == 0 ||
			bytes.Compare(scanStart, end) >= 0 {
			return nil
		}
	}
}

func (txn *kvTxn) ScanCount(ctx context.Context, start, end []byte) (uint64, error) {
	if txn.completed {
		return 0, txn.err
	}

	n := uint64(0)
	txn.reuse.scanReq.Reset()
	scanStart := start

	for {
		txn.reuse.scanReq.Start = scanStart
		txn.reuse.scanReq.End = end
		txn.reuse.scanReq.OnlyCount = true
		txn.ops = txn.ops[:0]
		txn.ops = append(txn.ops, txnpb.TxnOperation{
			Impacted:     txnpb.KeySet{Ranges: []txnpb.KeyRange{{Start: scanStart, End: end}}},
			Op:           uint32(rpcpb.CmdKVScan),
			Payload:      protoc.MustMarshal(&txn.reuse.scanReq),
			ImpactedType: txnpb.ImpactedType_ReadImpacted,
			ShardGroup:   txn.shardGroup,
		})

		resp, err := txn.operator.Read(ctx, txn.ops)
		if err != nil {
			return 0, txn.Rollback(ctx)
		}

		txn.reuse.scanResp.Reset()
		protoc.MustUnmarshal(&txn.reuse.scanResp, resp[0])

		if !txn.reuse.scanResp.Completed {
			panic("scan count can not completed is false")
		}

		n += txn.reuse.scanResp.Count
		scanStart = txn.reuse.scanResp.ShardEnd

		// scanStart >= end, completed
		if len(scanStart) == 0 ||
			bytes.Compare(scanStart, end) >= 0 {
			return n, nil
		}
	}
}

func (txn *kvTxn) ParallelScan(ctx context.Context,
	start, end []byte,
	handler raftstoreClient.ScanHandler,
	options ...raftstoreClient.ScanOption) error {
	if txn.completed {
		return txn.err
	}

	var ranges []txnpb.KeyRange
	txn.router.AscendRangeWithoutSelectReplica(txn.shardGroup,
		start, end,
		func(shard raftstore.Shard) bool {
			if len(ranges) == 0 {
				ranges = append(ranges, txnpb.KeyRange{Start: start, End: shard.End})
			} else {
				ranges = append(ranges, txnpb.KeyRange{Start: ranges[len(ranges)-1].End, End: shard.End})
			}

			return true
		})
	if len(ranges) == 1 {
		return txn.Scan(ctx, start, end, handler, options...)
	}

	var wg sync.WaitGroup
	var err error
	var lock sync.Mutex
	for idx := range ranges {
		wg.Add(1)
		i := idx
		e := txn.stopper.RunTask(ctx, func(ctx context.Context) {
			defer wg.Done()

			if e := txn.Scan(ctx, ranges[i].Start, ranges[i].End, handler, options...); e != nil {
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

func (txn *kvTxn) routeBatch(keys, values [][]byte, fn func(shardID uint64, keys, values [][]byte)) {
	buffer := txn.getBuffer()
	defer buffer.Clear()

	routeBatch(txn.router, txn.shardGroup, buffer, keys, values, fn)
}

func (txn *kvTxn) routeRange(start, end []byte, fn func(shardID uint64, start, end []byte)) {
	routeRange(txn.router, txn.shardGroup, start, end, fn)
}

func (txn *kvTxn) getBuffer() *buf.ByteBuf {
	if txn.lazy.buffer == nil {
		txn.lazy.buffer = buf.NewByteBuf(256)
	}
	return txn.lazy.buffer
}

type kvOperationRouter struct {
	router raftstore.Router
}

// NewKVOperationRouter returns kv operation router
func NewKVOperationRouter(router raftstore.Router) TxnOperationRouter {
	return &kvOperationRouter{router: router}
}

func (r *kvOperationRouter) Route(request *txnpb.TxnOperation) (bool, []txnpb.TxnOperation, error) {
	if request.ToShard > 0 {
		panic("re-route is unnecessary")
	}

	var routeKey []byte
	switch rpcpb.InternalCmd(request.Op) {
	case rpcpb.CmdKVSet:
		routeKey = request.Impacted.PointKeys[0]
	case rpcpb.CmdKVGet:
		routeKey = request.Impacted.PointKeys[0]
	case rpcpb.CmdKVDelete:
		routeKey = request.Impacted.PointKeys[0]
	case rpcpb.CmdKVScan:
		routeKey = request.Impacted.Ranges[0].Start
	case rpcpb.CmdKVBatchSet:
		var req rpcpb.KVBatchSetRequest
		protoc.MustUnmarshal(&req, request.Payload)

		var ops []txnpb.TxnOperation
		routeBatch(r.router, request.ShardGroup, nil, req.Keys, req.Values,
			func(shardID uint64, keys, values [][]byte) {
				req.Reset()
				req.Keys = keys
				req.Values = values
				ops = append(ops, txnpb.TxnOperation{
					Impacted:     txnpb.KeySet{PointKeys: keys},
					Op:           uint32(rpcpb.CmdKVBatchSet),
					Payload:      protoc.MustMarshal(&req),
					ImpactedType: txnpb.ImpactedType_WriteImpacted,
					ShardGroup:   request.ShardGroup,
					ToShard:      shardID,
				})

			})
		return true, ops, nil
	case rpcpb.CmdKVBatchGet:
		var req rpcpb.KVBatchGetRequest
		var ops []txnpb.TxnOperation
		routeBatch(r.router, request.ShardGroup, nil, request.Impacted.PointKeys, nil,
			func(shardID uint64, keys, values [][]byte) {
				req.Reset()
				req.Keys = keys
				ops = append(ops, txnpb.TxnOperation{
					Impacted:     txnpb.KeySet{PointKeys: keys},
					Op:           uint32(rpcpb.CmdKVBatchGet),
					Payload:      protoc.MustMarshal(&req),
					ImpactedType: txnpb.ImpactedType_ReadImpacted,
					ShardGroup:   request.ShardGroup,
					ToShard:      shardID,
				})

			})
		return true, ops, nil
	case rpcpb.CmdKVBatchDelete:
		var req rpcpb.KVBatchDeleteRequest
		var ops []txnpb.TxnOperation
		routeBatch(r.router, request.ShardGroup, nil, request.Impacted.PointKeys, nil,
			func(shardID uint64, keys, values [][]byte) {
				req.Reset()
				req.Keys = keys
				ops = append(ops, txnpb.TxnOperation{
					Impacted:     txnpb.KeySet{PointKeys: keys},
					Op:           uint32(rpcpb.CmdKVBatchDelete),
					Payload:      protoc.MustMarshal(&req),
					ImpactedType: txnpb.ImpactedType_WriteImpacted,
					ShardGroup:   request.ShardGroup,
					ToShard:      shardID,
				})

			})
		return true, ops, nil
	case rpcpb.CmdKVRangeDelete:
		var req rpcpb.KVRangeDeleteRequest
		var ops []txnpb.TxnOperation
		min, max := request.Impacted.GetKeyRange()
		routeRange(r.router, request.ShardGroup, min, max,
			func(shardID uint64, start, end []byte) {
				req.Reset()
				req.Start = start
				req.End = end
				ops = append(ops, txnpb.TxnOperation{
					Impacted:     txnpb.KeySet{Ranges: []txnpb.KeyRange{{Start: start, End: end}}},
					Op:           uint32(rpcpb.CmdKVRangeDelete),
					Payload:      protoc.MustMarshal(&req),
					ImpactedType: txnpb.ImpactedType_WriteImpacted,
					ShardGroup:   request.ShardGroup,
					ToShard:      shardID,
				})

			})
		return true, ops, nil
	default:
		panic("invalid kv operation")
	}

	request.ToShard = r.router.SelectShardIDByKey(request.ShardGroup, routeKey)
	if request.ToShard == 0 {
		panic("can not found shard by key")
	}
	return false, nil, nil
}

func routeBatch(router raftstore.Router,
	shardGroup uint64,
	buffer *buf.ByteBuf,
	keys, values [][]byte,
	fn func(shardID uint64, keys, values [][]byte)) {
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	if len(values) > 0 {
		sort.Slice(values, func(i, j int) bool {
			return bytes.Compare(keys[i], keys[j]) < 0
		})
	}

	n := len(keys)
	min := keys[0]
	max := keys[n-1]
	shard := router.SelectShardByKey(shardGroup, min)
	// all keys in the shard
	if shard.ContainsKey(min) &&
		shard.ContainsKey(max) {
		fn(shard.ID, keys, values)
		return
	}

	upperBound := keysutil.NextKey(max, buffer)
	router.AscendRangeWithoutSelectReplica(shardGroup,
		min, upperBound,
		func(shard raftstore.Shard) bool {
			n = len(keys)
			idx := sort.Search(n, func(i int) bool {
				return len(shard.End) > 0 && bytes.Compare(keys[i], shard.End) >= 0
			})
			if len(values) == 0 {
				fn(shard.ID, keys[:idx], nil)
			} else {
				fn(shard.ID, keys[:idx], values[:idx])
				values = values[idx:]
			}
			keys = keys[idx:]
			return true
		})
}

func routeRange(router raftstore.Router,
	shardGroup uint64,
	start, end []byte,
	fn func(shardID uint64, start, end []byte)) {
	shard := router.SelectShardByKey(shardGroup, start)
	if shard.ContainsKey(start) &&
		(len(shard.End) == 0 || bytes.Compare(end, shard.End) <= 0) {
		fn(shard.ID, start, end)
		return
	}

	lastStart := start
	router.AscendRangeWithoutSelectReplica(shardGroup,
		start, end,
		func(shard raftstore.Shard) bool {
			fn(shard.ID, lastStart, shard.MinEnd(end))
			lastStart = shard.End
			return true
		})
}
