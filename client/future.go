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
	"context"
	"sync"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
)

// Future is used to obtain response data synchronously.
type Future struct {
	value            []byte
	req              rpcpb.Request
	txnResponse      txnpb.TxnBatchResponse
	batchGetResponse rpcpb.KVBatchGetResponse
	err              error
	ctx              context.Context
	c                chan struct{}
	cancel           func()

	mu struct {
		sync.Mutex
		closed bool
	}
}

func newFuture(ctx context.Context) *Future {
	f := acquireFuture()
	f.ctx = ctx
	return f
}

func (f *Future) reset() {
	f.req.Reset()
	f.value = nil
	f.txnResponse.Reset()
	f.batchGetResponse.Reset()
	f.err = nil
	f.ctx = nil
	f.cancel = nil
	select {
	case <-f.c:
	default:
	}
}

// Get get the response data synchronously, blocking until `context.Done` or the response is received.
// This method cannot be called more than once. After calling `Get`, `Close` must be called to close
// `Future`.
func (f *Future) Get() ([]byte, error) {
	select {
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	case <-f.c:
		return f.value, f.err
	}
}

// GetError is similar to Get, but no data is returned.
func (f *Future) GetError() error {
	select {
	case <-f.ctx.Done():
		return f.ctx.Err()
	case <-f.c:
		return f.err
	}
}

// GetTxn get the txn response data synchronously, blocking until `context.Done` or the response is received.
// This method cannot be called more than once. After calling `Get`, `Close` must be called to close
// `Future`.
func (f *Future) GetTxn() (txnpb.TxnBatchResponse, error) {
	select {
	case <-f.ctx.Done():
		return f.txnResponse, f.ctx.Err()
	case <-f.c:
		return f.txnResponse, f.err
	}
}

// GetKVGetResponse get the kv get response
func (f *Future) GetKVGetResponse() (rpcpb.KVGetResponse, error) {
	v, err := f.Get()
	if err != nil {
		return rpcpb.KVGetResponse{}, err
	}

	var resp rpcpb.KVGetResponse
	protoc.MustUnmarshal(&resp, v)
	return resp, nil
}

// GetKVBatchGetResponse get the kv batch get response
func (f *Future) GetKVBatchGetResponse() (rpcpb.KVBatchGetResponse, error) {
	v, err := f.Get()
	if err != nil {
		return rpcpb.KVBatchGetResponse{}, err
	}

	if len(v) == 0 {
		return f.batchGetResponse, nil
	}

	var resp rpcpb.KVBatchGetResponse
	protoc.MustUnmarshal(&resp, v)
	return resp, nil
}

// GetKVScanResponse get the kv scan response
func (f *Future) GetKVScanResponse() (rpcpb.KVScanResponse, error) {
	v, err := f.Get()
	if err != nil {
		return rpcpb.KVScanResponse{}, err
	}

	var resp rpcpb.KVScanResponse
	protoc.MustUnmarshal(&resp, v)
	return resp, nil
}

// Close close the future.
func (f *Future) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.cancel()
	f.mu.closed = true
	releaseFuture(f)
}

func (f *Future) canRetry() bool {
	select {
	case <-f.ctx.Done():
		return false
	default:
		return true
	}
}

func (f *Future) done(value []byte, txnRespopnse *txnpb.TxnBatchResponse, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.mu.closed {
		if txnRespopnse != nil {
			f.txnResponse = *txnRespopnse
		}
		f.value = value
		f.err = err
		select {
		case f.c <- struct{}{}:
		default:
			panic("BUG")
		}
	}
}

func (f *Future) kvBatchGetDone(values [][]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.mu.closed {
		f.batchGetResponse.Values = values
		select {
		case f.c <- struct{}{}:
		default:
			panic("BUG")
		}
	}
}
