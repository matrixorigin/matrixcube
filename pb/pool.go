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

package pb

import (
	"sync"

	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/pb/rpc"
)

var (
	requestsPool           sync.Pool
	responsePool           sync.Pool
	raftMessagePool        sync.Pool
	raftCMDRequestPool     sync.Pool
	raftCMDResponsePool    sync.Pool
	raftRequestHeaderPool  sync.Pool
	raftResponseHeaderPool sync.Pool
)

// AcquireRaftMessage returns a raft message from pool
func AcquireRaftMessage() *meta.RaftMessage {
	v := raftMessagePool.Get()
	if v == nil {
		return &meta.RaftMessage{}
	}
	return v.(*meta.RaftMessage)
}

// ReleaseRaftMessage returns a raft message to pool
func ReleaseRaftMessage(msg *meta.RaftMessage) {
	msg.Reset()
	raftMessagePool.Put(msg)
}

// AcquireRequestBatch returns a raft cmd request from pool
func AcquireRequestBatch() *rpc.RequestBatch {
	v := raftCMDRequestPool.Get()
	if v == nil {
		return &rpc.RequestBatch{}
	}
	return v.(*rpc.RequestBatch)
}

// ReleaseRequestBatch returns a raft cmd request to pool
func ReleaseRequestBatch(req *rpc.RequestBatch) {
	req.Reset()
	raftCMDRequestPool.Put(req)
}

// AcquireRequestBatchHeader returns a raft request header from pool
func AcquireRequestBatchHeader() *rpc.RequestBatchHeader {
	v := raftRequestHeaderPool.Get()
	if v == nil {
		return &rpc.RequestBatchHeader{}
	}
	return v.(*rpc.RequestBatchHeader)
}

// ReleaseRequestBatchHeader returns a raft request header to pool
func ReleaseRequestBatchHeader(header *rpc.RequestBatchHeader) {
	header.Reset()
	raftRequestHeaderPool.Put(header)
}

// AcquireRequest returns a raft request from pool
func AcquireRequest() *rpc.Request {
	v := requestsPool.Get()
	if v == nil {
		return &rpc.Request{}
	}
	return v.(*rpc.Request)
}

// ReleaseRequest returns a request to pool
func ReleaseRequest(req *rpc.Request) {
	if req != nil {
		req.Reset()
		requestsPool.Put(req)
	}
}

// AcquireResponse returns a response from pool
func AcquireResponse() *rpc.Response {
	v := responsePool.Get()
	if v == nil {
		return &rpc.Response{}
	}
	return v.(*rpc.Response)
}

// ReleaseResponse returns a response to pool
func ReleaseResponse(resp *rpc.Response) {
	resp.Reset()
	responsePool.Put(resp)
}

// AcquireResponseBatch returns a raft cmd response from pool
func AcquireResponseBatch() *rpc.ResponseBatch {
	v := raftCMDResponsePool.Get()
	if v == nil {
		return &rpc.ResponseBatch{}
	}
	return v.(*rpc.ResponseBatch)
}

// ReleaseResponseBatch returns a raft cmd response to pool
func ReleaseResponseBatch(resp *rpc.ResponseBatch) {
	if resp.Header != nil {
		ReleaseResponseBatchHeader(resp.Header)
	}

	resp.Reset()
	raftCMDResponsePool.Put(resp)
}

// AcquireResponseBatchHeader returns a raft response header from pool
func AcquireResponseBatchHeader() *rpc.ResponseBatchHeader {
	v := raftResponseHeaderPool.Get()
	if v == nil {
		return &rpc.ResponseBatchHeader{}
	}
	return v.(*rpc.ResponseBatchHeader)
}

// ReleaseResponseBatchHeader returns a raft response header to pool
func ReleaseResponseBatchHeader(header *rpc.ResponseBatchHeader) {
	header.Reset()
	raftResponseHeaderPool.Put(header)
}

// ReleaseRaftRequestAll release requests, header and self to pool
func ReleaseRaftRequestAll(req *rpc.RequestBatch) {
	for _, req := range req.Requests {
		ReleaseRequest(req)
	}

	if req.Header != nil {
		ReleaseRequestBatchHeader(req.Header)
	}

	ReleaseRequestBatch(req)
}

// ReleaseRaftResponseAll release responses, header and self to pool
func ReleaseRaftResponseAll(resp *rpc.ResponseBatch) {
	for _, rsp := range resp.Responses {
		ReleaseResponse(rsp)
	}

	if resp.Header != nil {
		ReleaseResponseBatchHeader(resp.Header)
	}

	ReleaseResponseBatch(resp)
}
