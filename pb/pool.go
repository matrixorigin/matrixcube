package pb

import (
	"sync"

	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/pb/raftpb"
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
func AcquireRaftMessage() *raftpb.RaftMessage {
	v := raftMessagePool.Get()
	if v == nil {
		return &raftpb.RaftMessage{}
	}
	return v.(*raftpb.RaftMessage)
}

// ReleaseRaftMessage returns a raft message to pool
func ReleaseRaftMessage(msg *raftpb.RaftMessage) {
	msg.Reset()
	raftMessagePool.Put(msg)
}

// AcquireRaftCMDRequest returns a raft cmd request from pool
func AcquireRaftCMDRequest() *raftcmdpb.RaftCMDRequest {
	v := raftCMDRequestPool.Get()
	if v == nil {
		return &raftcmdpb.RaftCMDRequest{}
	}
	return v.(*raftcmdpb.RaftCMDRequest)
}

// ReleaseRaftCMDRequest returns a raft cmd request to pool
func ReleaseRaftCMDRequest(req *raftcmdpb.RaftCMDRequest) {
	req.Reset()
	raftCMDRequestPool.Put(req)
}

// AcquireRaftRequestHeader returns a raft request header from pool
func AcquireRaftRequestHeader() *raftcmdpb.RaftRequestHeader {
	v := raftRequestHeaderPool.Get()
	if v == nil {
		return &raftcmdpb.RaftRequestHeader{}
	}
	return v.(*raftcmdpb.RaftRequestHeader)
}

// ReleaseRaftRequestHeader returns a raft request header to pool
func ReleaseRaftRequestHeader(header *raftcmdpb.RaftRequestHeader) {
	header.Reset()
	raftRequestHeaderPool.Put(header)
}

// AcquireRequest returns a raft request from pool
func AcquireRequest() *raftcmdpb.Request {
	v := requestsPool.Get()
	if v == nil {
		return &raftcmdpb.Request{}
	}
	return v.(*raftcmdpb.Request)
}

// ReleaseRequest returns a request to pool
func ReleaseRequest(req *raftcmdpb.Request) {
	req.Reset()
	requestsPool.Put(req)
}

// AcquireResponse returns a response from pool
func AcquireResponse() *raftcmdpb.Response {
	v := responsePool.Get()
	if v == nil {
		return &raftcmdpb.Response{}
	}
	return v.(*raftcmdpb.Response)
}

// ReleaseResponse returns a response to pool
func ReleaseResponse(resp *raftcmdpb.Response) {
	resp.Reset()
	responsePool.Put(resp)
}

// AcquireRaftCMDResponse returns a raft cmd response from pool
func AcquireRaftCMDResponse() *raftcmdpb.RaftCMDResponse {
	v := raftCMDResponsePool.Get()
	if v == nil {
		return &raftcmdpb.RaftCMDResponse{}
	}
	return v.(*raftcmdpb.RaftCMDResponse)
}

// ReleaseRaftCMDResponse returns a raft cmd response to pool
func ReleaseRaftCMDResponse(resp *raftcmdpb.RaftCMDResponse) {
	if resp.Header != nil {
		ReleaseRaftResponseHeader(resp.Header)
	}

	resp.Reset()
	raftCMDResponsePool.Put(resp)
}

// AcquireRaftResponseHeader returns a raft response header from pool
func AcquireRaftResponseHeader() *raftcmdpb.RaftResponseHeader {
	v := raftResponseHeaderPool.Get()
	if v == nil {
		return &raftcmdpb.RaftResponseHeader{}
	}
	return v.(*raftcmdpb.RaftResponseHeader)
}

// ReleaseRaftResponseHeader returns a raft response header to pool
func ReleaseRaftResponseHeader(header *raftcmdpb.RaftResponseHeader) {
	header.Reset()
	raftResponseHeaderPool.Put(header)
}

// ReleaseRaftRequestAll release requests, header and self to pool
func ReleaseRaftRequestAll(req *raftcmdpb.RaftCMDRequest) {
	for _, req := range req.Requests {
		ReleaseRequest(req)
	}

	if req.Header != nil {
		ReleaseRaftRequestHeader(req.Header)
	}

	ReleaseRaftCMDRequest(req)
}

// ReleaseRaftResponseAll release responses, header and self to pool
func ReleaseRaftResponseAll(resp *raftcmdpb.RaftCMDResponse) {
	for _, rsp := range resp.Responses {
		ReleaseResponse(rsp)
	}

	if resp.Header != nil {
		ReleaseRaftResponseHeader(resp.Header)
	}

	ReleaseRaftCMDResponse(resp)
}
