// Copyright 2016 DeepFabric, Inc.
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
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/errorpb"
	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/fagongzi/util/uuid"
)

type cmd struct {
	req  *raftcmdpb.RaftCMDRequest
	cb   func(*raftcmdpb.RaftCMDResponse)
	term uint64
	tp   int
	size int
}

func (c *cmd) isFull(n, max int) bool {
	// -64 means exclude etcd raft entry other fields
	return max-64 <= c.size+n
}

func (c *cmd) reset() {
	c.req = nil
	c.cb = nil
	c.term = 0
	c.tp = -1
	c.size = 0
}

func newCMD(req *raftcmdpb.RaftCMDRequest, cb func(*raftcmdpb.RaftCMDResponse), tp int, size int) cmd {
	c := cmd{}
	c.req = req
	c.cb = cb
	c.tp = tp
	c.size = size
	return c
}

func resp(req *raftcmdpb.Request, resp *raftcmdpb.Response, cb func(*raftcmdpb.RaftCMDResponse)) {
	resp.ID = req.ID
	resp.SID = req.SID
	resp.PID = req.PID

	rsp := pb.AcquireRaftCMDResponse()
	rsp.Responses = append(rsp.Responses, resp)
	cb(rsp)
}

func respWithRetry(req *raftcmdpb.Request, cb func(*raftcmdpb.RaftCMDResponse)) {
	resp := pb.AcquireResponse()
	resp.Type = raftcmdpb.RaftError
	resp.ID = req.ID
	resp.SID = req.SID
	resp.PID = req.PID
	resp.OriginRequest = req

	rsp := pb.AcquireRaftCMDResponse()
	rsp.Responses = append(rsp.Responses, resp)

	cb(rsp)
}

func respStoreNotMatch(err error, req *raftcmdpb.Request, cb func(*raftcmdpb.RaftCMDResponse)) {
	rsp := errorPbResp(&errorpb.Error{
		Message:       err.Error(),
		StoreNotMatch: storeNotMatch,
	}, uuid.NewV4().Bytes(), 0)

	resp := pb.AcquireResponse()
	resp.ID = req.ID
	resp.SID = req.SID
	resp.PID = req.PID
	resp.OriginRequest = req
	rsp.Responses = append(rsp.Responses, resp)
	cb(rsp)
}

func (c *cmd) resp(resp *raftcmdpb.RaftCMDResponse) {
	if c.cb != nil {
		if len(c.req.Requests) > 0 {
			if len(c.req.Requests) != len(resp.Responses) {
				if resp.Header == nil {
					logger.Fatalf("BUG: requests and response count not match expect %d, but %d",
						len(c.req.Requests),
						len(resp.Responses))
				} else if len(resp.Responses) != 0 {
					logger.Fatalf("BUG: responses len must be 0")
				}

				for _, req := range c.req.Requests {
					rsp := pb.AcquireResponse()
					rsp.ID = req.ID
					rsp.SID = req.SID
					rsp.PID = req.PID
					resp.Responses = append(resp.Responses, rsp)
				}
			} else {
				for idx, req := range c.req.Requests {
					resp.Responses[idx].ID = req.ID
					resp.Responses[idx].SID = req.SID
					resp.Responses[idx].PID = req.PID
				}
			}

			if resp.Header != nil {
				for idx, rsp := range resp.Responses {
					rsp.OriginRequest = c.req.Requests[idx]
					rsp.OriginRequest.Key = DecodeDataKey(rsp.OriginRequest.Key)
					rsp.Error = resp.Header.Error
				}
			}
		}

		c.cb(resp)
	} else {
		pb.ReleaseRaftResponseAll(resp)
	}
}

func (c *cmd) respShardNotFound(shardID uint64) {
	err := new(errorpb.ShardNotFound)
	err.ShardID = shardID

	rsp := errorPbResp(&errorpb.Error{
		Message:       errShardNotFound.Error(),
		ShardNotFound: err,
	}, c.req.Header.ID, c.term)

	c.resp(rsp)
}

func (c *cmd) respLargeRaftEntrySize(shardID uint64, size uint64) {
	err := &errorpb.RaftEntryTooLarge{
		ShardID:   shardID,
		EntrySize: size,
	}

	rsp := errorPbResp(&errorpb.Error{
		Message:           errLargeRaftEntrySize.Error(),
		RaftEntryTooLarge: err,
	}, c.getUUID(), c.term)

	c.resp(rsp)
}

func (c *cmd) respOtherError(err error) {
	rsp := errorOtherCMDResp(err)
	c.resp(rsp)
}

func (c *cmd) respNotLeader(shardID uint64, leader metapb.Peer) {
	err := &errorpb.NotLeader{
		ShardID: shardID,
		Leader:  leader,
	}

	rsp := errorPbResp(&errorpb.Error{
		Message:   errNotLeader.Error(),
		NotLeader: err,
	}, c.getUUID(), c.term)

	c.resp(rsp)
}

func (c *cmd) getUUID() []byte {
	return c.req.Header.ID
}
