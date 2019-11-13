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
}

func (c *cmd) reset() {
	c.req = nil
	c.cb = nil
	c.term = 0
}

func newCMD(req *raftcmdpb.RaftCMDRequest, cb func(*raftcmdpb.RaftCMDResponse)) *cmd {
	c := acquireCmd()
	c.req = req
	c.cb = cb
	return c
}

func respStoreNotMatch(err error, req *raftcmdpb.Request, cb func(*raftcmdpb.RaftCMDResponse)) {
	rsp := errorPbResp(&errorpb.Error{
		Message:       err.Error(),
		StoreNotMatch: storeNotMatch,
	}, uuid.NewV4().Bytes(), 0)

	resp := pb.AcquireResponse()
	resp.ID = req.ID
	resp.SessionID = req.SessionID
	rsp.Responses = append(rsp.Responses, resp)
	cb(rsp)
}

func (c *cmd) resp(resp *raftcmdpb.RaftCMDResponse) {
	if c.cb != nil {
		logger.Debugf("shard %d response to client with %+v",
			c.req.Header.ShardID,
			resp)

		if len(c.req.Requests) > 0 {
			if len(c.req.Requests) != len(resp.Responses) {
				if resp.Header == nil {
					logger.Fatalf("BUG: requests and response count not match")
				} else if len(resp.Responses) != 0 {
					logger.Fatalf("BUG: responses len must be 0")
				}

				for _, req := range c.req.Requests {
					rsp := pb.AcquireResponse()
					rsp.ID = req.ID
					rsp.SessionID = req.SessionID

					resp.Responses = append(resp.Responses, rsp)
				}
			} else {
				for idx, req := range c.req.Requests {
					resp.Responses[idx].ID = req.ID
					resp.Responses[idx].SessionID = req.SessionID
				}
			}

			if resp.Header != nil {
				for _, rsp := range resp.Responses {
					rsp.Error = resp.Header.Error
				}
			}
		}

		c.cb(resp)
	} else {
		pb.ReleaseRaftResponseAll(resp)
	}

	c.release()
}

func (c *cmd) release() {
	pb.ReleaseRaftRequestAll(c.req)
	releaseCmd(c)
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
