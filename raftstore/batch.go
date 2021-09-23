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
	"github.com/fagongzi/util/uuid"
	"github.com/matrixorigin/matrixcube/components/keys"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/errorpb"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"go.uber.org/zap"
)

func epochMatch(e1, e2 metapb.ResourceEpoch) bool {
	return e1.ConfVer == e2.ConfVer && e1.Version == e2.Version
}

type batch struct {
	req                     *rpc.RequestBatch
	cb                      func(*rpc.ResponseBatch)
	readIndexCommittedIndex uint64
	tp                      int
	size                    int
}

func (c *batch) notifyStaleCmd() {
	c.resp(errorStaleCMDResp(c.getUUID()))
}

func (c *batch) notifyShardRemoved() {
	if c.req != nil && c.req.Header != nil {
		c.respShardNotFound(c.req.Header.ShardID)
	}
}

func (c *batch) isFull(n, max int) bool {
	return max <= c.size+n ||
		(testMaxProposalRequestCount > 0 && len(c.req.Requests) >= testMaxProposalRequestCount)
}

func (c *batch) canAppend(epoch metapb.ResourceEpoch, req *rpc.Request) bool {
	return (c.req.Header.IgnoreEpochCheck && req.IgnoreEpochCheck) ||
		(epochMatch(c.req.Header.Epoch, epoch) &&
			!c.req.Header.IgnoreEpochCheck && !req.IgnoreEpochCheck)
}

func newCMD(req *rpc.RequestBatch, cb func(*rpc.ResponseBatch), tp int, size int) batch {
	c := batch{}
	c.req = req
	c.cb = cb
	c.tp = tp
	c.size = size
	return c
}

func resp(req *rpc.Request, resp *rpc.Response, cb func(*rpc.ResponseBatch)) {
	resp.ID = req.ID
	resp.SID = req.SID
	resp.PID = req.PID

	rsp := pb.AcquireResponseBatch()
	rsp.Responses = append(rsp.Responses, resp)
	cb(rsp)
}

func respWithRetry(req *rpc.Request, cb func(*rpc.ResponseBatch)) {
	resp := pb.AcquireResponse()
	resp.Type = rpc.CmdType_Invalid
	resp.ID = req.ID
	resp.SID = req.SID
	resp.PID = req.PID
	resp.Request = req

	rsp := pb.AcquireResponseBatch()
	rsp.Responses = append(rsp.Responses, resp)

	cb(rsp)
}

func respStoreNotMatch(err error, req *rpc.Request, cb func(*rpc.ResponseBatch)) {
	rsp := errorPbResp(&errorpb.Error{
		Message:       err.Error(),
		StoreNotMatch: storeNotMatch,
	}, uuid.NewV4().Bytes())

	resp := pb.AcquireResponse()
	resp.ID = req.ID
	resp.SID = req.SID
	resp.PID = req.PID
	resp.Request = req
	rsp.Responses = append(rsp.Responses, resp)
	cb(rsp)
}

func (c *batch) resp(resp *rpc.ResponseBatch) {
	if c.cb != nil {
		if len(c.req.Requests) > 0 {
			if len(c.req.Requests) != len(resp.Responses) {
				if resp.Header == nil {
					logger2.Fatal("requests and response not match",
						zap.Int("request-count", len(c.req.Requests)),
						zap.Int("response-count", len(resp.Responses)))
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
					rsp.Request = c.req.Requests[idx]
					rsp.Request.Key = keys.DecodeDataKey(rsp.Request.Key)
					rsp.Error = resp.Header.Error
				}
			}
		}

		c.cb(resp)
	} else {
		pb.ReleaseRaftResponseAll(resp)
	}
}

func (c *batch) respShardNotFound(shardID uint64) {
	err := new(errorpb.ShardNotFound)
	err.ShardID = shardID

	rsp := errorPbResp(&errorpb.Error{
		Message:       errShardNotFound.Error(),
		ShardNotFound: err,
	}, c.req.Header.ID)

	c.resp(rsp)
}

func (c *batch) respLargeRaftEntrySize(shardID uint64, size uint64) {
	err := &errorpb.RaftEntryTooLarge{
		ShardID:   shardID,
		EntrySize: size,
	}

	rsp := errorPbResp(&errorpb.Error{
		Message:           errLargeRaftEntrySize.Error(),
		RaftEntryTooLarge: err,
	}, c.getUUID())

	c.resp(rsp)
}

func (c *batch) respOtherError(err error) {
	rsp := errorOtherCMDResp(err)
	c.resp(rsp)
}

func (c *batch) respNotLeader(shardID uint64, leader metapb.Peer) {
	err := &errorpb.NotLeader{
		ShardID: shardID,
		Leader:  leader,
	}

	rsp := errorPbResp(&errorpb.Error{
		Message:   errNotLeader.Error(),
		NotLeader: err,
	}, c.getUUID())

	c.resp(rsp)
}

func (c *batch) getUUID() []byte {
	return c.req.Header.ID
}
