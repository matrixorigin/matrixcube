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
	"github.com/matrixorigin/matrixcube/pb/errorpb"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"go.uber.org/zap"
)

// The batch is responsible for aggregating the requests received by the cube
// over a period of time as a Raft-Log together with the Proposal, while also
// considering the request type, total batch size and other factors to determine
// which requests can be batches together.
type batch struct {
	logger       *zap.Logger
	requestBatch rpc.RequestBatch
	cb           func(rpc.ResponseBatch)
	tp           int // request type of this batch
	byteSize     int // bytes of this batch
}

func newBatch(logger *zap.Logger, requestBatch rpc.RequestBatch, cb func(rpc.ResponseBatch), tp int, byteSize int) batch {
	c := batch{}
	c.logger = logger
	c.requestBatch = requestBatch
	c.cb = cb
	c.tp = tp
	c.byteSize = byteSize
	return c
}

func (c *batch) notifyStaleCmd() {
	c.resp(errorStaleCMDResp(c.getRequestID()))
}

func (c *batch) notifyShardRemoved() {
	if !c.requestBatch.Header.IsEmpty() {
		c.respShardNotFound(c.requestBatch.Header.ShardID)
	}
}

func (c *batch) isFull(n, max int) bool {
	return max <= c.byteSize+n ||
		(testMaxProposalRequestCount > 0 && len(c.requestBatch.Requests) >= testMaxProposalRequestCount)
}

func (c *batch) canBatches(epoch metapb.ResourceEpoch, req rpc.Request) bool {
	return (c.requestBatch.Header.IgnoreEpochCheck && req.IgnoreEpochCheck) ||
		(epochMatch(c.requestBatch.Header.Epoch, epoch) &&
			!c.requestBatch.Header.IgnoreEpochCheck && !req.IgnoreEpochCheck)
}

func (c *batch) resp(resp rpc.ResponseBatch) {
	if c.cb != nil {
		if len(c.requestBatch.Requests) > 0 {
			if len(c.requestBatch.Requests) != len(resp.Responses) {
				if resp.Header.IsEmpty() {
					c.logger.Fatal("requests and response not match",
						zap.Int("request-count", len(c.requestBatch.Requests)),
						zap.Int("response-count", len(resp.Responses)))
				} else if len(resp.Responses) != 0 {
					c.logger.Fatal("BUG: responses len must be 0")
				}

				for _, req := range c.requestBatch.Requests {
					rsp := rpc.Response{}
					rsp.ID = req.ID
					rsp.SID = req.SID
					rsp.PID = req.PID
					resp.Responses = append(resp.Responses, rsp)
				}
			} else {
				for idx, req := range c.requestBatch.Requests {
					resp.Responses[idx].ID = req.ID
					resp.Responses[idx].SID = req.SID
					resp.Responses[idx].PID = req.PID
				}
			}

			if !resp.Header.IsEmpty() {
				for idx, rsp := range resp.Responses {
					rsp.Request = &c.requestBatch.Requests[idx]
					rsp.Request.Key = keys.DecodeDataKey(rsp.Request.Key)
					rsp.Error = resp.Header.Error
				}
			}
		}

		c.cb(resp)
	}
}

func (c *batch) respShardNotFound(shardID uint64) {
	err := new(errorpb.ShardNotFound)
	err.ShardID = shardID

	rsp := errorPbResp(c.getRequestID(), errorpb.Error{
		Message:       errShardNotFound.Error(),
		ShardNotFound: err,
	})

	c.resp(rsp)
}

func (c *batch) respLargeRaftEntrySize(shardID uint64, size uint64) {
	err := &errorpb.RaftEntryTooLarge{
		ShardID:   shardID,
		EntrySize: size,
	}

	rsp := errorPbResp(c.getRequestID(), errorpb.Error{
		Message:           errLargeRaftEntrySize.Error(),
		RaftEntryTooLarge: err,
	})

	c.resp(rsp)
}

func (c *batch) respOtherError(err error) {
	rsp := errorOtherCMDResp(err)
	c.resp(rsp)
}

func (c *batch) respNotLeader(shardID uint64, leader Replica) {
	err := &errorpb.NotLeader{
		ShardID: shardID,
		Leader:  leader,
	}

	rsp := errorPbResp(c.getRequestID(), errorpb.Error{
		Message:   errNotLeader.Error(),
		NotLeader: err,
	})

	c.resp(rsp)
}

func (c *batch) getRequestID() []byte {
	return c.requestBatch.Header.ID
}

func respStoreNotMatch(err error, req rpc.Request, cb func(rpc.ResponseBatch)) {
	rsp := errorPbResp(uuid.NewV4().Bytes(), errorpb.Error{
		Message:       err.Error(),
		StoreNotMatch: storeNotMatch,
	})

	resp := rpc.Response{}
	resp.ID = req.ID
	resp.SID = req.SID
	resp.PID = req.PID
	resp.Request = &req
	rsp.Responses = append(rsp.Responses, resp)
	cb(rsp)
}

func epochMatch(e1, e2 metapb.ResourceEpoch) bool {
	return e1.ConfVer == e2.ConfVer && e1.Version == e2.Version
}
