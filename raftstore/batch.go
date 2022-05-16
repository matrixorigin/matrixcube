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
	"fmt"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/errorpb"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/util/uuid"
	"go.uber.org/zap"
)

// The batch is responsible for aggregating the requests received by the cube
// over a period of time as a Raft-Log together with the Proposal, while also
// considering the request type, total batch size and other factors to determine
// which requests can be batches together.
type batch struct {
	logger       *zap.Logger
	requestBatch rpcpb.RequestBatch
	cb           func(rpcpb.ResponseBatch)
	tp           int // request type of this batch
	byteSize     int // bytes of this batch
}

func newBatch(logger *zap.Logger, requestBatch rpcpb.RequestBatch, cb func(rpcpb.ResponseBatch), tp int, byteSize int) batch {
	return batch{
		logger:       log.Adjust(logger),
		requestBatch: requestBatch,
		cb:           cb,
		tp:           tp,
		byteSize:     byteSize,
	}
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

func (c *batch) canBatches(req rpcpb.Request) bool {
	return (c.requestBatch.Requests[0].IgnoreEpochCheck && req.IgnoreEpochCheck) || // batch IgnoreEpochCheck requests
		(epochMatch(c.requestBatch.Requests[0].Epoch, req.Epoch) && // batch epoch match requests
			!c.requestBatch.Requests[0].IgnoreEpochCheck && !req.IgnoreEpochCheck)
}

func (c *batch) resp(resp rpcpb.ResponseBatch) {
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
					rsp := rpcpb.Response{
						Type:       req.Type,
						CustomType: req.CustomType,
						ID:         req.ID,
						PID:        req.PID,
					}
					resp.Responses = append(resp.Responses, rsp)
				}
			} else {
				for idx, req := range c.requestBatch.Requests {
					resp.Responses[idx].Type = req.Type
					resp.Responses[idx].CustomType = req.CustomType
					resp.Responses[idx].ID = req.ID
					resp.Responses[idx].PID = req.PID
				}
			}

			if !resp.Header.IsEmpty() {
				for idx := range resp.Responses {
					resp.Responses[idx].Error = resp.Header.Error
				}
			}

			if ce := c.logger.Check(zap.DebugLevel, "response to client"); ce != nil {
				ce.Write(log.ResponseBatchField("responses", resp))
			}
		}

		c.cb(resp)
	}
}

func (c *batch) respShardNotFound(shardID uint64) {
	err := &errorpb.ShardNotFound{
		ShardID: shardID,
	}
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

func respOtherError(err error, req rpcpb.Request, cb func(rpcpb.ResponseBatch)) {
	rsp := errorPbResp(uuid.NewV4().Bytes(), errorpb.Error{
		Message: err.Error(),
	})
	resp := rpcpb.Response{
		ID:  req.ID,
		PID: req.PID,
	}
	rsp.Responses = append(rsp.Responses, resp)
	cb(rsp)
}

func respStoreNotMatch(err error, req rpcpb.Request, cb func(rpcpb.ResponseBatch)) {
	rsp := errorPbResp(uuid.NewV4().Bytes(), errorpb.Error{
		Message:       err.Error(),
		StoreMismatch: storeMismatch,
	})
	resp := rpcpb.Response{
		ID:  req.ID,
		PID: req.PID,
	}
	rsp.Responses = append(rsp.Responses, resp)
	cb(rsp)
}

func respMissingLease(shardID, replicaID uint64, req rpcpb.Request, cb func(rpcpb.ResponseBatch)) {
	rsp := errorPbResp(uuid.NewV4().Bytes(), errorpb.Error{
		Message:      fmt.Sprintf("shard %d missing lease on replcia %d", shardID, replicaID),
		LeaseMissing: &errorpb.LeaseMissing{ShardID: shardID, ReplicaID: replicaID},
	})
	resp := rpcpb.Response{
		ID:  req.ID,
		PID: req.PID,
	}
	rsp.Responses = append(rsp.Responses, resp)
	cb(rsp)
}

func respLeaseMismatch(requestLease, replicaHeldLease *metapb.EpochLease, req rpcpb.Request, cb func(rpcpb.ResponseBatch)) {
	rsp := errorPbResp(uuid.NewV4().Bytes(), errorpb.Error{
		Message:       "request lease and replica held lease not match",
		LeaseMismatch: &errorpb.LeaseMismatch{RequestLease: requestLease, ReplicaHeldLease: replicaHeldLease},
	})
	resp := rpcpb.Response{
		ID:  req.ID,
		PID: req.PID,
	}
	rsp.Responses = append(rsp.Responses, resp)
	cb(rsp)
}

func respLeaseReadNotReady(req rpcpb.Request, cb func(rpcpb.ResponseBatch)) {
	rsp := errorPbResp(uuid.NewV4().Bytes(), errorpb.Error{
		Message:           "lease read not ready",
		LeaseReadNotReady: &errorpb.LeaseReadNotReady{},
	})
	resp := rpcpb.Response{
		ID:  req.ID,
		PID: req.PID,
	}
	rsp.Responses = append(rsp.Responses, resp)
	cb(rsp)
}

func respShardUnavailable(id uint64, req rpcpb.Request, cb func(responseBatch rpcpb.ResponseBatch)) {
	rsp := errorPbResp(uuid.NewV4().Bytes(), errorpb.Error{
		Message:          fmt.Sprintf("shard %d is unavailable", id),
		ShardUnavailable: &errorpb.ShardUnavailable{ShardID: id},
	})
	resp := rpcpb.Response{
		ID:  req.ID,
		PID: req.PID,
	}
	rsp.Responses = append(rsp.Responses, resp)
	cb(rsp)
}

func epochMatch(e1, e2 metapb.ShardEpoch) bool {
	return e1.ConfigVer == e2.ConfigVer && e1.Generation == e2.Generation
}
