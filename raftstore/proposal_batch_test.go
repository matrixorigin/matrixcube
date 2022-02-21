// Copyright 2021 MatrixOrigin.
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
	"testing"

	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

var (
	testMaxBatchSize uint64 = 1024 * 1024
)

func TestProposalBatchNeverBatchesAdminReq(t *testing.T) {
	defer leaktest.AfterTest(t)()
	b := newProposalBatch(nil, testMaxBatchSize, 10, Replica{})
	r1 := newReqCtx(rpcpb.Request{Type: rpcpb.Admin}, nil)
	r2 := newReqCtx(rpcpb.Request{Type: rpcpb.Admin}, nil)
	b.push(1, r1)
	b.push(1, r2)
	assert.Equal(t, 2, b.size())
}

func TestProposalBatchNeverBatchesDifferentTypeOfRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r1 := newReqCtx(rpcpb.Request{
		Type: rpcpb.Write,
	}, nil)
	r2 := newReqCtx(rpcpb.Request{
		Type: rpcpb.Read,
	}, nil)
	b := newProposalBatch(nil, testMaxBatchSize, 10, Replica{})
	b.push(1, r1)
	b.push(1, r2)
	assert.True(t, r1.req.Size()+r2.req.Size() < int(b.maxSize))
	assert.Equal(t, 2, b.size())
}

func TestProposalBatchLimitsBatchSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r1 := newReqCtx(rpcpb.Request{
		Type: rpcpb.Write,
	}, nil)
	r2 := newReqCtx(rpcpb.Request{
		Type: rpcpb.Write,
	}, nil)
	b1 := newProposalBatch(nil, testMaxBatchSize, 10, Replica{})
	b1.push(1, r1)
	b1.push(1, r2)
	assert.True(t, r1.req.Size()+r2.req.Size() < int(b1.maxSize))
	assert.Equal(t, 1, b1.size())
	assert.Equal(t, 2, len(b1.batches[0].requestBatch.Requests))

	b2 := newProposalBatch(nil, 1, 10, Replica{})
	b2.push(1, r1)
	b2.push(1, r2)
	assert.True(t, r1.req.Size()+r2.req.Size() > int(b2.maxSize))
	assert.Equal(t, 2, b2.size())
}

func TestProposalBatchNeverBatchesRequestsFromDifferentEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r1 := newReqCtx(rpcpb.Request{
		Type:  rpcpb.Write,
		Epoch: metapb.ShardEpoch{ConfVer: 1, Version: 1},
	}, nil)
	r2 := newReqCtx(rpcpb.Request{
		Type:  rpcpb.Write,
		Epoch: metapb.ShardEpoch{ConfVer: 2, Version: 2},
	}, nil)
	b := newProposalBatch(nil, testMaxBatchSize, 10, Replica{})
	b.push(1, r1)
	b.push(1, r2)
	assert.Equal(t, 2, b.size())

	r2.req.Epoch = metapb.ShardEpoch{ConfVer: 1, Version: 1}
	b2 := newProposalBatch(nil, testMaxBatchSize, 10, Replica{})
	b2.push(1, r1)
	b2.push(1, r2)
	assert.Equal(t, 1, b2.size())
}

func TestProposalBatchPop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r1 := newReqCtx(rpcpb.Request{
		Type: rpcpb.Write,
	}, nil)
	r2 := newReqCtx(rpcpb.Request{
		Type: rpcpb.Read,
	}, nil)
	b := newProposalBatch(nil, testMaxBatchSize, 10, Replica{})
	b.push(1, r1)
	b.push(1, r2)
	assert.Equal(t, 2, b.size())
	v1, ok := b.pop()
	assert.True(t, ok)
	assert.Equal(t, r1.req, v1.requestBatch.Requests[0])
	v2, ok := b.pop()
	assert.True(t, ok)
	assert.Equal(t, r2.req, v2.requestBatch.Requests[0])
	v3, ok := b.pop()
	assert.False(t, ok)
	assert.Equal(t, emptyCMD, v3)
}
