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

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/stretchr/testify/assert"
)

var (
	testMaxBatchSize uint64 = 1024 * 1024
)

func TestProposalBatchNeverBatchesAdminReq(t *testing.T) {
	b := newBatch(nil, testMaxBatchSize, 10, Replica{})
	r1 := reqCtx{
		admin: &rpc.AdminRequest{},
	}
	r2 := reqCtx{
		admin: &rpc.AdminRequest{},
	}
	b.push(1, metapb.ResourceEpoch{}, r1)
	b.push(1, metapb.ResourceEpoch{}, r2)
	assert.Equal(t, 2, b.size())
}

func TestProposalBatchNeverBatchesDifferentTypeOfRequest(t *testing.T) {
	r1 := reqCtx{
		req: &rpc.Request{
			Type: rpc.CmdType_Write,
		},
	}
	r2 := reqCtx{
		req: &rpc.Request{
			Type: rpc.CmdType_Read,
		},
	}
	b := newBatch(nil, testMaxBatchSize, 10, Replica{})
	b.push(1, metapb.ResourceEpoch{}, r1)
	b.push(1, metapb.ResourceEpoch{}, r2)
	assert.True(t, r1.req.Size()+r2.req.Size() < int(b.maxSize))
	assert.Equal(t, 2, b.size())
}

func TestProposalBatchLimitsBatchSize(t *testing.T) {
	r1 := reqCtx{
		req: &rpc.Request{
			Type: rpc.CmdType_Write,
		},
	}
	r2 := reqCtx{
		req: &rpc.Request{
			Type: rpc.CmdType_Write,
		},
	}
	b1 := newBatch(nil, testMaxBatchSize, 10, Replica{})
	b1.push(1, metapb.ResourceEpoch{}, r1)
	b1.push(1, metapb.ResourceEpoch{}, r2)
	assert.True(t, r1.req.Size()+r2.req.Size() < int(b1.maxSize))
	assert.Equal(t, 1, b1.size())
	assert.Equal(t, 2, len(b1.cmds[0].req.Requests))

	b2 := newBatch(nil, 1, 10, Replica{})
	b2.push(1, metapb.ResourceEpoch{}, r1)
	b2.push(1, metapb.ResourceEpoch{}, r2)
	assert.True(t, r1.req.Size()+r2.req.Size() > int(b2.maxSize))
	assert.Equal(t, 2, b2.size())
}

func TestProposalBatchNeverBatchesRequestsFromDifferentEpoch(t *testing.T) {
	epoch1 := metapb.ResourceEpoch{ConfVer: 1, Version: 1}
	epoch2 := metapb.ResourceEpoch{ConfVer: 2, Version: 2}
	r1 := reqCtx{
		req: &rpc.Request{
			Type: rpc.CmdType_Write,
		},
	}
	r2 := reqCtx{
		req: &rpc.Request{
			Type: rpc.CmdType_Write,
		},
	}
	b := newBatch(nil, testMaxBatchSize, 10, Replica{})
	b.push(1, epoch1, r1)
	b.push(1, epoch2, r2)
	assert.Equal(t, 2, b.size())

	b2 := newBatch(nil, testMaxBatchSize, 10, Replica{})
	b2.push(1, epoch1, r1)
	b2.push(1, epoch1, r2)
	assert.Equal(t, 1, b2.size())
}

func TestProposalBatchPop(t *testing.T) {
	r1 := reqCtx{
		req: &rpc.Request{
			Type: rpc.CmdType_Write,
		},
	}
	r2 := reqCtx{
		req: &rpc.Request{
			Type: rpc.CmdType_Read,
		},
	}
	b := newBatch(nil, testMaxBatchSize, 10, Replica{})
	b.push(1, metapb.ResourceEpoch{}, r1)
	b.push(1, metapb.ResourceEpoch{}, r2)
	assert.Equal(t, 2, b.size())
	v1, ok := b.pop()
	assert.True(t, ok)
	assert.Equal(t, r1.req, v1.req.Requests[0])
	v2, ok := b.pop()
	assert.True(t, ok)
	assert.Equal(t, r2.req, v2.req.Requests[0])
	v3, ok := b.pop()
	assert.False(t, ok)
	assert.Equal(t, emptyCMD, v3)
}
