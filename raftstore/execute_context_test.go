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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
)

func TestExecuteContextCanBeAppendedAndReset(t *testing.T) {
	cases := []struct {
		batch rpc.RequestBatch
	}{
		{
			batch: rpc.RequestBatch{Requests: newTestRPCRequests(1)},
		},
		{
			batch: rpc.RequestBatch{Requests: newTestRPCRequests(2)},
		},
		{
			batch: rpc.RequestBatch{Requests: newTestRPCRequests(3)},
		},
	}

	fs := vfs.GetTestFS()
	ctx := newExecuteContext(mem.NewStorage(fs))
	assert.False(t, ctx.hasRequest())
	for i, c := range cases {
		ctx.appendRequestBatch(c.batch)
		assert.True(t, ctx.hasRequest())
		assert.Equal(t, len(ctx.batches[i].requestBatch.Requests), len(c.batch.Requests), "index %d", i)
		assert.Equal(t, ctx.batches[i].requestBatch, c.batch, "index %d", i)

		for j, req := range ctx.batches[i].requestBatch.Requests {
			assert.Equal(t, req, c.batch.Requests[j], "index %d, %d request", i, j)
		}
	}

	ctx.reset(Shard{})
	assert.Empty(t, ctx.requests)
	assert.Empty(t, ctx.batches)
	assert.Empty(t, ctx.responses)
	assert.Equal(t, Shard{}, ctx.shard)
}

func newTestRPCRequests(n uint64) []rpc.Request {
	var requests []rpc.Request
	for i := uint64(0); i < n; i++ {
		requests = append(requests, rpc.Request{
			ID:         []byte(fmt.Sprintf("%d", n)),
			Key:        []byte(fmt.Sprintf("%d", n)),
			CustemType: n,
		})
	}
	return requests
}
