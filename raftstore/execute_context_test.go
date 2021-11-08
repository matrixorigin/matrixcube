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
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
)

func TestWriteContextCanBeInitialized(t *testing.T) {
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

	shard := Shard{ID: 12345}
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	base := kv.NewBaseStorage(mem.NewStorage(), fs)
	defer base.Close()
	ctx := newWriteContext(base)
	assert.False(t, ctx.hasRequest())
	for i, c := range cases {
		ctx.initialize(shard, 0, 0, c.batch)
		assert.True(t, ctx.hasRequest())
		assert.Equal(t, len(c.batch.Requests), len(ctx.batch.Requests), "index %d", i)
		for idx := range c.batch.Requests {
			assert.Equal(t, c.batch.Requests[idx].CustomType, ctx.batch.Requests[idx].CmdType)
			assert.Equal(t, c.batch.Requests[idx].Key, ctx.batch.Requests[idx].Key)
			assert.Equal(t, c.batch.Requests[idx].Cmd, ctx.batch.Requests[idx].Cmd)
		}
		assert.Empty(t, ctx.responses)
		assert.Equal(t, shard, ctx.shard)
	}
}

func newTestRPCRequests(n uint64) []rpc.Request {
	var requests []rpc.Request
	for i := uint64(0); i < n; i++ {
		requests = append(requests, rpc.Request{
			ID:         []byte(fmt.Sprintf("%d", n)),
			Key:        []byte(fmt.Sprintf("%d", n)),
			CustomType: n,
		})
	}
	return requests
}
