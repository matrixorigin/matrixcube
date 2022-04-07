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

	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
)

func TestWriteContextCanBeInitialized(t *testing.T) {
	defer leaktest.AfterTest(t)()

	shard := Shard{ID: 12345}
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	base := kv.NewBaseStorage(mem.NewStorage(), fs)
	defer base.Close()
	ctx := newWriteContext(base)
	assert.False(t, ctx.hasRequest())

	ctx.initialize(shard, 0)
	assert.Empty(t, ctx.responses)
	assert.Equal(t, shard, ctx.shard)
}

func newTestRPCRequests(n uint64) []rpcpb.Request {
	var requests []rpcpb.Request
	for i := uint64(0); i < n; i++ {
		requests = append(requests, rpcpb.Request{
			ID:         []byte(fmt.Sprintf("%d", n)),
			Key:        []byte(fmt.Sprintf("%d", n)),
			CustomType: uint64(rpcpb.CmdReserved) + n,
		})
	}
	return requests
}
