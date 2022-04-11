// Copyright 2022 MatrixOrigin.
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

package executor

import (
	"testing"

	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/util/buf"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
)

func TestRegisterWriteHandler(t *testing.T) {
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kvStore := mem.NewStorage()
	defer kvStore.Close()

	cmdType := uint64(rpcpb.CmdReserved) + 1
	exec := NewKVExecutor(kvStore)
	handled := false
	exec.RegisterWrite(cmdType, func(shard metapb.Shard, cmd []byte, wb util.WriteBatch, buffer *buf.ByteBuf, kvStore storage.KVStorage) (KVWriteCommandResult, error) {
		handled = true
		return KVWriteCommandResult{}, nil
	})

	assert.NoError(t, exec.UpdateWriteBatch(storage.NewSimpleWriteContext(1, kvStore, storage.Batch{
		Index:    1,
		Requests: []storage.Request{{CmdType: cmdType}},
	})))
	assert.True(t, handled)
}

func TestRegisterReadHandler(t *testing.T) {
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	kvStore := mem.NewStorage()
	defer kvStore.Close()

	cmdType := uint64(rpcpb.CmdReserved) + 1
	exec := NewKVExecutor(kvStore)
	handled := false
	exec.RegisterRead(cmdType, func(shard metapb.Shard, cmd []byte, buffer *buf.ByteBuf, kvStore storage.KVStorage) (KVReadCommandResult, error) {
		handled = true
		return KVReadCommandResult{}, nil
	})

	_, err := exec.Read(storage.NewSimpleReadContext(1, storage.Request{CmdType: cmdType}))
	assert.NoError(t, err)
	assert.True(t, handled)
}
