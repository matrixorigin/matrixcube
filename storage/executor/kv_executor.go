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
	"fmt"

	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/util/buf"
)

// RegisterExecutor executor to support registration of custom read and write handlers
type RegisterExecutor interface {
	storage.Executor

	// RegisterWrite register write handler
	RegisterWrite(uint64, KVWriteCommandHandler)
	// RegisterRead register read handler
	RegisterRead(uint64, KVReadCommandHandler)
}

// KVWriteCommandResult kv write command handle result
type KVWriteCommandResult struct {
	// DiffBytes used to update storage.WriteContext.DiffBytes
	DiffBytes int64
	// WrittenBytes used to update storage.WriteContext.WrittenBytes
	WrittenBytes uint64
	// Response serialized response
	Response []byte
}

// KVReadCommandResult kv read command handle result
type KVReadCommandResult struct {
	// ReadBytes used to update storage.ReadContext.ReadBytes
	ReadBytes uint64
	// Response serialized response
	Response []byte
}

// KVWriteCommandHandler kv write command handler
type KVWriteCommandHandler func(shard metapb.Shard, cmd []byte, wb util.WriteBatch, buffer *buf.ByteBuf, kvStore storage.KVStorage) (KVWriteCommandResult, error)

// KVReadCommandHandler kv read command handler
type KVReadCommandHandler func(shard metapb.Shard, cmd []byte, buffer *buf.ByteBuf, kvStore storage.KVStorage) (KVReadCommandResult, error)

// kvExecutor is a kv executor.
type kvExecutor struct {
	kv storage.KVStorage

	writeHandlers map[uint64]KVWriteCommandHandler
	readHandlers  map[uint64]KVReadCommandHandler
}

var _ storage.Executor = (*kvExecutor)(nil)

// NewKVExecutor returns a kv executor.
func NewKVExecutor(kv storage.KVStorage) RegisterExecutor {
	ke := &kvExecutor{
		kv:            kv,
		writeHandlers: map[uint64]KVWriteCommandHandler{},
		readHandlers:  map[uint64]KVReadCommandHandler{},
	}

	ke.writeHandlers[uint64(rpcpb.CmdKVSet)] = handleSet
	ke.writeHandlers[uint64(rpcpb.CmdKVBatchSet)] = handleBatchSet
	ke.writeHandlers[uint64(rpcpb.CmdKVDelete)] = handleDelete
	ke.writeHandlers[uint64(rpcpb.CmdKVBatchDelete)] = handleBatchDelete
	ke.writeHandlers[uint64(rpcpb.CmdKVRangeDelete)] = handleRangeDelete
	ke.writeHandlers[uint64(rpcpb.CmdKVBatchMixedWrite)] = handleBatchMixedWrite

	ke.readHandlers[uint64(rpcpb.CmdKVGet)] = handleGet
	ke.readHandlers[uint64(rpcpb.CmdKVBatchGet)] = handleBatchGet
	ke.readHandlers[uint64(rpcpb.CmdKVScan)] = handleScan
	return ke
}

func (ke *kvExecutor) RegisterWrite(cmdType uint64, handler KVWriteCommandHandler) {
	if _, ok := ke.writeHandlers[cmdType]; ok {
		panic(fmt.Sprintf("%d already register", cmdType))
	}
	ke.writeHandlers[cmdType] = handler
}

func (ke *kvExecutor) RegisterRead(cmdType uint64, handler KVReadCommandHandler) {
	if _, ok := ke.readHandlers[cmdType]; ok {
		panic(fmt.Sprintf("%d already register", cmdType))
	}
	ke.readHandlers[cmdType] = handler
}

func (ke *kvExecutor) UpdateWriteBatch(ctx storage.WriteContext) error {
	changedBytes := int64(0)
	writtenBytes := uint64(0)
	r := ctx.WriteBatch()
	wb := r.(util.WriteBatch)
	batch := ctx.Batch()
	requests := batch.Requests
	buffer := ctx.(storage.InternalContext).ByteBuf()

	for idx := range requests {
		handlerFunc, ok := ke.writeHandlers[requests[idx].CmdType]
		if !ok {
			panic(fmt.Errorf("not support write cmd %d", requests[idx].CmdType))
		}

		result, err := handlerFunc(ctx.Shard(), requests[idx].Cmd, wb, buffer, ke.kv)
		if err != nil {
			return err
		}
		changedBytes += result.DiffBytes
		writtenBytes += result.WrittenBytes
		ctx.AppendResponse(result.Response)
	}

	ctx.SetDiffBytes(changedBytes)
	ctx.SetWrittenBytes(writtenBytes)
	return nil
}

func (ke *kvExecutor) ApplyWriteBatch(r storage.Resetable) error {
	wb := r.(util.WriteBatch)
	return ke.kv.Write(wb, false)
}

func (ke *kvExecutor) Read(ctx storage.ReadContext) ([]byte, error) {
	request := ctx.Request()
	buffer := ctx.(storage.InternalContext).ByteBuf()

	handlerFunc, ok := ke.readHandlers[request.CmdType]
	if !ok {
		panic(fmt.Errorf("not support read cmd %d", request.CmdType))
	}

	result, err := handlerFunc(ctx.Shard(), request.Cmd, buffer, ke.kv)
	if err != nil {
		return nil, err
	}

	ctx.SetReadBytes(result.ReadBytes)
	return result.Response, nil
}
