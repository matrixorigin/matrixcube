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

package simple

import (
	"fmt"

	"github.com/matrixorigin/matrixcube/components/keys"
	"github.com/matrixorigin/matrixcube/storage"
)

const (
	setCmd = 1
	getCmd = 2
)

var (
	// OK response with set
	OK = []byte("OK")
)

type simpleKVCommandExecutor struct {
	kv storage.KVStorage
}

// NewSimpleKVCommandExecutor returns a simple kv command executor to support set/get command
func NewSimpleKVCommandExecutor(kv storage.KVStorage) storage.CommandExecutor {
	return &simpleKVCommandExecutor{kv: kv}
}

func (ce *simpleKVCommandExecutor) ExecuteWrite(ctx storage.Context) error {
	writtenBytes := uint64(0)
	wb := ce.kv.NewWriteBatch()
	lastLogIndex := uint64(0)
	for i := range ctx.Requests() {
		lastLogIndex = ctx.Requests()[i].Index
		requests := ctx.Requests()[i].Requests
		for j := range requests {
			switch requests[j].CmdType {
			case setCmd:
				wb.Set(requests[j].Key, requests[j].Cmd)
				writtenBytes += uint64(len(requests[j].Key) + len(requests[j].Cmd))
				ctx.AppendResponse(OK)
			default:
				panic(fmt.Errorf("invalid write cmd %d", requests[j].CmdType))
			}
		}
	}

	ctx.ByteBuf().MarkWrite()
	ctx.ByteBuf().WriteUInt64(lastLogIndex)
	wb.Set(keys.GetDataStorageAppliedIndexKey(ctx.Shard().ID), ctx.ByteBuf().WrittenDataAfterMark().Data())
	writtenBytes += uint64(16)

	err := ce.kv.Write(wb, false)
	if err != nil {
		return err
	}

	ctx.SetDiffBytes(int64(writtenBytes))
	ctx.SetWrittenBytes(writtenBytes)
	return nil
}

func (ce *simpleKVCommandExecutor) ExecuteRead(ctx storage.Context) error {
	readBytes := uint64(0)
	for i := range ctx.Requests() {
		requests := ctx.Requests()[i].Requests
		for j := range requests {
			switch requests[j].CmdType {
			case getCmd:
				v, err := ce.kv.Get(requests[j].Key)
				if err != nil {
					return err
				}

				readBytes += uint64(len(v))
				ctx.AppendResponse(v)
			default:
				panic(fmt.Errorf("invalid read cmd %d", requests[j].CmdType))
			}
		}
	}

	ctx.SetReadBytes(readBytes)
	return nil
}

// NewWriteRequest return write request
func NewWriteRequest(k, v []byte) storage.CustomCmd {
	return storage.CustomCmd{
		CmdType: setCmd,
		Key:     k,
		Cmd:     v,
	}
}

// NewReadRequest return write request
func NewReadRequest(k []byte) storage.CustomCmd {
	return storage.CustomCmd{
		CmdType: getCmd,
		Key:     k,
	}
}
