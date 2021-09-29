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

	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/util"
)

const (
	setCmd = 1
	getCmd = 2
)

var (
	// OK response with set
	OK = []byte("OK")
)

// simpleKVExecutor is a kv executor used for testing.
type simpleKVExecutor struct {
	kv storage.KVStorage
}

var _ storage.Executor = (*simpleKVExecutor)(nil)

// NewSimpleKVExecutor returns a simple kv executor that supports set/get
// commands.
func NewSimpleKVExecutor(kv storage.KVStorage) storage.Executor {
	return &simpleKVExecutor{kv: kv}
}

func (ce *simpleKVExecutor) UpdateWriteBatch(ctx storage.Context) error {
	writtenBytes := uint64(0)
	r := ctx.WriteBatch()
	wb := r.(util.WriteBatch)
	batches := ctx.Batches()
	for i := range batches {
		requests := batches[i].Requests
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

	writtenBytes += uint64(16)
	ctx.SetDiffBytes(int64(writtenBytes))
	ctx.SetWrittenBytes(writtenBytes)
	return nil
}

func (ce *simpleKVExecutor) ApplyWriteBatch(r storage.Resetable) error {
	wb := r.(util.WriteBatch)
	return ce.kv.Write(wb, false)
}

func (ce *simpleKVExecutor) Read(ctx storage.Context) error {
	readBytes := uint64(0)
	batches := ctx.Batches()
	for i := range batches {
		requests := batches[i].Requests
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
func NewWriteRequest(k, v []byte) storage.Request {
	return storage.Request{
		CmdType: setCmd,
		Key:     k,
		Cmd:     v,
	}
}

// NewReadRequest return write request
func NewReadRequest(k []byte) storage.Request {
	return storage.Request{
		CmdType: getCmd,
		Key:     k,
	}
}
