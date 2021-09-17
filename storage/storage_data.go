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

package storage

import (
	"github.com/fagongzi/goetty/buf"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
)

// BaseDataStorage basic data storage interface
type BaseDataStorage interface {
	StatisticalStorage
	CloseableStorage

	// SplitCheck Find a key from [start, end), so that the sum of bytes of the value of [start, key) <=size,
	// returns the current bytes in [start,end), and the founded key
	SplitCheck(start []byte, end []byte, size uint64) (currentSize uint64, currentKeys uint64, splitKeys [][]byte, err error)
	// CreateSnapshot create a snapshot file under the giving path
	CreateSnapshot(path string, start, end []byte) error
	// ApplySnapshot apply a snapshort file from giving path
	ApplySnapshot(path string) error
}

// KVBaseDataStorage kv based BaseDataStorage
type KVBaseDataStorage interface {
	BaseDataStorage
	KVStorage
}

// CommandExecutor used to execute read/write command
type CommandExecutor interface {
	// ExecuteWrite execute write requests.
	// The `Context` holds all the requests involved in this execution and packs as many requests from multiple Raft-Logs
	// together as possible, the implementation needs to ensure that the data and AppliedIndex of each Raft-Log must be
	// written atomically.
	// The implementation should call `SetWrittenBytes`, `SetReadBytes`, `SetDiffBytes` of `Context` to set the changes
	// to the statistics involved in this execution before returning.
	ExecuteWrite(Context) error
	// ExecuteRead execute read requests. The `Context` holds all the requests involved in this execution.
	// The implementation should call `SetReadBytes`  of `Context` to set the changes to the statistics involved in this
	// execution before returning.
	ExecuteRead(Context) error
}

// DataStorage responsible for maintaining the data storage of a set of shards for the application.
type DataStorage interface {
	BaseDataStorage

	// GetCommandExecutor returns `CommandExecutor` to execute custom read/write commands
	GetCommandExecutor() CommandExecutor
	// GetInitialStates return the initial state of all shards at local, including the `AppliedIndex` and the corresponding
	// `Shard Metadata`.
	// The metadata of the corresponding version<=AppliedIndex.
	// For example, shard 1 has multiple versions of metadata in storage: v1, v10, v20.
	//   `GetInitialState(1)` => 1, v1
	//   `GetInitialState(1)` => 5, v1
	//   `GetInitialState(1)` => 10, v10
	//   `GetInitialState(1)` => 15, v10
	//   `GetInitialState(1)` => 20, v20
	//   `GetInitialState(1)` => 21, v20
	GetInitialStates() ([]ShardMetadata, error)
	// GetPersistentLogIndex return the last persistent applied log index. The storage save the last applied log index in
	// `CommandExecutor`. When restarting, Cube will use the last persistent applied log index to call `GetShardMetadata` to
	// load the metadata of the shard, and finally complete the startup of the shard.
	GetPersistentLogIndex(shardID uint64) (uint64, error)
	// SaveShardMetadata save shard metadata, whether to fsync to disk is determined by the storage engine itself.
	// The metadata of the shard contains key information such as the range/peers of the shard, fsync to disk is not required
	// for this call, but the storage needs to ensure that when the data is persisted to disk, the data and metadata of the
	// specific shard are consistent. In storage, each shard will have many versions of metadata corresponding to logIndex.
	SaveShardMetadata(...ShardMetadata) error
	// RemoveShardData When a shard is deleted on the current node, cube will call this method to clean up local data.
	// The specific data storage can be performed asynchronously or synchronously, and only needs to ensure that the final
	// data can be cleaned up.
	RemoveShardData(shard bhmetapb.Shard, encodedStartKey, encodedEndKey []byte) error
	// Sync persistent data and metadata of the shards to disk.
	Sync(...uint64) error
}

// ShardInitialState shard init state, include the metadata and the last applied log index that persistent to disk.
type ShardMetadata struct {
	ShardID  uint64
	LogIndex uint64
	Metadata []byte
}

// Context
type Context interface {
	// ByteBuf returns the bytebuf that used to avoid memory allocation
	ByteBuf() *buf.ByteBuf
	// Shard returns the current shard id
	Shard() bhmetapb.Shard
	// Requests returns LogRequests, a `LogRequest` corresponds to a Raft-Log.
	// For write scenarios, the engine needs to ensure that each log write and applied Index write is atomic
	// and does not require fsync to disk.
	Requests() []LogRequest
	// AppendResponse once the engine has finished executing a request, call this method to append the response.
	AppendResponse([]byte)
	// SetWrittenBytes set the number of bytes written to storage for all requests currently being executed.
	// This is an approximation value that contributes to the scheduler's auto-rebalance decision.
	// This method must be called before `Read` or `Write` returns.
	SetWrittenBytes(uint64)
	// SetReadBytes set the number of bytes read from storage for all requests currently being executed.
	// This is an approximation value that contributes to the scheduler's auto-rebalance decision.
	// This method must be called before `Read` or `Write` returns.
	SetReadBytes(uint64)
	// SetDiffBytes set the diff of the bytes stored in storage after the command is executed.
	// This is an approximation,  this value is used to modify the approximate amount of data
	// in the `Shard` and is used to help trigger the auto-split process, which is meaningless
	// if the Split operation is customized.
	// This method must be called before `Read` or `Write` returns.
	SetDiffBytes(int64)
}

// LogRequest contains all requests and log index inside a `Raft-Log`.
type LogRequest struct {
	// Index the corresponding log index. For read operations, this value has no meaning.
	Index uint64
	// Requests request cmds of this log
	Requests []CustomCmd
}

// CustomCmd Customized commands
type CustomCmd struct {
	// CmdType cmd type
	CmdType uint64
	// Key request key
	Key []byte
	// Cmd request content
	Cmd []byte
}

// SimpleContext simple context, just for testing
type SimpleContext struct {
	buf          *buf.ByteBuf
	shard        bhmetapb.Shard
	requests     []LogRequest
	responses    [][]byte
	writtenBytes uint64
	readBytes    uint64
	diffBytes    int64
}

// NewSimpleContext returns a testing context.
func NewSimpleContext(shard uint64, requests ...LogRequest) *SimpleContext {
	c := &SimpleContext{buf: buf.NewByteBuf(32), requests: requests}
	c.shard.ID = shard
	return c
}

func (ctx *SimpleContext) ByteBuf() *buf.ByteBuf        { return ctx.buf }
func (ctx *SimpleContext) Shard() bhmetapb.Shard        { return ctx.shard }
func (ctx *SimpleContext) Requests() []LogRequest       { return ctx.requests }
func (ctx *SimpleContext) AppendResponse(value []byte)  { ctx.responses = append(ctx.responses, value) }
func (ctx *SimpleContext) SetWrittenBytes(value uint64) { ctx.writtenBytes = value }
func (ctx *SimpleContext) SetReadBytes(value uint64)    { ctx.readBytes = value }
func (ctx *SimpleContext) SetDiffBytes(value int64)     { ctx.diffBytes = value }

func (ctx *SimpleContext) GetWrittenBytes() uint64 { return ctx.writtenBytes }
func (ctx *SimpleContext) GetReadBytes() uint64    { return ctx.readBytes }
func (ctx *SimpleContext) GetDiffBytes() int64     { return ctx.diffBytes }
func (ctx *SimpleContext) Responses() [][]byte     { return ctx.responses }
