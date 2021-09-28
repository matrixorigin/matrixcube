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
	"github.com/matrixorigin/matrixcube/pb/meta"
)

// Executor is used to execute read/write requests
type Executor interface {
	// Write applies write requests into the underlying data storage. The `Context`
	// holds all requests involved and packs as many requests from multiple Raft
	// logs together as possible. The implementation must ensure that the content
	// of each LogRequest instance provided by the Context is atomically applied
	// into the underlying storage. The implementation should call the
	// `SetWrittenBytes`, `SetReadBytes`, `SetDiffBytes` methods of `Context` to
	// report the statistical changes involved in applying the specified `Context`
	// before returning.
	Write(Context) error
	// Read execute read requests. The `Context` holds all the requests involved
	// in this execution. The implementation should call the `SetReadBytes` method
	// of `Context` to report the statistical changes involved in this execution
	// before returning.
	Read(Context) error
}

// DataStorage is the interface to be implemented by data engines for storing
// both table shards data and shards metadata. We assume that data engines are
// WAL-less engines meaning some of its most recent writes will be lost on
// restarts. On such restart, table shards data in the DataStorage will rollback
// to a certain point in time state. GetPersistentLogIndex() can be invoked
// to query the most recent persistent raft log index that can be used to
// identify such point in time state. DataStorage guarantees that its table
// shards data and shards metadata will never rollback to any earlier state in
// feuture restarts. GetInitialStates() is invoked immediate after each restart,
// it returns ShardMetadata of all known shards consistent to the above
// mentioned persistent table shards data state. This means the state of the
// data storage will be consistent as long as raft logs are replayed from the
// GetPersistentLogIndex() + 1.
type DataStorage interface {
	BaseStorage
	// GetExecutor returns the `Executor` instance used for applying read and
	// write requests.
	GetExecutor() Executor
	// GetInitialStates returns the most recent shard states of all shards known
	// to the DataStorage instance that are consistent with their related table
	// shards data. The shard metadata is last changed by the raft log identified
	// by the LogIndex value.
	GetInitialStates() ([]ShardMetadata, error)
	// GetPersistentLogIndex returns the most recent raft log index that is known
	// to have its update persistently stored. This means all updates made by Raft
	// logs no greater than the returned index value have been persistently stored,
	// they are guaranteed to be available after reboot.
	GetPersistentLogIndex(shardID uint64) (uint64, error)
	// SaveShardMetadata saves the provided shards metadata into the DataStorage.
	// It is up to the storage engine to determine whether to synchronize the
	// saved content to persistent storage or not. It is also the responsibility
	// of the data storage to ensure that a consistent view of shard data and
	// metadata is always available on restart.
	SaveShardMetadata([]ShardMetadata) error
	// RemoveShardData is used for cleaning up data for the specified shard. It is
	// up to the implementation to decide whether to do the cleaning asynchronously
	// or not.
	RemoveShardData(shard meta.Shard, start, end []byte) error
	// Sync persistently saves table shards data and shards metadata of the
	// specified shards to the underlying persistent storage.
	Sync([]uint64) error
}

// ShardMetadata is the metadata of the shard consistent with the current table
// shard data.
type ShardMetadata struct {
	ShardID  uint64
	LogIndex uint64
	Metadata []byte
}

// TODO: split this to ReadContext and WriteContext

// Context
type Context interface {
	// ByteBuf returns the bytebuf that can be used to avoid memory allocation.
	ByteBuf() *buf.ByteBuf
	// Shard returns the current shard details.
	Shard() meta.Shard
	// Batches returns a list of Batch instance, each representing requests from a
	// single Raft log.
	Batches() []Batch
	// AppendResponse is used for appending responses once each request is handled.
	AppendResponse([]byte)
	// SetWrittenBytes set the number of bytes written to storage for all requests
	// in the current Context instance. This is an approximation value that
	// contributes to the scheduler's auto-rebalancing feature.
	// This method must be called before `Read` or `Write` returns.
	SetWrittenBytes(uint64)
	// SetReadBytes set the number of bytes read from storage for all requests in
	// the current context. This is an approximation value that contributes to the
	// scheduler's auto-rebalancing feature.
	SetReadBytes(uint64)
	// SetDiffBytes set the diff of the bytes stored in storage after Write is
	// executed. This is an approximation value used to modify the approximate
	// amount of data in the `Shard` which is used for triggering the auto-split
	// procedure.
	SetDiffBytes(int64)
}

// Batch contains a list of requests. For write batches, all requests are from
// the same raft log specified by the Index value. They must be atomically
// applied into the data storage together with the Index value itself. For
// read operation, each Batch contains multiple read requests.
type Batch struct {
	// Index is the corresponding raft log index of the batch. It is always zero
	// for read related batches.
	Index uint64
	// Requests is the requests included in the batch.
	Requests []Request
}

// Request is the custom request type.
type Request struct {
	// CmdType is the request type.
	CmdType uint64
	// Key is the key of the request.
	Key []byte
	// Cmd is the content of the request.
	Cmd []byte
}

// SimpleContext is a simple Context implementation used for testing.
type SimpleContext struct {
	buf          *buf.ByteBuf
	shard        meta.Shard
	requests     []Batch
	responses    [][]byte
	writtenBytes uint64
	readBytes    uint64
	diffBytes    int64
}

var _ Context = (*SimpleContext)(nil)

// NewSimpleContext returns a testing context.
func NewSimpleContext(shard uint64, requests ...Batch) *SimpleContext {
	c := &SimpleContext{buf: buf.NewByteBuf(32), requests: requests}
	c.shard.ID = shard
	return c
}

func (ctx *SimpleContext) ByteBuf() *buf.ByteBuf        { return ctx.buf }
func (ctx *SimpleContext) Shard() meta.Shard            { return ctx.shard }
func (ctx *SimpleContext) Batches() []Batch             { return ctx.requests }
func (ctx *SimpleContext) AppendResponse(value []byte)  { ctx.responses = append(ctx.responses, value) }
func (ctx *SimpleContext) SetWrittenBytes(value uint64) { ctx.writtenBytes = value }
func (ctx *SimpleContext) SetReadBytes(value uint64)    { ctx.readBytes = value }
func (ctx *SimpleContext) SetDiffBytes(value int64)     { ctx.diffBytes = value }

func (ctx *SimpleContext) GetWrittenBytes() uint64 { return ctx.writtenBytes }
func (ctx *SimpleContext) GetReadBytes() uint64    { return ctx.readBytes }
func (ctx *SimpleContext) GetDiffBytes() int64     { return ctx.diffBytes }
func (ctx *SimpleContext) Responses() [][]byte     { return ctx.responses }
