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

package storage

import (
	"github.com/cockroachdb/errors"

	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/storage/stats"
	"github.com/matrixorigin/matrixcube/util/buf"
)

var (
	// ErrAborted indicates that the request operation was aborted as the data
	// storage couldn't handle the request at the time of request. The client
	// is suggested to retry later.
	ErrAborted = errors.New("operation aborted")
	// ErrShardNotFound is returned by the data storage to indicate that the
	// requested shard is not found.
	ErrShardNotFound = errors.New("shard not found")
)

// Closeable is an instance that can be closed.
type Closeable interface {
	// Close closes the instance.
	Close() error
}

// StatsKeeper is an instance that can provide stats.
type StatsKeeper interface {
	// Stats returns the stats of the instance.
	Stats() stats.Stats
}

// WriteBatchCreator is capable of creating new write batches.
type WriteBatchCreator interface {
	// NewWriteBatch creates and returns a new write batch that can be used with
	// the base storage instance.
	NewWriteBatch() Resetable
}

// Resetable is an instance that can be reset and reused.
type Resetable interface {
	// Reset makes the instance ready to be reused.
	Reset()
}

// Executor is used to execute read/write requests.
type Executor interface {
	// UpdateWriteBatch applies write requests into the provided `WriteContext`.
	// No writes is allowed to be written to the actual underlying data storage.
	UpdateWriteBatch(WriteContext) error
	// ApplyWriteBatch atomically applies the write batch into the underlying
	// data storage.
	ApplyWriteBatch(wb Resetable) error
	// Read executes the read request and returns the result. The `ReadContext`
	// holds the read request to be invoked in this execution. The implementation
	// should call the `SetReadBytes` method of `Context` to report the
	// statistical changes involved in this execution before returning.
	Read(ReadContext) ([]byte, error)
}

// BaseStorage is the interface to be implemented by all storage types.
type BaseStorage interface {
	Closeable
	StatsKeeper
	WriteBatchCreator
	// CreateSnapshot creates a snapshot from the specified shard and store the
	// generated snapshot into the directory specified by the path parameter. It
	// returns the encountered error if there is any.
	CreateSnapshot(shardID uint64, path string) error
	// ApplySnapshot applies the snapshort stored in the given path into the
	// specified shard.
	ApplySnapshot(shardID uint64, path string) error
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
	// Write applies write requests into the underlying data storage. The
	// `WriteContext` holds all requests involved and packs as many requests from
	// multiple Raft logs together as possible. The implementation must ensure
	// that the content of each LogRequest instance provided by the WriteContext
	// is atomically applied into the underlying storage. The implementation
	// should call the `SetWrittenBytes` and `SetDiffBytes` methods of the
	// `WriteContext` to report the statistical changes involved in applying
	// the specified `WriteContext` before returning.
	Write(WriteContext) error
	// TODO: refactor this method again to consider what is the best approach
	// to avoid extra allocation.

	// Read execute read requests and returns the read result. The `ReadContext`
	// holds the read request to invoked. The implementation should call the
	// `SetReadBytes` method of `ReadContext` to report the statistical changes
	// involved in this execution before returning.
	Read(ReadContext) ([]byte, error)
	// GetInitialStates returns the most recent shard states of all shards known
	// to the DataStorage instance that are consistent with their related table
	// shards data. The shard metadata is last changed by the raft log identified
	// by the LogIndex value.
	GetInitialStates() ([]meta.ShardMetadata, error)
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
	SaveShardMetadata([]meta.ShardMetadata) error
	// RemoveShard is used for notifying the data storage that a shard has been
	// removed by MatrixCube. The removeData parameter indicates whether shard
	// data should be removed by the data storage. When removeData is set to true,
	// the data storage is required do the cleaning asynchronously.
	RemoveShard(shard meta.Shard, removeData bool) error
	// Sync persistently saves table shards data and shards metadata of the
	// specified shards to the underlying persistent storage.
	Sync([]uint64) error
	// SplitCheck finds keys within the [start, end) range so that the sum of bytes
	// of each value is no greater than the specified size in bytes. It returns the
	// current bytes(approximate) and the total number of keys(approximate) in [start,end),
	// the founded split keys. The ctx is context information of this check will be passed
	// to the engine by cube in the subsequent split operation.
	SplitCheck(shard meta.Shard, size uint64) (currentApproximateSize uint64,
		currentApproximateKeys uint64, splitKeys [][]byte, ctx []byte, err error)
	// Split After the split request completes raft consensus, it is used to save the
	// metadata after the Shard has executed the split, metadata needs atomically saved
	// into the underlying storage.
	Split(old meta.ShardMetadata, news []meta.ShardMetadata, ctx []byte) error
}

// WriteContext contains the details of write requests to be handled by the
// data storage.
type WriteContext interface {
	// ByteBuf returns the bytebuf that can be used to avoid memory allocation.
	// Note, the will be reset after each Read or Write execution, so it is safe
	// to use bytebuf in one Read or Write call. Multiple calls to `ByteBuf` in
	// a single Read or Write return the same instance.
	ByteBuf() *buf.ByteBuf
	// WriteBatch returns a write batch which will be used to hold a sequence of
	// updates to be atomically made into the underlying storage engine. A
	// resetable instance is returned by this method and it is up to the user to
	// cast it to the actual write batch type compatible with the intended data
	// storage.
	WriteBatch() Resetable
	// Shard returns the current shard details.
	Shard() meta.Shard
	// Batch returns the Batch instance transformed from a single Raft log.
	Batch() Batch
	// AppendResponse is used for appending responses once each request is handled.
	AppendResponse([]byte)
	// SetWrittenBytes set the number of bytes written to storage for all requests
	// in the current Context instance. This is an approximation value that
	// contributes to the scheduler's auto-rebalancing feature.
	// This method must be called before `Read` or `Write` returns.
	SetWrittenBytes(uint64)
	// SetDiffBytes set the diff of the bytes stored in storage after Write is
	// executed. This is an approximation value used to modify the approximate
	// amount of data in the `Shard` which is used for triggering the auto-split
	// procedure.
	SetDiffBytes(int64)
}

type ReadContext interface {
	// ByteBuf returns the bytebuf that can be used to avoid memory allocation.
	ByteBuf() *buf.ByteBuf
	// Shard returns the current shard details.
	Shard() meta.Shard
	// Requeset returns the read request to be processed on the storage engine.
	Request() Request
	// SetReadBytes set the number of bytes read from storage for all requests in
	// the current context. This is an approximation value that contributes to the
	// scheduler's auto-rebalancing feature.
	SetReadBytes(uint64)
}

// Batch contains a list of requests. For write batches, all requests are from
// the same raft log specified by the Index value. They must be atomically
// applied into the data storage together with the Index value itself. For
// read operation, each Batch contains multiple read requests.
type Batch struct {
	// Index is the corresponding raft log index of the batch.
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

// SimpleWriteContext is a simple WriteContext implementation used for testing.
type SimpleWriteContext struct {
	buf          *buf.ByteBuf
	shard        meta.Shard
	wb           Resetable
	batch        Batch
	responses    [][]byte
	writtenBytes uint64
	diffBytes    int64
}

var _ WriteContext = (*SimpleWriteContext)(nil)

// NewSimpleWriteContext returns a testing context.
func NewSimpleWriteContext(shardID uint64,
	base KVStorage, batch Batch) *SimpleWriteContext {
	c := &SimpleWriteContext{
		wb:    base.NewWriteBatch(),
		buf:   buf.NewByteBuf(32),
		batch: batch,
	}
	c.shard.ID = shardID
	return c
}

func (ctx *SimpleWriteContext) ByteBuf() *buf.ByteBuf { return ctx.buf }
func (ctx *SimpleWriteContext) WriteBatch() Resetable { return ctx.wb }
func (ctx *SimpleWriteContext) Shard() meta.Shard     { return ctx.shard }
func (ctx *SimpleWriteContext) Batch() Batch          { return ctx.batch }
func (ctx *SimpleWriteContext) AppendResponse(value []byte) {
	ctx.responses = append(ctx.responses, value)
}
func (ctx *SimpleWriteContext) SetWrittenBytes(value uint64) { ctx.writtenBytes = value }
func (ctx *SimpleWriteContext) SetDiffBytes(value int64)     { ctx.diffBytes = value }
func (ctx *SimpleWriteContext) GetWrittenBytes() uint64      { return ctx.writtenBytes }
func (ctx *SimpleWriteContext) GetDiffBytes() int64          { return ctx.diffBytes }
func (ctx *SimpleWriteContext) Responses() [][]byte          { return ctx.responses }

type SimpleReadContext struct {
	buf       *buf.ByteBuf
	shard     meta.Shard
	request   Request
	readBytes uint64
}

// NewSimpleReadContext returns a testing context.
func NewSimpleReadContext(shardID uint64, req Request) *SimpleReadContext {
	c := &SimpleReadContext{
		buf:     buf.NewByteBuf(32),
		request: req,
	}
	c.shard.ID = shardID
	return c
}

func (c *SimpleReadContext) ByteBuf() *buf.ByteBuf         { return c.buf }
func (c *SimpleReadContext) Shard() meta.Shard             { return c.shard }
func (c *SimpleReadContext) Request() Request              { return c.request }
func (c *SimpleReadContext) SetReadBytes(readBytes uint64) { c.readBytes = readBytes }
func (c *SimpleReadContext) GetReadBytes() uint64          { return c.readBytes }

// KVStorageWrapper is a KVStorage wrapper
type KVStorageWrapper interface {
	// GetKVStorage returns the wrapped KVStorage
	GetKVStorage() KVStorage
}
