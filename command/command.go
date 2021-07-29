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

package command

import (
	"github.com/fagongzi/goetty/buf"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/util"
)

// Context apply context.
type Context interface {
	// WriteBatch returns the write batch for write application's data, it's only used in write command handle.
	WriteBatch() *util.WriteBatch
	// LogIndex log index, the raft log index of the current command, it's only used in write command handle.
	LogIndex() uint64
	// Offset is the index in the current command batch
	Offset() int
	// BatchSize number of the current command batch
	BatchSize() int
	// Attrs returns a map to store attrs
	Attrs() map[string]interface{}
	// ByteBuf returns the bytebuf
	ByteBuf() *buf.ByteBuf
}

// ReadCommandFunc the read command handler func
type ReadCommandFunc func(bhmetapb.Shard, *raftcmdpb.Request, Context) (*raftcmdpb.Response, uint64)

// WriteCommandFunc the write command handler func, returns write bytes and the diff bytes
// that used to modify the size of the current shard
type WriteCommandFunc func(bhmetapb.Shard, *raftcmdpb.Request, Context) (uint64, int64, *raftcmdpb.Response)

// LocalCommandFunc directly exec on local func
type LocalCommandFunc func(bhmetapb.Shard, *raftcmdpb.Request) (*raftcmdpb.Response, error)
