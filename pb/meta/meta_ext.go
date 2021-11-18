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

package meta

import (
	"github.com/fagongzi/util/protoc"
)

// IsLastFileChunk returns a boolean value indicating whether the chunk is the
// last chunk of a snapshot file.
func (c *SnapshotChunk) IsLastFileChunk() bool {
	return c.FileChunkID+1 == c.FileChunkCount
}

// IsLastChunk returns a boolean value indicating whether the current chunk is
// the last one for the snapshot.
func (c *SnapshotChunk) IsLastChunk() bool {
	return c.ChunkCount == c.ChunkID+1
}

// Clone clone the shard
func (m Shard) Clone() Shard {
	var value Shard
	protoc.MustUnmarshal(&value, protoc.MustMarshal(&m))
	return value
}
