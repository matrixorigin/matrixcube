// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
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
//
// this file is adopted from github.com/lni/dragonboat

package transport

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/errors"

	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/vfs"
)

var (
	// ErrStopped is the error returned to indicate that the connection has
	// already been stopped.
	ErrStopped = errors.New("connection stopped")
)

type job struct {
	logger    *zap.Logger
	conn      SnapshotConnection
	fs        vfs.IFS
	ctx       context.Context
	transImpl TransImpl
	ch        chan meta.SnapshotChunk
	completed chan struct{}
	stopc     chan struct{}
	failed    chan struct{}
	shardID   uint64
	replicaID uint64
}

func newJob(logger *zap.Logger,
	ctx context.Context, shardID uint64, replicaID uint64, sz int,
	transport TransImpl, stopc chan struct{}, fs vfs.FS) *job {
	j := &job{
		shardID:   shardID,
		replicaID: replicaID,
		logger:    logger,
		ctx:       ctx,
		transport: transport,
		stopc:     stopc,
		failed:    make(chan struct{}),
		completed: make(chan struct{}),
		fs:        fs,
	}
	j.ch = make(chan pb.Chunk, sz)
	return j
}

func (j *job) close() {
	if j.conn != nil {
		j.conn.Close()
	}
}

func (j *job) connect(addr string) error {
	conn, err := j.transport.GetSnapshotConnection(j.ctx, addr)
	if err != nil {
		plog.Errorf("failed to get a job to %s, %v", addr, err)
		return err
	}
	j.conn = conn
	return nil
}

func (j *job) addSnapshot(chunks []meta.SnapshotChunk) {
	if len(chunks) != cap(j.ch) {
		plog.Panicf("cap of ch is %d, want %d", cap(j.ch), len(chunks))
	}
	for _, chunk := range chunks {
		j.ch <- chunk
	}
}

func (j *job) process() error {
	if j.conn == nil {
		panic("nil connection")
	}
	// TODO: when to remove the snapshot image on disk
	return j.sendSnapshot()
}

func (j *job) sendSnapshot() error {
	chunks := make([]meta.SnapshotChunk, 0)
	for {
		select {
		case <-j.stopc:
			return ErrStopped
		case chunk := <-j.ch:
			if len(chunks) == 0 && chunk.ChunkID != 0 {
				panic("chunk alignment error")
			}
			chunks = append(chunks, chunk)
			if chunk.ChunkID+1 == chunk.ChunkCount {
				return j.sendChunks(chunks)
			}
		}
	}
}

func (j *job) sendChunks(chunks []meta.SnapshotChunk) error {
	chunkData := make([]byte, snapshotChunkSize)
	for _, chunk := range chunks {
		select {
		case <-j.stopc:
			return ErrStopped
		default:
		}
		data, err := loadChunkData(chunk, chunkData, j.fs)
		if err != nil {
			panicNow(err)
		}
		chunk.Data = data
		if err := j.conn.SendChunk(chunk); err != nil {
			return err
		}
	}
	return nil
}
