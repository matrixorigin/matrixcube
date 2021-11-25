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

	"github.com/cockroachdb/errors"
	"github.com/fagongzi/util/protoc"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/snapshot"
	"github.com/matrixorigin/matrixcube/vfs"
)

var (
	// ErrStopped is the error returned to indicate that the connection has
	// already been stopped.
	ErrStopped = errors.New("connection stopped")
)

type job struct {
	logger            *zap.Logger
	conn              SnapshotConnection
	fs                vfs.FS
	ctx               context.Context
	dir               snapshot.SnapshotDirFunc
	transImpl         TransImpl
	ch                chan meta.SnapshotChunk
	completed         chan struct{}
	stopc             chan struct{}
	failed            chan struct{}
	shardID           uint64
	replicaID         uint64
	snapshotChunkSize uint64
}

func newJob(logger *zap.Logger,
	ctx context.Context, shardID uint64, replicaID uint64, count int,
	trans TransImpl, dir snapshot.SnapshotDirFunc,
	stopc chan struct{}, snapshotChunkSize uint64, fs vfs.FS) *job {
	j := &job{
		shardID:           shardID,
		replicaID:         replicaID,
		logger:            logger,
		ctx:               ctx,
		transImpl:         trans,
		dir:               dir,
		stopc:             stopc,
		failed:            make(chan struct{}),
		completed:         make(chan struct{}),
		fs:                fs,
		snapshotChunkSize: snapshotChunkSize,
	}
	j.ch = make(chan meta.SnapshotChunk, count)
	return j
}

func (j *job) close() {
	if j.conn != nil {
		j.conn.Close()
	}
}

func (j *job) connect(addr string) error {
	conn, err := j.transImpl.GetSnapshotConnection(j.ctx, addr)
	if err != nil {
		j.logger.Error("failed to get a job",
			zap.String("addr", addr),
			zap.Error(err))
		return err
	}
	j.conn = conn
	return nil
}

func (j *job) addSnapshot(chunks []meta.SnapshotChunk) {
	if len(chunks) != cap(j.ch) {
		j.logger.Fatal("unexpected snapshot chunk count")
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
	chunkData := make([]byte, j.snapshotChunkSize)
	for _, chunk := range chunks {
		select {
		case <-j.stopc:
			return ErrStopped
		default:
		}
		env := j.getEnv(chunk)
		data, err := loadChunkData(chunk,
			env.GetFinalDir(), chunkData, j.snapshotChunkSize, j.fs)
		if err != nil {
			j.logger.Fatal("failed to load chunk data",
				zap.Error(err))
		}
		chunk.Data = data
		if err := j.conn.SendChunk(chunk); err != nil {
			return err
		}
	}
	return nil
}

func (j *job) getEnv(chunk meta.SnapshotChunk) snapshot.SSEnv {
	si := meta.SnapshotInfo{}
	protoc.MustUnmarshal(&si, chunk.Extra)
	env := snapshot.NewSSEnv(j.dir, chunk.ShardID, chunk.From,
		chunk.Index, si.Extra, snapshot.CreatingMode, j.fs)
	env.FinalizeIndex(chunk.Index)
	return env
}
