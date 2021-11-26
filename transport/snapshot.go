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
	"sync/atomic"

	"github.com/cockroachdb/errors"
	"github.com/fagongzi/util/protoc"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/snapshot"
	"github.com/matrixorigin/matrixcube/vfs"
)

var (
	defaultSnapshotChunkSize uint64 = 1024 * 1024 * 4
	maxConnectionCount       uint64 = 64
)

// SendSnapshot asynchronously sends raft snapshot message to its target.
func (t *Transport) SendSnapshot(m meta.RaftMessage) bool {
	if !t.sendSnapshot(m) {
		t.logger.Error("failed to send snapshot",
			log.RaftMessageField("message", m))
		t.sendSnapshotNotification(m.ShardID, m.To.ID, m.Message.Snapshot, true)
		return false
	}
	return true
}

func (t *Transport) sendSnapshot(m meta.RaftMessage) bool {
	if m.Message.Type != raftpb.MsgSnap {
		panic("not a snapshot message")
	}
	env := t.getEnv(m)
	chunks, err := splitSnapshotMessage(m,
		env.GetFinalDir(), defaultSnapshotChunkSize, t.fs)
	if err != nil {
		t.logger.Error("failed to get snapshot chunks",
			zap.Error(err))
		return false
	}

	storeID := m.To.ContainerID
	targetInfo, resolved := t.resolve(storeID, m.ShardID)
	if !resolved {
		return false
	}

	// fail fast
	if !t.getCircuitBreaker(targetInfo.addr).Ready() {
		return false
	}

	job := t.createJob(m.ShardID, m.To.ID, targetInfo.addr, false, len(chunks))
	if job == nil {
		return false
	}
	shutdown := func() {
		atomic.AddUint64(&t.jobs, ^uint64(0))
	}
	t.stopper.RunWorker(func() {
		t.processSnapshot(job, m.Message.Snapshot, targetInfo.addr)
		shutdown()
	})
	job.addSnapshot(chunks)
	return true
}

func (t *Transport) getEnv(m meta.RaftMessage) snapshot.SSEnv {
	ss := m.Message.Snapshot
	si := meta.SnapshotInfo{}
	protoc.MustUnmarshal(&si, ss.Data)
	env := snapshot.NewSSEnv(t.dir, m.ShardID, m.From.ID,
		ss.Metadata.Index, si.Extra, snapshot.CreatingMode, t.fs)
	env.FinalizeIndex(ss.Metadata.Index)
	return env
}

func (t *Transport) createJob(shardID uint64, toReplicaID uint64,
	addr string, streaming bool, sz int) *job {
	if v := atomic.AddUint64(&t.jobs, 1); v > maxConnectionCount {
		r := atomic.AddUint64(&t.jobs, ^uint64(0))
		t.logger.Warn("job count is rate limited",
			zap.Uint64("job-count", r))
		return nil
	}
	return newJob(t.logger, t.ctx, shardID, toReplicaID,
		sz, t.trans, t.dir, t.stopper.ShouldStop(), defaultSnapshotChunkSize, t.fs)
}

func (t *Transport) processSnapshot(c *job, ss raftpb.Snapshot, addr string) {
	breaker := t.getCircuitBreaker(addr)
	successes := breaker.Successes()
	consecFailures := breaker.ConsecFailures()
	shardID := c.shardID
	replicaID := c.replicaID
	if err := func() error {
		if err := c.connect(addr); err != nil {
			t.logger.Warn("failed to get snapshot connection",
				zap.String("addr", addr))
			t.sendSnapshotNotification(shardID, replicaID, ss, true)
			close(c.failed)
			return err
		}
		defer c.close()
		breaker.Success()
		if successes == 0 || consecFailures > 0 {
			t.logger.Debug("snapshot connection established",
				zap.String("addr", addr))
		}
		err := c.process()
		if err != nil {
			t.logger.Error("failed to process snapshot chunk",
				zap.Error(err))
		}
		t.sendSnapshotNotification(shardID, replicaID, ss, err != nil)
		return err
	}(); err != nil {
		t.logger.Warn("processSnapshot failed",
			zap.Error(err))
		breaker.Fail()
	}
}

func (t *Transport) sendSnapshotNotification(shardID uint64,
	replicaID uint64, ss raftpb.Snapshot, rejected bool) {
	t.snapshotStatus(shardID, replicaID, ss, rejected)
}

func splitSnapshotMessage(m meta.RaftMessage,
	snapshotDir string, chunkSize uint64,
	fs vfs.FS) ([]meta.SnapshotChunk, error) {
	if m.Message.Type != raftpb.MsgSnap {
		panic("not a snapshot message")
	}
	return getChunks(m, snapshotDir, "", chunkSize, fs)
}

func getChunks(m meta.RaftMessage,
	snapshotDir string, checkDir string, chunkSize uint64,
	fs vfs.FS) ([]meta.SnapshotChunk, error) {
	startChunkID := uint64(0)
	results := make([]meta.SnapshotChunk, 0)

	dir := snapshotDir
	if len(checkDir) > 0 {
		dir = fs.PathJoin(snapshotDir, checkDir)
	}

	files, err := fs.List(dir)
	if err != nil {
		return nil, err
	}

	for _, fp := range files {
		fullFilePath := fs.PathJoin(dir, fp)
		fileInfo, err := fs.Stat(fullFilePath)
		if err != nil {
			return nil, err
		}
		var chunks []meta.SnapshotChunk
		if fileInfo.IsDir() {
			chunks, err = getChunks(m,
				snapshotDir, fs.PathJoin(checkDir, fp), chunkSize, fs)
			if err != nil {
				return nil, err
			}
		} else {
			chunks = splitBySnapshotFile(m,
				fs.PathJoin(checkDir, fp), uint64(fileInfo.Size()),
				startChunkID, chunkSize)
		}
		startChunkID += uint64(len(chunks))
		results = append(results, chunks...)
	}

	for idx := range results {
		results[idx].ChunkCount = uint64(len(results))
	}
	return results, nil
}

// filepath is the relative path from the snapshot dir
func splitBySnapshotFile(msg meta.RaftMessage,
	filepath string, filesize uint64, startChunkID uint64,
	chunkSize uint64) []meta.SnapshotChunk {
	if filesize == 0 {
		panic("empty file")
	}
	results := make([]meta.SnapshotChunk, 0)
	chunkCount := (filesize-1)/chunkSize + 1
	for i := uint64(0); i < chunkCount; i++ {
		var csz uint64
		if i == chunkCount-1 {
			csz = filesize - (chunkCount-1)*chunkSize
		} else {
			csz = chunkSize
		}
		c := meta.SnapshotChunk{
			ShardID:        msg.ShardID,
			ReplicaID:      msg.To.ID,
			From:           msg.From.ID,
			FileChunkID:    i,
			FileChunkCount: chunkCount,
			ChunkID:        startChunkID + i,
			ChunkSize:      csz,
			Extra:          msg.Message.Snapshot.Data,
			Index:          msg.Message.Snapshot.Metadata.Index,
			Term:           msg.Message.Snapshot.Metadata.Term,
			FilePath:       filepath,
			FileSize:       filesize,
		}
		results = append(results, c)
	}
	return results
}

func loadChunkData(chunk meta.SnapshotChunk,
	snapshotDir string, data []byte, chunkSize uint64,
	fs vfs.FS) (result []byte, err error) {
	fp := fs.PathJoin(snapshotDir, chunk.FilePath)
	f, err := openChunkFileForRead(fp, fs)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = firstError(err, f.close())
	}()
	offset := chunk.FileChunkID * chunkSize
	if chunk.ChunkSize != uint64(len(data)) {
		data = make([]byte, chunk.ChunkSize)
	}
	n, err := f.readAt(data, int64(offset))
	if err != nil {
		return nil, err
	}
	if uint64(n) != chunk.ChunkSize {
		return nil, errors.New("failed to read the snapshot chunk")
	}
	return data, nil
}
