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

	"github.com/matrixorigin/matrixcube/components/log"
)

var (
	snapshotChunkSize  uint64 = 1024 * 1024 * 4
	maxConnectionCount uint64 = 64
)

// SendSnapshot asynchronously sends raft snapshot message to its target.
func (t *Transport) SendSnapshot(m meta.RaftMessage) bool {
	if !t.sendSnapshot(m) {
		t.logger.Error("failed to send snapshot",
			log.RaftMessageField("message", m))
		t.sendSnapshotNotification(m.ClusterId, m.To, true)
		return false
	}
	return true
}

func (t *Transport) doSendSnapshot(m meta.RaftMessage) bool {
	if m.Message.Type != raftpb.MsgSnap {
		panic("not a snapshot message")
	}
	chunks, err := splitSnapshotMessage(m, t.fs)
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

	key := raftio.GetNodeInfo(clusterID, toNodeID)
	job := t.createJob(key, addr, false, len(chunks))
	if job == nil {
		return false
	}
	shutdown := func() {
		atomic.AddUint64(&t.jobs, ^uint64(0))
	}
	t.stopper.RunWorker(func() {
		t.processSnapshot(job, addr)
		shutdown()
	})
	job.addSnapshot(chunks)
	return true
}

func (t *Transport) createJob(key raftio.NodeInfo,
	addr string, streaming bool, sz int) *job {
	if v := atomic.AddUint64(&t.jobs, 1); v > maxConnectionCount {
		r := atomic.AddUint64(&t.jobs, ^uint64(0))
		t.logger.Warn("job count is rate limited",
			zap.Uint64(r))
		return nil
	}
	return newJob(t.ctx, key.ClusterID, key.NodeID, t.nhConfig.GetDeploymentID(),
		streaming, sz, t.trans, t.stopper.ShouldStop(), t.fs)
}

func (t *Transport) processSnapshot(c *job, addr string) {
	breaker := t.GetCircuitBreaker(addr)
	successes := breaker.Successes()
	consecFailures := breaker.ConsecFailures()
	shardID := c.shardID
	replicaID := c.replicaID
	if err := func() error {
		if err := c.connect(addr); err != nil {
			t.logger.Warn("failed to get snapshot connection",
				zap.String(addr))
			t.sendSnapshotNotification(shardID, replicaID, true)
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
		t.sendSnapshotNotification(shardID, replicaID, err != nil)
		return err
	}(); err != nil {
		plog.Warningf("processSnapshot failed",
			zap.Error(err))
		breaker.Fail()
	}
}

func (t *Transport) sendSnapshotNotification(shardID uint64,
	replicaID uint64, rejected bool) {
	t.snapshotStatus(clusterID, nodeID, rejected)
}

// filepath is the relative path from the snapshot dir
func splitBySnapshotFile(msg meta.RaftMessage,
	filepath string, filesize uint64, startChunkID uint64) []meta.SnapshotChunk {
	if filesize == 0 {
		panic("empty file")
	}
	results := make([]meta.SnapshotChunk, 0)
	chunkCount := (filesize-1)/snapshotChunkSize + 1
	for i := uint64(0); i < chunkCount; i++ {
		var csz uint64
		if i == chunkCount-1 {
			csz = filesize - (chunkCount-1)*snapshotChunkSize
		} else {
			csz = snapshotChunkSize
		}
		c := meta.SnapshotChunk{
			ShardID:        msg.ShardID,
			ReplicaID:      msg.To.ID,
			From:           msg.From.ID,
			FileChunkID:    i,
			FileChunkCount: chunkCount,
			ChunkID:        startChunkID + i,
			ChunkSize:      csz,
			Index:          msg.Message.Snapshot.Metadata.Index,
			Term:           msg.Message.Snapshot.Metadata.Term,
			Filepath:       filepath,
			FileSize:       filesize,
		}
		results = append(results, c)
	}
	return results
}

func getChunks(m meta.RaftMessage,
	snapshotDir string, checkDir string, fs vfs.FS) ([]meta.SnapshotChunk, error) {
	startChunkID := uint64(0)
	results := make([]meta.SnapshotChunk, 0)

	dir := snapshotDir
	if len(checkDir) > 0 {
		dir = fs.PathJoin(snapshotDir, checkDir)
	}

	files, err := s.fs.List(dir)
	if err != nil {
		return nil, err
	}

	for _, fp := range files {
		fullFilePath := s.fs.PathJoin(dir, fp)
		fileInfo, err := s.fs.Stat(fullFilePath)
		if err != nil {
			return nil, err
		}
		var chunks []meta.SnapshotChunk
		var err error
		if fileInfo.IsDir() {
			chunks, err = getChunks(m, snapshotDir, fs.PathJoin(checkDir, fp), fs)
			if err != nil {
				return nil, err
			}
		} else {
			chunks := splitBySnapshotFile(m,
				fs.PathJoin(checkDir, fp), uint64(fileInfo.Size()), startChunkID)
		}
		startChunkID += uint64(len(chunks))
		results = append(results, chunks...)
	}

	for idx := range results {
		results[idx].ChunkCount = uint64(len(results))
	}
	return results, nil
}

func splitSnapshotMessage(m meta.RaftMessage,
	snapshotDir string, fs vfs.FS) ([]meta.SnapshotChunk, error) {
	if m.Message.Type != meta.MsgSnap {
		panic("not a snapshot message")
	}
	return getChunks(m, snapshotDir, "", fs), nil
}

func loadChunkData(chunk meta.SnapshotChunk,
	snapshotDir string, data []byte, fs vfs.IFS) (result []byte, err error) {
	fp := fs.PathJoin(snapshotDir, chunk.Filepath)
	f, err := openChunkFileForRead(fp, fs)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = firstError(err, f.close())
	}()
	offset := chunk.FileChunkId * snapshotChunkSize
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
