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
	"crypto/rand"
	"fmt"
	"reflect"
	"testing"

	"github.com/fagongzi/util/protoc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/util/fileutil"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/vfs"
)

func snapshotDirFunc(shardID uint64, replicaID uint64) string {
	fs := vfs.GetTestFS()
	return fs.PathJoin(testSnapshotDir,
		fmt.Sprintf("snapshot-%d-%d", shardID, replicaID))
}

func runChunkTest(t *testing.T,
	fn func(*testing.T, *Chunk, *testMessageHandler), fs vfs.FS) {
	defer func() {
		if err := fs.RemoveAll(testSnapshotDir); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	defer leaktest.AfterTest(t)()
	require.NoError(t, fs.RemoveAll(testSnapshotDir))
	inputs := getTestChunks()
	dir := snapshotDirFunc(inputs[0].ShardID, inputs[0].ReplicaID)
	require.NoError(t, fs.MkdirAll(dir, 0755))

	logger := log.GetDefaultZapLoggerWithLevel(zap.DebugLevel)
	handler := newTestMessageHandler()
	chunks := NewChunk(logger,
		handler.HandleMessageBatch, snapshotDirFunc, fs)
	fn(t, chunks, handler)
}

func hasSnapshotTempDir(cs *Chunk, c meta.SnapshotChunk) bool {
	env := cs.getEnv(c)
	fp := env.GetTempDir()
	if _, err := cs.fs.Stat(fp); vfs.IsNotExist(err) {
		return false
	}
	return true
}

func getTestChunks() []meta.SnapshotChunk {
	si := &meta.SnapshotInfo{
		Extra: 12345,
	}
	result := make([]meta.SnapshotChunk, 0)
	for chunkID := uint64(0); chunkID < 10; chunkID++ {
		c := meta.SnapshotChunk{
			ShardID:        100,
			ReplicaID:      2,
			From:           12,
			FileChunkID:    chunkID,
			FileChunkCount: 10,
			ChunkID:        chunkID,
			ChunkSize:      1024,
			ChunkCount:     10,
			Index:          2,
			Term:           3,
			FilePath:       "dir/test.data",
			FileSize:       10240,
			Extra:          protoc.MustMarshal(si),
		}
		data := make([]byte, 1024)
		rand.Read(data)
		c.Data = data
		result = append(result, c)
	}
	return result
}

func TestMaxSlotIsEnforced(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunks()
		v := uint64(1)
		c := inputs[0]
		for i := uint64(0); i < maxConcurrentSlot; i++ {
			v++
			c.ShardID = v
			snapDir := chunks.dir(v, c.ReplicaID)
			if err := chunks.fs.MkdirAll(snapDir, 0755); err != nil {
				t.Fatalf("%v", err)
			}
			if !chunks.addLocked(c) {
				t.Errorf("failed to add chunk")
			}
		}
		count := len(chunks.mu.tracked)
		for i := uint64(0); i < maxConcurrentSlot; i++ {
			v++
			c.ShardID = v
			if chunks.addLocked(c) {
				t.Errorf("not rejected")
			}
		}
		if len(chunks.mu.tracked) != count {
			t.Errorf("tracked count changed")
		}
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestOutOfOrderChunkWillBeIgnored(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunks()
		chunks.addLocked(inputs[0])
		key := chunkKey(inputs[0])
		td := chunks.mu.tracked[key]
		next := td.next
		td.next = next + 10
		if chunks.record(inputs[1]) != nil {
			t.Fatalf("out of order chunk is not rejected")
		}
		td = chunks.mu.tracked[key]
		if next+10 != td.next {
			t.Fatalf("next chunk id unexpected moved")
		}
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestChunkFromANewLeaderIsIgnored(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunks()
		chunks.addLocked(inputs[0])
		key := chunkKey(inputs[0])
		td := chunks.mu.tracked[key]
		next := td.next
		td.first.From = td.first.From + 1
		if chunks.record(inputs[1]) != nil {
			t.Fatalf("chunk from a different leader is not rejected")
		}
		td = chunks.mu.tracked[key]
		if next != td.next {
			t.Fatalf("next chunk id unexpected moved")
		}
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestNotTrackedChunkWillBeIgnored(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunks()
		if chunks.record(inputs[1]) != nil {
			t.Errorf("not tracked chunk not rejected")
		}
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestGetOrCreateSnapshotLock(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		l := chunks.getSnapshotLock("k1")
		l1, ok := chunks.mu.locks["k1"]
		if !ok || l != l1 {
			t.Errorf("lock not recorded")
		}
		l2 := chunks.getSnapshotLock("k2")
		l3 := chunks.getSnapshotLock("k3")
		if l2 == nil || l3 == nil {
			t.Errorf("lock not returned")
		}
		ll := chunks.getSnapshotLock("k1")
		if l1 != ll {
			t.Errorf("lock changed")
		}
		if len(chunks.mu.locks) != 3 {
			t.Errorf("%d locks, want 3", len(chunks.mu.locks))
		}
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestAddFirstChunkRecordsTheSnapshotAndCreatesTheTempDir(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunks()
		chunks.addLocked(inputs[0])
		td, ok := chunks.mu.tracked[chunkKey(inputs[0])]
		if !ok {
			t.Errorf("failed to record last received time")
		}
		receiveTime := td.tick
		if receiveTime != chunks.getTick() {
			t.Errorf("unexpected time")
		}
		recordedChunk, ok := chunks.mu.tracked[chunkKey(inputs[0])]
		if !ok {
			t.Errorf("failed to record chunk")
		}
		expectedChunk := inputs[0]
		if !reflect.DeepEqual(&expectedChunk, &recordedChunk.first) {
			t.Errorf("chunk changed")
		}
		if !hasSnapshotTempDir(chunks, inputs[0]) {
			t.Errorf("no temp file")
		}
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestGcRemovesRecordAndTempDir(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunks()
		chunks.addLocked(inputs[0])
		if !chunks.addLocked(inputs[0]) {
			t.Fatalf("failed to add chunk")
		}
		count := chunks.timeout + chunks.gcTick
		for i := uint64(0); i < count; i++ {
			chunks.Tick()
		}
		_, ok := chunks.mu.tracked[chunkKey(inputs[0])]
		if ok {
			t.Errorf("failed to remove last received time")
		}
		if hasSnapshotTempDir(chunks, inputs[0]) {
			t.Errorf("failed to remove temp file")
		}
		if handler.getSnapshotCount(100, 2) != 0 {
			t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 0)
		}
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestReceiveCompleteChunksWillBeMergedIntoSnapshot(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunks()
		for _, c := range inputs {
			if !chunks.addLocked(c) {
				t.Errorf("failed to add chunk")
			}
		}
		_, ok := chunks.mu.tracked[chunkKey(inputs[0])]
		if ok {
			t.Errorf("failed to remove last received time")
		}
		if hasSnapshotTempDir(chunks, inputs[0]) {
			t.Errorf("failed to remove temp file")
		}
		if handler.getSnapshotCount(100, 2) != 1 {
			t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 1)
		}
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestChunkAreIgnoredWhenReplicaIsRemoved(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunks()
		env := chunks.getEnv(inputs[0])
		if !chunks.addLocked(inputs[0]) {
			t.Fatalf("failed to add chunk")
		}
		if !chunks.addLocked(inputs[1]) {
			t.Fatalf("failed to add chunk")
		}
		snapshotDir := env.GetRootDir()
		if err := fileutil.MarkDirAsDeleted(snapshotDir, &meta.RaftMessage{}, chunks.fs); err != nil {
			t.Fatalf("failed to create the delete flag %v", err)
		}
		for idx, c := range inputs {
			if idx <= 1 {
				continue
			}
			if chunks.addLocked(c) {
				t.Fatalf("chunks not rejected")
			}
		}
		tmpSnapDir := env.GetTempDir()
		if _, err := chunks.fs.Stat(tmpSnapDir); !vfs.IsNotExist(err) {
			t.Errorf("tmp dir not removed")
		}
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

// when there is no flag file
func TestOutOfDateChunkCanBeHandled(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunks()
		env := chunks.getEnv(inputs[0])
		snapDir := env.GetFinalDir()
		if err := chunks.fs.MkdirAll(snapDir, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		for _, c := range inputs {
			chunks.addLocked(c)
		}
		if _, ok := chunks.mu.tracked[chunkKey(inputs[0])]; ok {
			t.Errorf("failed to remove last received time")
		}
		if handler.getSnapshotCount(100, 2) != 0 {
			t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 0)
		}
		tmpSnapDir := env.GetTempDir()
		if _, err := chunks.fs.Stat(tmpSnapDir); !vfs.IsNotExist(err) {
			t.Errorf("tmp dir not removed")
		}
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestSignificantlyDelayedNonFirstChunkAreIgnored(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunks()
		chunks.addLocked(inputs[0])
		count := chunks.timeout + chunks.gcTick
		for i := uint64(0); i < count; i++ {
			chunks.Tick()
		}
		if _, ok := chunks.mu.tracked[chunkKey(inputs[0])]; ok {
			t.Errorf("failed to remove last received time")
		}
		if hasSnapshotTempDir(chunks, inputs[0]) {
			t.Errorf("failed to remove temp file")
		}
		if handler.getSnapshotCount(100, 2) != 0 {
			t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 0)
		}
		// now we have the remaining chunks
		for _, c := range inputs[1:] {
			if chunks.addLocked(c) {
				t.Errorf("failed to reject chunks")
			}
		}
		if _, ok := chunks.mu.tracked[chunkKey(inputs[0])]; ok {
			t.Errorf("failed to remove last received time")
		}
		if hasSnapshotTempDir(chunks, inputs[0]) {
			t.Errorf("failed to remove temp file")
		}
		if handler.getSnapshotCount(100, 2) != 0 {
			t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 0)
		}
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func checkTestSnapshotFile(t *testing.T,
	chunks *Chunk, chunk meta.SnapshotChunk, size uint64) {
	env := chunks.getEnv(chunk)
	dir := env.GetFinalDir()
	fp := chunks.fs.PathJoin(dir, chunk.FilePath)
	f, err := chunks.fs.Open(fp)
	if err != nil {
		t.Fatalf("failed to open the file %v", err)
	}
	defer f.Close()
	fi, _ := f.Stat()
	if uint64(fi.Size()) != size {
		t.Fatalf("size %d, want %d", fi.Size(), size)
	}
}

func TestAddingFirstChunkAgainResetsTempDir(t *testing.T) {
	fn := func(t *testing.T, chunks *Chunk, handler *testMessageHandler) {
		inputs := getTestChunks()
		chunks.addLocked(inputs[0])
		chunks.addLocked(inputs[1])
		chunks.addLocked(inputs[2])
		inputs = getTestChunks()
		// now add everything
		for _, c := range inputs {
			if !chunks.addLocked(c) {
				t.Errorf("chunk rejected")
			}
		}
		_, ok := chunks.mu.tracked[chunkKey(inputs[0])]
		if ok {
			t.Errorf("failed to remove last received time")
		}
		if hasSnapshotTempDir(chunks, inputs[0]) {
			t.Errorf("failed to remove temp file")
		}
		if handler.getSnapshotCount(100, 2) != 1 {
			t.Errorf("got %d, want %d", handler.getSnapshotCount(100, 2), 1)
		}
		checkTestSnapshotFile(t, chunks, inputs[0], 10240)
	}
	fs := vfs.GetTestFS()
	runChunkTest(t, fn, fs)
}

func TestToMessageFromChunk(t *testing.T) {
	si := &meta.SnapshotInfo{
		Extra: 12345,
	}
	chunk := meta.SnapshotChunk{
		ShardID:   123,
		ReplicaID: 45,
		From:      23,
		Index:     100,
		Term:      200,
		Extra:     protoc.MustMarshal(si),
	}
	chunks := &Chunk{}
	mb := chunks.toMessage(chunk)
	require.Equal(t, 1, len(mb.Messages))
	msg := mb.Messages[0]
	assert.Equal(t, chunk.ShardID, msg.ShardID)
	assert.Equal(t, chunk.ReplicaID, msg.To.ID)
	assert.Equal(t, chunk.From, msg.From.ID)
	assert.Equal(t, raftpb.MsgSnap, msg.Message.Type)
	assert.Equal(t, chunk.Index, msg.Message.Snapshot.Metadata.Index)
	assert.Equal(t, chunk.Term, msg.Message.Snapshot.Metadata.Term)
	assert.Equal(t, chunk.Extra, msg.Message.Snapshot.Data)
	assert.Equal(t, chunk.From, msg.Message.From)
	assert.Equal(t, chunk.ReplicaID, msg.Message.To)
}
