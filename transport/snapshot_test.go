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
	"io"
	"path/filepath"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fagongzi/util/protoc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/snapshot"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/vfs"
)

var (
	testSnapshotDir   = "/tmp/snapshot_test_dir_safe_to_delete"
	testTransportAddr = "localhost:36001"
)

// test dir layout
//
// snapshot_test_dir_safe_to_delete
//    |____dir1
//          |__datafile1
//          |__datafile2
//    |____dir2
//          |__dir3
//              |__datafile3
//              |__datafile4
func generateTestSnapshotDir(dir string, fs vfs.FS) error {
	if err := fs.RemoveAll(dir); err != nil {
		return err
	}
	fp1 := fs.PathJoin(dir, "dir1")
	if err := fs.MkdirAll(fp1, 0755); err != nil {
		return err
	}
	fp2 := fs.PathJoin(dir, "dir2")
	if err := fs.MkdirAll(fp2, 0755); err != nil {
		return err
	}
	fp3 := fs.PathJoin(dir, "dir2", "dir3")
	if err := fs.MkdirAll(fp3, 0755); err != nil {
		return err
	}

	data := make([]byte, 1024)
	rand.Read(data)
	dfp1 := fs.PathJoin(fp1, "datafile1")
	dfp2 := fs.PathJoin(fp1, "datafile2")
	dfp3 := fs.PathJoin(fp3, "datafile3")
	dfp4 := fs.PathJoin(fp3, "datafile4")
	for _, fp := range []string{dfp1, dfp2, dfp3, dfp4} {
		if err := func() error {
			f, err := fs.Create(fp)
			if err != nil {
				return err
			}
			defer f.Close()
			if _, err := f.Write(data); err != nil {
				return err
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}

func TestSplitSnapshotMessage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	assert.NoError(t, generateTestSnapshotDir(testSnapshotDir, fs))
	defer fs.RemoveAll(testSnapshotDir)
	si := &meta.SnapshotInfo{
		Extra: 12345,
	}
	shardID := uint64(100)
	from := uint64(1)
	to := uint64(2)
	index := uint64(300)
	term := uint64(200)

	m := meta.RaftMessage{
		ShardID: shardID,
		From:    metapb.Replica{ID: from},
		To:      metapb.Replica{ID: to},
		Message: raftpb.Message{
			Type: raftpb.MsgSnap,
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: index,
					Term:  term,
				},
				Data: protoc.MustMarshal(si),
			},
		},
	}

	// 1 chunk for each file
	chunks, err := splitSnapshotMessage(m, testSnapshotDir, 1024, fs)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(chunks))

	for _, chunk := range chunks {
		assert.Equal(t, shardID, chunk.ShardID)
		assert.Equal(t, from, chunk.From)
		assert.Equal(t, to, chunk.ReplicaID)
		assert.Equal(t, index, chunk.Index)
		assert.Equal(t, term, chunk.Term)
		assert.Equal(t, uint64(1024), chunk.ChunkSize)
		assert.Equal(t, uint64(4), chunk.ChunkCount)
		assert.Equal(t, uint64(1), chunk.FileChunkCount)
		assert.Equal(t, uint64(0), chunk.FileChunkID)
		assert.Equal(t, uint64(1024), chunk.FileSize)
		assert.Equal(t, protoc.MustMarshal(si), chunk.Extra)
	}

	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].FilePath < chunks[j].FilePath
	})

	assert.Equal(t, "dir1/datafile1", chunks[0].FilePath)
	assert.Equal(t, "dir1/datafile2", chunks[1].FilePath)
	assert.Equal(t, "dir2/dir3/datafile3", chunks[2].FilePath)
	assert.Equal(t, "dir2/dir3/datafile4", chunks[3].FilePath)

	// more than 1 chunk for each file
	chunks, err = splitSnapshotMessage(m, testSnapshotDir, 1000, fs)
	assert.NoError(t, err)
	assert.Equal(t, 8, len(chunks))

	for _, chunk := range chunks {
		assert.Equal(t, shardID, chunk.ShardID)
		assert.Equal(t, from, chunk.From)
		assert.Equal(t, to, chunk.ReplicaID)
		assert.Equal(t, index, chunk.Index)
		assert.Equal(t, term, chunk.Term)
		assert.Equal(t, uint64(8), chunk.ChunkCount)
		assert.Equal(t, uint64(2), chunk.FileChunkCount)
		assert.Equal(t, uint64(1024), chunk.FileSize)
		assert.Equal(t, protoc.MustMarshal(si), chunk.Extra)

		if chunk.FileChunkID == 0 {
			assert.Equal(t, uint64(1000), chunk.ChunkSize)
		} else if chunk.FileChunkID == 1 {
			assert.Equal(t, uint64(24), chunk.ChunkSize)
		} else {
			t.Errorf("unexpected file chunk id %d", chunk.FileChunkID)
		}
	}

	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].FilePath < chunks[j].FilePath
	})

	assert.Equal(t, "dir1/datafile1", chunks[0].FilePath)
	assert.Equal(t, "dir1/datafile1", chunks[1].FilePath)
	assert.Equal(t, "dir1/datafile2", chunks[2].FilePath)
	assert.Equal(t, "dir1/datafile2", chunks[3].FilePath)
	assert.Equal(t, "dir2/dir3/datafile3", chunks[4].FilePath)
	assert.Equal(t, "dir2/dir3/datafile3", chunks[5].FilePath)
	assert.Equal(t, "dir2/dir3/datafile4", chunks[6].FilePath)
	assert.Equal(t, "dir2/dir3/datafile4", chunks[7].FilePath)
}

func TestLoadChunkData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	assert.NoError(t, generateTestSnapshotDir(testSnapshotDir, fs))
	defer fs.RemoveAll(testSnapshotDir)
	si := &meta.SnapshotInfo{
		Extra: 12345,
	}
	shardID := uint64(100)
	from := uint64(1)
	to := uint64(2)
	index := uint64(300)
	term := uint64(200)

	m := meta.RaftMessage{
		ShardID: shardID,
		From:    metapb.Replica{ID: from},
		To:      metapb.Replica{ID: to},
		Message: raftpb.Message{
			Type: raftpb.MsgSnap,
			Snapshot: raftpb.Snapshot{
				Metadata: raftpb.SnapshotMetadata{
					Index: index,
					Term:  term,
				},
				Data: protoc.MustMarshal(si),
			},
		},
	}

	chunks, err := splitSnapshotMessage(m, testSnapshotDir, 1000, fs)
	assert.NoError(t, err)
	assert.Equal(t, 8, len(chunks))

	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].FilePath < chunks[j].FilePath
	})

	data1, err := loadChunkData(chunks[0], testSnapshotDir, nil, 1000, fs)
	assert.NoError(t, err)
	data2, err := loadChunkData(chunks[1], testSnapshotDir, nil, 1000, fs)
	assert.NoError(t, err)
	data1 = append(data1, data2...)

	data := make([]byte, 1024)
	f, err := fs.Open(fs.PathJoin(testSnapshotDir, chunks[0].FilePath))
	assert.NoError(t, err)
	_, err = f.Read(data)
	assert.NoError(t, err)
	assert.Equal(t, data, data1)
}

func generateTestSnapshotDirWithFiles(fileCount int,
	fileSize int, dir string, fs vfs.FS) error {
	if err := fs.RemoveAll(dir); err != nil {
		return err
	}
	if err := fs.MkdirAll(dir, 0755); err != nil {
		return err
	}
	for i := 0; i < fileCount; i++ {
		fn := fmt.Sprintf("testdata-%d.dat", i)
		data := make([]byte, fileSize)
		rand.Read(data)
		if err := func() error {
			fp := fs.PathJoin(dir, fn)
			f, err := fs.Create(fp)
			if err != nil {
				return err
			}
			defer f.Close()
			if _, err := f.Write(data); err != nil {
				return err
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}

func compareDir(source string, target string, fs vfs.FS) (bool, error) {
	srcFiles, err := fs.List(source)
	if err != nil {
		return false, err
	}
	sourceData := make([]byte, 0)
	dstData := make([]byte, 0)
	for _, fn := range srcFiles {
		if func() error {
			fp := fs.PathJoin(source, fn)
			f, err := fs.Open(fp)
			if err != nil {
				return err
			}
			defer f.Close()
			result, err := io.ReadAll(f)
			if err != nil {
				return err
			}
			sourceData = append(sourceData, result...)

			dfp := fs.PathJoin(target, fn)
			df, err := fs.Open(dfp)
			if err != nil {
				return err
			}
			defer df.Close()
			result, err = io.ReadAll(df)
			if err != nil {
				return err
			}
			dstData = append(dstData, result...)
			return nil
		}(); err != nil {
			return false, err
		}
	}

	if len(sourceData) != len(dstData) {
		return false, nil
	}

	return reflect.DeepEqual(sourceData, dstData), nil
}

func getTestSnapshotDir(shardID uint64, replicaID uint64) string {
	return filepath.Join(testSnapshotDir,
		fmt.Sprintf("shard-%d-replica-%d", shardID, replicaID))
}

func testContainerResolver(storeID uint64) (string, error) {
	return testTransportAddr, nil
}

type testTransportStatus struct {
	msg                     meta.RaftMessageBatch
	rejected                bool
	messageHandlerCount     uint64
	unreachableHandlerCount uint64
	statusCount             uint64
}

func (t *testTransportStatus) MessageHandler(m meta.RaftMessageBatch) {
	t.msg = m
	atomic.AddUint64(&t.messageHandlerCount, 1)
}

func (t *testTransportStatus) UnreachableHandler(_ uint64, _ uint64) {
	atomic.AddUint64(&t.unreachableHandlerCount, 1)
}

func (t *testTransportStatus) SnapshotStatusHandler(_, _ uint64, _ raftpb.Snapshot, rejected bool) {
	t.rejected = rejected
	atomic.AddUint64(&t.statusCount, 1)
}

func (t *testTransportStatus) getMessageCount() int {
	return int(atomic.LoadUint64(&t.messageHandlerCount))
}

func (t *testTransportStatus) waitMessageCount(tt *testing.T, value int, timeout time.Duration) {
	t.waitCount(tt, value, &t.messageHandlerCount, timeout)
}

func (t *testTransportStatus) waitStatusCount(tt *testing.T, value int, timeout time.Duration) {
	t.waitCount(tt, value, &t.statusCount, timeout)
}

func (t *testTransportStatus) waitCount(tt *testing.T, value int, p *uint64, timeout time.Duration) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ticker.C:
			if int(atomic.LoadUint64(p)) >= value {
				return
			}
		case <-timer.C:
			tt.Fatalf("failed to wait message count")
		}
	}
}

func TestSnapshotCanBeTransported(t *testing.T) {
	// shard 1 replica 1 wants to send a snapshot at index 100 to replica 2, extra value is 12345
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	fs.RemoveAll(testSnapshotDir)
	defer fs.RemoveAll(testSnapshotDir)
	extra := uint64(12345)
	index := uint64(100)
	si := &meta.SnapshotInfo{
		Extra: extra,
	}
	ss := raftpb.Snapshot{
		Data: protoc.MustMarshal(si),
		Metadata: raftpb.SnapshotMetadata{
			Index: index,
			Term:  1,
		},
	}
	m := raftpb.Message{
		Type:     raftpb.MsgSnap,
		From:     1,
		To:       2,
		Term:     1,
		Snapshot: ss,
	}
	raftMsg := meta.RaftMessage{
		ShardID: 1,
		From:    metapb.Replica{ID: 1},
		To:      metapb.Replica{ID: 2},
		Message: m,
	}

	dir := getTestSnapshotDir(1, 2)
	require.NoError(t, fs.MkdirAll(dir, 0755))

	env := snapshot.NewSSEnv(getTestSnapshotDir, 1, 1, index, extra,
		snapshot.CreatingMode, fs)
	env.FinalizeIndex(index)
	require.NoError(t, generateTestSnapshotDirWithFiles(10, 1024, env.GetFinalDir(), fs))
	logger := log.GetDefaultZapLoggerWithLevel(zap.DebugLevel)
	status := &testTransportStatus{}
	trans := NewTransport(logger, testTransportAddr, 2,
		status.MessageHandler, status.UnreachableHandler, status.SnapshotStatusHandler,
		getTestSnapshotDir, testContainerResolver, fs)
	require.NoError(t, trans.Start())
	defer trans.Close()
	assert.True(t, trans.SendSnapshot(raftMsg))
	status.waitMessageCount(t, 1, 10*time.Second)
	status.waitStatusCount(t, 1, 10*time.Second)
}
