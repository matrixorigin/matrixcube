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
	"sort"
	"testing"

	"github.com/fagongzi/util/protoc"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/vfs"
)

var (
	testSnapshotDir = "snapshot_test_dir_safe_to_delete"
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
