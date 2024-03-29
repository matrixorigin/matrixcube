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
//
// this file is adopted from github.com/lni/dragonboat

package raftstore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/vfs"
)

const (
	tmpSnapshotDirSuffix = "generating"
	recvTmpDirSuffix     = "receiving"
	snapshotterTestDir   = "/tmp/snapshotter_test_dir_safe_to_delete"
)

func getNewTestDB() (logdb.LogDB, func()) {
	m := mem.NewStorage()
	logger := log.GetDefaultZapLoggerWithLevel(zap.DebugLevel)
	ldb := logdb.NewKVLogDB(m, logger)
	return ldb, func() { m.Close() }
}

func deleteSnapshotterTestDir(fs vfs.FS) {
	if err := fs.RemoveAll(snapshotterTestDir); err != nil {
		panic(err)
	}
}

func getTestSnapshotter(ldb logdb.LogDB, fs vfs.FS) *snapshotter {
	fp := fs.PathJoin(snapshotterTestDir, "snapshot")
	if err := fs.MkdirAll(fp, 0777); err != nil {
		panic(err)
	}
	f := func(shardID uint64, replicaID uint64) string {
		return fp
	}
	logger := log.GetDefaultZapLoggerWithLevel(zap.DebugLevel)
	return newSnapshotter(1, 1, logger, f, ldb, fs)
}

func runSnapshotterTest(t *testing.T,
	fn func(t *testing.T, logdb logdb.LogDB, snapshotter *snapshotter), fs vfs.FS) {
	defer leaktest.AfterTest(t)()
	defer vfs.ReportLeakedFD(fs, t)
	deleteSnapshotterTestDir(fs)
	ldb, closer := getNewTestDB()
	defer closer()
	s := getTestSnapshotter(ldb, fs)
	defer deleteSnapshotterTestDir(fs)
	defer ldb.Close()
	fn(t, ldb, s)
}

func TestPrepareReplicaSnapshotDir(t *testing.T) {
	defer leaktest.AfterTest(t)()

	fs := vfs.GetTestFS()
	ldb, closer := getNewTestDB()
	defer closer()
	fp := fs.PathJoin(snapshotterTestDir, "snapshot")
	assert.NoError(t, fs.RemoveAll(fp))
	f := func(shardID uint64, replicaID uint64) string {
		return fp
	}
	logger := log.GetDefaultZapLoggerWithLevel(zap.DebugLevel)
	ss := newSnapshotter(1, 1, logger, f, ldb, fs)
	if _, err := fs.Stat(fp); vfs.IsExist(err) {
		t.Errorf("replica snapshot dir already created")
	}
	assert.NoError(t, ss.prepareReplicaSnapshotDir())
	if _, err := fs.Stat(fp); vfs.IsNotExist(err) {
		t.Errorf("replica snapshot dir not created")
	}
}

func TestOrphanesWillBeCheckedWhenReplicaSnapshotDirIsAvailable(t *testing.T) {
	testZombieSnapshotDirsCanBeRemoved(t, false)
}

func TestZombieSnapshotDirsCanBeRemoved(t *testing.T) {
	testZombieSnapshotDirsCanBeRemoved(t, true)
}

func testZombieSnapshotDirsCanBeRemoved(t *testing.T, explicit bool) {
	fs := vfs.GetTestFS()
	fn := func(t *testing.T, ldb logdb.LogDB, s *snapshotter) {
		env1 := s.getCreatingSnapshotEnv(0)
		env1.FinalizeIndex(100)
		env2 := s.getCreatingSnapshotEnv(0)
		env2.FinalizeIndex(200)
		fd1 := env1.GetFinalDir()
		fd2 := env2.GetFinalDir()
		fd1 = fd1 + "." + tmpSnapshotDirSuffix
		fd2 = fd2 + "." + recvTmpDirSuffix
		if err := fs.MkdirAll(fd1, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := fs.MkdirAll(fd2, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if explicit {
			if err := s.removeOrphanSnapshots(); err != nil {
				t.Errorf("failed to process orphaned snapshtos %s", err)
			}
		} else {
			if err := s.prepareReplicaSnapshotDir(); err != nil {
				t.Errorf("failed to prepare replica snapshot dir")
			}
		}
		if _, err := fs.Stat(fd1); !vfs.IsNotExist(err) {
			t.Errorf("fd1 not removed")
		}
		if _, err := fs.Stat(fd2); !vfs.IsNotExist(err) {
			t.Errorf("fd2 not removed")
		}
	}
	runSnapshotterTest(t, fn, fs)
}

func TestSnapshotsNotInLogDBAreRemoved(t *testing.T) {
	fs := vfs.GetTestFS()
	fn := func(t *testing.T, ldb logdb.LogDB, s *snapshotter) {
		env1 := s.getCreatingSnapshotEnv(0)
		env1.FinalizeIndex(100)
		env2 := s.getCreatingSnapshotEnv(0)
		env2.FinalizeIndex(200)
		fd1 := env1.GetFinalDir()
		fd2 := env2.GetFinalDir()
		if err := fs.MkdirAll(fd1, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := fs.MkdirAll(fd2, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := s.removeOrphanSnapshots(); err != nil {
			t.Errorf("failed to process orphaned snapshtos %s", err)
		}
		if _, err := fs.Stat(fd1); !vfs.IsNotExist(err) {
			t.Errorf("fd1 %s not removed", fd1)
		}
		if _, err := fs.Stat(fd2); !vfs.IsNotExist(err) {
			t.Errorf("fd2 %s not removed", fd2)
		}
	}
	runSnapshotterTest(t, fn, fs)
}

func TestOnlyMostRecentSnapshotIsKept(t *testing.T) {
	fs := vfs.GetTestFS()
	fn := func(t *testing.T, ldb logdb.LogDB, s *snapshotter) {
		env1 := s.getCreatingSnapshotEnv(0)
		env1.FinalizeIndex(100)
		env2 := s.getCreatingSnapshotEnv(0)
		env2.FinalizeIndex(200)
		env3 := s.getCreatingSnapshotEnv(0)
		env3.FinalizeIndex(300)
		s1 := raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: 200,
				Term:  200,
			},
		}
		fd1 := env1.GetFinalDir()
		fd2 := env2.GetFinalDir()
		fd3 := env3.GetFinalDir()
		if err := s.saveSnapshot(s1); err != nil {
			t.Errorf("failed to save snapshot to logdb")
		}
		if err := fs.MkdirAll(fd1, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := fs.MkdirAll(fd2, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := fs.MkdirAll(fd3, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := s.removeOrphanSnapshots(); err != nil {
			t.Errorf("failed to process orphaned snapshtos %s", err)
		}
		if _, err := fs.Stat(fd1); !vfs.IsNotExist(err) {
			t.Errorf("fd1 %s not removed", fd1)
		}
		if _, err := fs.Stat(fd2); vfs.IsNotExist(err) {
			t.Errorf("fd2 %s removed by mistake", fd2)
		}
		if _, err := fs.Stat(fd3); !vfs.IsNotExist(err) {
			t.Errorf("fd3 %s not removed", fd3)
		}
	}
	runSnapshotterTest(t, fn, fs)
}

func TestFirstSnapshotBecomeOrphanedIsHandled(t *testing.T) {
	fs := vfs.GetTestFS()
	fn := func(t *testing.T, ldb logdb.LogDB, s *snapshotter) {
		env := s.getCreatingSnapshotEnv(0)
		env.FinalizeIndex(100)
		fd1 := env.GetFinalDir()
		if err := fs.MkdirAll(fd1, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := s.removeOrphanSnapshots(); err != nil {
			t.Errorf("failed to process orphaned snapshtos %s", err)
		}
		if _, err := fs.Stat(fd1); !vfs.IsNotExist(err) {
			t.Errorf("fd1 not removed")
		}
	}
	runSnapshotterTest(t, fn, fs)
}

func TestOrphanedSnapshotRecordIsRemoved(t *testing.T) {
	fs := vfs.GetTestFS()
	fn := func(t *testing.T, ldb logdb.LogDB, s *snapshotter) {
		s1 := raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: 100,
				Term:  200,
			},
		}
		s2 := raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: 200,
				Term:  200,
			},
		}
		env1 := s.getCreatingSnapshotEnv(0)
		env1.FinalizeIndex(s1.Metadata.Index)
		env2 := s.getCreatingSnapshotEnv(0)
		env2.FinalizeIndex(s2.Metadata.Index)
		fd1 := env1.GetFinalDir()
		fd2 := env2.GetFinalDir()
		if err := fs.MkdirAll(fd1, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := fs.MkdirAll(fd2, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := s.saveSnapshot(s1); err != nil {
			t.Errorf("failed to save snapshot to logdb")
		}
		if err := s.saveSnapshot(s2); err != nil {
			t.Errorf("failed to save snapshot to logdb")
		}
		// s1 will be removed, s2 will be kept
		if err := s.removeOrphanSnapshots(); err != nil {
			t.Errorf("failed to process orphaned snapshtos %s", err)
		}
		if _, err := fs.Stat(fd1); vfs.IsExist(err) {
			t.Errorf("failed to remove fd1")
		}
		if _, err := fs.Stat(fd2); vfs.IsNotExist(err) {
			t.Errorf("unexpectedly removed fd2")
		}
		snapshot, err := s.ldb.GetSnapshot(1)
		if err != nil {
			t.Fatalf("failed to list snapshot %v", err)
		}
		assert.Equal(t, s2, snapshot)
	}
	runSnapshotterTest(t, fn, fs)
}

func TestOrphanedSnapshotsCanBeProcessed(t *testing.T) {
	fs := vfs.GetTestFS()
	fn := func(t *testing.T, ldb logdb.LogDB, s *snapshotter) {
		s1 := raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: 100,
				Term:  200,
			},
		}
		s2 := raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: 200,
				Term:  200,
			},
		}
		s3 := raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: 300,
				Term:  200,
			},
		}
		env1 := s.getCreatingSnapshotEnv(0)
		env1.FinalizeIndex(s1.Metadata.Index)
		env2 := s.getCreatingSnapshotEnv(0)
		env2.FinalizeIndex(s2.Metadata.Index)
		env3 := s.getCreatingSnapshotEnv(0)
		env3.FinalizeIndex(s3.Metadata.Index)
		fd1 := env1.GetFinalDir()
		fd2 := env2.GetFinalDir()
		fd3 := env3.GetFinalDir()
		fd4 := fmt.Sprintf("%s%s", fd3, "xx")
		if err := fs.MkdirAll(fd1, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := fs.MkdirAll(fd2, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := fs.MkdirAll(fd4, 0755); err != nil {
			t.Errorf("failed to create dir %v", err)
		}
		if err := s.saveSnapshot(s1); err != nil {
			t.Errorf("failed to save snapshot to logdb")
		}
		// fd1 has record in logdb. flag file expected to be removed while the fd1
		// foler is expected to be kept
		// fd2 doesn't has its record in logdb, while the most recent snapshot record
		// in logdb is not for fd2, fd2 will be entirely removed
		if err := s.removeOrphanSnapshots(); err != nil {
			t.Errorf("failed to process orphaned snapshtos %s", err)
		}
		if _, err := fs.Stat(fd1); vfs.IsNotExist(err) {
			t.Errorf("fd1 removed by mistake")
		}
		if _, err := fs.Stat(fd2); !vfs.IsNotExist(err) {
			t.Errorf("fd2 not removed")
		}
		if _, err := fs.Stat(fd4); vfs.IsNotExist(err) {
			t.Errorf("fd4 removed by mistake")
		}
	}
	runSnapshotterTest(t, fn, fs)
}

func TestSnapshotDirNameMatchWorks(t *testing.T) {
	fn := func(t *testing.T, ldb logdb.LogDB, s *snapshotter) {
		tests := []struct {
			dirName string
			valid   bool
		}{
			{"snapshot-AB-CD", true},
			{"snapshot-AB", false},
			{"snapshot", false},
			{"xxxsnapshot-AB", false},
			{"snapshot-ABd", false},
			{"snapshot-", false},
		}
		for idx, tt := range tests {
			v := s.isSnapshotDirectory(tt.dirName)
			if v != tt.valid {
				t.Errorf("dir name %s (%d) failed to match", tt.dirName, idx)
			}
		}
	}
	fs := vfs.GetTestFS()
	runSnapshotterTest(t, fn, fs)
}

func TestZombieSnapshotDirNameMatchWorks(t *testing.T) {
	fn := func(t *testing.T, ldb logdb.LogDB, s *snapshotter) {
		tests := []struct {
			dirName string
			valid   bool
		}{
			{"snapshot-AB", false},
			{"snapshot", false},
			{"xxxsnapshot-AB", false},
			{"snapshot-", false},
			{"snapshot-AB-01.receiving", true},
			{"snapshot-AB-CD.receiving", true},
			{"snapshot-AB-1G.receiving", false},
			{"snapshot-AB.receiving", false},
			{"snapshot-XX.receiving", false},
			{"snapshot-AB.receivingd", false},
			{"dsnapshot-AB.receiving", false},
			{"snapshot-AB-CD.generating", true},
			{"snapshot-AB-01.generating", true},
			{"snapshot-AB-0G.generating", false},
			{"snapshot-XX.generating", false},
			{"snapshot-AB.generatingd", false},
			{"dsnapshot-AB.generating", false},
		}
		for idx, tt := range tests {
			v := s.isZombie(tt.dirName)
			if v != tt.valid {
				t.Errorf("dir name %s (%d) failed to match", tt.dirName, idx)
			}
		}
	}
	fs := vfs.GetTestFS()
	runSnapshotterTest(t, fn, fs)
}

func TestSnapshotterParseIndex(t *testing.T) {
	tests := []struct {
		name  string
		index uint64
		fail  bool
	}{
		{"snapshot-123-456", 0x123, false},
		{"snapshot-123-123", 0x123, false},
		{"snapshot-123", 0, true},
		{"xsnapshot-123", 0, true},
		{"snapshot-123x", 0, true},
		{"snapshot-FF", 0, true},
		{"snapshot-FFx", 0, true},
		{"snapshot-123.receiving", 0, true},
		{"snapshot-123.creating", 0, true},
		{"snapshot-123-456.receiving", 0, true},
		{"snapshot-123-456.creating", 0, true},
	}

	fs := vfs.GetTestFS()
	defer leaktest.AfterTest(t)()
	defer vfs.ReportLeakedFD(fs, t)
	deleteSnapshotterTestDir(fs)
	ldb, closer := getNewTestDB()
	defer closer()
	fp := fs.PathJoin(snapshotterTestDir, "snapshot")
	if err := fs.MkdirAll(fp, 0777); err != nil {
		panic(err)
	}
	f := func(shardID uint64, replicaID uint64) string {
		return fp
	}
	logger := log.GetPanicZapLogger()
	snapshotter := newSnapshotter(1, 1, logger, f, ldb, fs)
	defer deleteSnapshotterTestDir(fs)
	defer ldb.Close()

	for _, tt := range tests {
		func() {
			defer func() {
				r := recover()
				if tt.fail {
					if r == nil {
						t.Fatalf("failed to trigger panic")
					}
				} else {
					if r != nil {
						t.Fatalf("unexpectedly triggered panic")
					}
				}
			}()
			result := snapshotter.parseIndex(tt.name)
			assert.Equal(t, tt.index, result)
		}()
	}
}

func TestSnapshotCanBeFinalized(t *testing.T) {
	fs := vfs.GetTestFS()
	fn := func(t *testing.T, ldb logdb.LogDB, s *snapshotter) {
		sm := raftpb.SnapshotMetadata{
			Index: 100,
			Term:  200,
		}
		ss := raftpb.Snapshot{
			Metadata: sm,
		}
		env := s.getCreatingSnapshotEnv(0)
		env.FinalizeIndex(sm.Index)
		finalSnapDir := env.GetFinalDir()
		tmpDir := env.GetTempDir()
		err := env.CreateTempDir()
		if err != nil {
			t.Errorf("create tmp snapshot dir failed %v", err)
		}
		_, err = fs.Stat(tmpDir)
		if err != nil {
			t.Errorf("failed to get stat for tmp dir, %v", err)
		}
		testfp := fs.PathJoin(tmpDir, "test.data")
		f, err := fs.Create(testfp)
		if err != nil {
			t.Errorf("failed to create test file")
		}
		if _, err := f.Write(make([]byte, 12)); err != nil {
			t.Fatalf("write failed %v", err)
		}
		f.Close()
		if err = s.commit(ss, env); err != nil {
			t.Errorf("finalize snapshot failed %v", err)
		}
		_, err = ldb.GetSnapshot(1)
		if err != logdb.ErrNoSnapshot {
			t.Errorf("unexpected snapshot query result %v", err)
		}
		if _, err = fs.Stat(tmpDir); !vfs.IsNotExist(err) {
			t.Errorf("tmp dir not removed, %v", err)
		}
		fi, err := fs.Stat(finalSnapDir)
		if err != nil {
			t.Errorf("failed to get stats, %v", err)
		}
		if !fi.IsDir() {
			t.Errorf("not a dir")
		}
		vfp := fs.PathJoin(finalSnapDir, "test.data")
		fi, err = fs.Stat(vfp)
		if err != nil {
			t.Errorf("failed to get stat %v", err)
		}
		if fi.IsDir() || fi.Size() != 12 {
			t.Errorf("not the same test file. ")
		}
	}
	runSnapshotterTest(t, fn, fs)
}
