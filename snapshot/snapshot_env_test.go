// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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
// this file is adopted from https://github.com/lni/dragonboat

package snapshot

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	pb "go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/vfs"
)

func reportLeakedFD(fs vfs.FS, t *testing.T) {
	vfs.ReportLeakedFD(fs, t)
}

func TestGetFinalDirWillPanicWhenIndexIsNotFinalizedInCreatingMode(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("failed to trigger panic")
		}
	}()
	f := func(cid uint64, nid uint64) string {
		return "/data"
	}
	fs := vfs.GetTestFS()
	defer reportLeakedFD(fs, t)
	env := NewSSEnv(f, 1, 1, 1, 2, CreatingMode, fs)
	env.GetFinalDir()
}

func TestGetFinalDirWillNotPanicInReceivingMode(t *testing.T) {
	f := func(cid uint64, nid uint64) string {
		return "/data"
	}
	fs := vfs.GetTestFS()
	defer reportLeakedFD(fs, t)
	env := NewSSEnv(f, 1, 1, 1, 2, ReceivingMode, fs)
	assert.Equal(t, "/data/snapshot-0000000000000001-0000000000000002", env.GetFinalDir())
}

func TestGetFinalDirWillNotPanicWhenIndexIsFinalizedInCreatingMode(t *testing.T) {
	f := func(cid uint64, nid uint64) string {
		return "/data"
	}
	fs := vfs.GetTestFS()
	defer reportLeakedFD(fs, t)
	env := NewSSEnv(f, 1, 1, 1, 2, CreatingMode, fs)
	env.FinalizeIndex(100)
	assert.Equal(t, "/data/snapshot-0000000000000064-0000000000000002", env.GetFinalDir())
}

func TestGetSnapshotDirName(t *testing.T) {
	v := GetSnapshotDirName(1, 1)
	if v != "snapshot-0000000000000001-0000000000000001" {
		t.Errorf("unexpected value, %s", v)
	}
	v = GetSnapshotDirName(255, 255)
	if v != "snapshot-00000000000000FF-00000000000000FF" {
		t.Errorf("unexpected value, %s", v)
	}
}

func TestMustBeChild(t *testing.T) {
	tests := []struct {
		parent string
		child  string
		ok     bool
	}{
		{"/home/test", "/home", false},
		{"/home/test", "/home/test", false},
		{"/home/test", "/home/data", false},
		{"/home/test", "/home/test1", false},
		{"/home/test", "/home/test/data", true},
		{"/home/test", "", false},
	}
	for idx, tt := range tests {
		ok := true
		ttok := tt.ok
		ttidx := idx
		parent := tt.parent
		child := tt.child

		f := func() {
			defer func() {
				if r := recover(); r != nil {
					ok = false
				}
				if ok != ttok {
					t.Errorf("idx %d, expected ok value %t", ttidx, ttok)
				}
			}()
			mustBeChild(parent, child)
		}
		f()
	}
}

func TestTempSuffix(t *testing.T) {
	f := func(cid uint64, nid uint64) string {
		return "/data"
	}
	fs := vfs.GetTestFS()
	defer reportLeakedFD(fs, t)
	env := NewSSEnv(f, 1, 1, 1, 2, CreatingMode, fs)
	dir := env.GetTempDir()
	if !strings.Contains(dir, ".generating") {
		t.Errorf("unexpected suffix")
	}
	env = NewSSEnv(f, 1, 1, 1, 2, ReceivingMode, fs)
	dir = env.GetTempDir()
	if !strings.Contains(dir, ".receiving") {
		t.Errorf("unexpected suffix: %s", dir)
	}
}

func TestFinalSnapshotDirDoesNotContainTempSuffix(t *testing.T) {
	f := func(cid uint64, nid uint64) string {
		return "/data"
	}
	fs := vfs.GetTestFS()
	defer reportLeakedFD(fs, t)
	env := NewSSEnv(f, 1, 1, 1, 2, CreatingMode, fs)
	env.FinalizeIndex(1)
	dir := env.GetFinalDir()
	if strings.Contains(dir, ".generating") {
		t.Errorf("unexpected suffix")
	}
}

func TestRootDirIsTheParentOfTempFinalDirs(t *testing.T) {
	f := func(cid uint64, nid uint64) string {
		return "/data"
	}
	fs := vfs.GetTestFS()
	defer reportLeakedFD(fs, t)
	env := NewSSEnv(f, 1, 1, 1, 2, ReceivingMode, fs)
	tmpDir := env.GetTempDir()
	finalDir := env.GetFinalDir()
	rootDir := env.GetRootDir()
	mustBeChild(rootDir, tmpDir)
	mustBeChild(rootDir, finalDir)
}

func runEnvTest(t *testing.T, f func(t *testing.T, env SSEnv), fs vfs.FS) {
	rd := "server-pkg-test-data-safe-to-delete"
	defer reportLeakedFD(fs, t)
	defer func() {
		if err := fs.RemoveAll(rd); err != nil {
			t.Fatalf("%v", err)
		}
	}()
	func() {
		ff := func(cid uint64, nid uint64) string {
			return rd
		}
		env := NewSSEnv(ff, 1, 1, 1, 2, CreatingMode, fs)
		tmpDir := env.GetTempDir()
		if err := fs.MkdirAll(tmpDir, 0755); err != nil {
			t.Fatalf("%v", err)
		}
		f(t, env)
	}()
}

func TestRenameTempDirToFinalDir(t *testing.T) {
	tf := func(t *testing.T, env SSEnv) {
		env.FinalizeIndex(100)
		if err := env.renameToFinalDir(); err != nil {
			t.Errorf("failed to rename dir, %v", err)
		}
	}
	fs := vfs.GetTestFS()
	runEnvTest(t, tf, fs)
}

func TestRenameTempDirToFinalDirCanComplete(t *testing.T) {
	tf := func(t *testing.T, env SSEnv) {
		env.FinalizeIndex(100)
		if env.finalDirExists() {
			t.Errorf("final dir already exist")
		}
		err := env.renameToFinalDir()
		if err != nil {
			t.Errorf("rename tmp dir to final dir failed %v", err)
		}
		if !env.finalDirExists() {
			t.Errorf("final dir does not exist")
		}
		if env.HasFlagFile() {
			t.Errorf("flag file not suppose to be there")
		}
	}
	fs := vfs.GetTestFS()
	runEnvTest(t, tf, fs)
}

func TestFlagFileExists(t *testing.T) {
	tf := func(t *testing.T, env SSEnv) {
		env.FinalizeIndex(100)
		if env.finalDirExists() {
			t.Errorf("final dir already exist")
		}
		msg := &pb.Message{}
		if err := env.createFlagFile(msg); err != nil {
			t.Errorf("failed to create flag file")
		}
		err := env.renameToFinalDir()
		if err != nil {
			t.Errorf("rename tmp dir to final dir failed %v", err)
		}
		if !env.finalDirExists() {
			t.Errorf("final dir does not exist")
		}
		if !env.HasFlagFile() {
			t.Errorf("flag file not suppose to be there")
		}
	}
	fs := vfs.GetTestFS()
	runEnvTest(t, tf, fs)
}

func TestFinalizeSnapshotCanComplete(t *testing.T) {
	tf := func(t *testing.T, env SSEnv) {
		m := &pb.Message{}
		env.FinalizeIndex(100)
		if err := env.FinalizeSnapshot(m); err != nil {
			t.Errorf("failed to finalize snapshot %v", err)
		}
		if !env.HasFlagFile() {
			t.Errorf("no flag file")
		}
		if !env.finalDirExists() {
			t.Errorf("no final dir")
		}
	}
	fs := vfs.GetTestFS()
	runEnvTest(t, tf, fs)
}

func TestFinalizeSnapshotReturnOutOfDateWhenFinalDirExist(t *testing.T) {
	tf := func(t *testing.T, env SSEnv) {
		env.FinalizeIndex(100)
		finalDir := env.GetFinalDir()
		if err := env.fs.MkdirAll(finalDir, 0755); err != nil {
			t.Fatalf("%v", err)
		}
		m := &pb.Message{}
		if err := env.FinalizeSnapshot(m); err != ErrSnapshotOutOfDate {
			t.Errorf("didn't return ErrSnapshotOutOfDate %v", err)
		}
		if env.HasFlagFile() {
			t.Errorf("flag file exist")
		}
	}
	fs := vfs.GetTestFS()
	runEnvTest(t, tf, fs)
}
