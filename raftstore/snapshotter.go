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

package raftstore

import (
	"strconv"

	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/snapshot"
	"github.com/matrixorigin/matrixcube/util/fileutil"
	"github.com/matrixorigin/matrixcube/vfs"
)

var (
	errSnapshotOutOfDate = errors.New("snapshot being generated is out of date")
)

type saveable interface {
	CreateSnapshot(shardID uint64, path string) (uint64, error)
}

type snapshotter struct {
	logger      *zap.Logger
	shardID     uint64
	replicaID   uint64
	rootDirFunc snapshot.SnapshotDirFunc
	rootDir     string
	ldb         logdb.LogDB
	fs          vfs.FS
}

func newSnapshotter(shardID uint64, replicaID uint64,
	logger *zap.Logger, rootDirFunc snapshot.SnapshotDirFunc,
	ldb logdb.LogDB, fs vfs.FS) *snapshotter {
	return &snapshotter{
		logger:      logger,
		shardID:     shardID,
		replicaID:   replicaID,
		rootDirFunc: rootDirFunc,
		rootDir:     rootDirFunc(shardID, replicaID),
		ldb:         ldb,
		fs:          fs,
	}
}

func (s *snapshotter) prepareReplicaSnapshotDir() error {
	exist, err := fileutil.Exist(s.rootDir, s.fs)
	if err != nil {
		return err
	}
	if !exist {
		return fileutil.MkdirAll(s.rootDir, s.fs)
	}
	return s.removeOrphanSnapshots()
}

// TODO: this can only be invoked after the replica is fully unloaded.
// need to consider the snapshot transport pool as well.
func (s *snapshotter) removeReplicaSnapshotDir() error {
	exist, err := fileutil.Exist(s.rootDir, s.fs)
	if err != nil {
		return err
	}
	if exist {
		return s.fs.RemoveAll(s.rootDir)
	}
	return nil
}

func (s *snapshotter) removeOrphanSnapshots() error {
	noss := false
	ss, err := s.ldb.GetSnapshot(s.shardID)
	if err != nil {
		if errors.Is(err, logdb.ErrNoSnapshot) {
			noss = true
		} else {
			return err
		}
	}
	files, err := s.fs.List(s.rootDir)
	if err != nil {
		return err
	}

	removeDir := func(name string) error {
		if err := s.fs.RemoveAll(name); err != nil {
			return err
		}
		return fileutil.SyncDir(s.rootDir, s.fs)
	}

	for _, n := range files {
		dirInfo, err := s.fs.Stat(s.fs.PathJoin(s.rootDir, n))
		if err != nil {
			return err
		}
		if !dirInfo.IsDir() {
			continue
		}
		dirName := s.fs.PathJoin(s.rootDir, dirInfo.Name())
		if s.isOrphan(dirInfo.Name()) {
			if err := s.processOrphans(dirName, noss, ss); err != nil {
				return err
			}
		} else if s.isZombie(dirInfo.Name()) {
			if err := removeDir(dirName); err != nil {
				return err
			}
		} else if s.isSnapshot(dirInfo.Name()) {
			index := s.parseIndex(dirInfo.Name())
			if noss || index != ss.Index {
				if err := removeDir(dirName); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *snapshotter) processOrphans(dirName string,
	noss bool, ss raftpb.SnapshotMetadata) error {
	var ssFromDir raftpb.SnapshotMetadata
	if err := fileutil.GetFlagFileContent(dirName,
		fileutil.SnapshotFlagFilename, &ssFromDir, s.fs); err != nil {
		return err
	}
	if ssFromDir.Index == 0 {
		panic("empty snapshot found")
	}
	remove := false
	if noss {
		remove = true
	} else {
		if ss.Index != ssFromDir.Index {
			remove = true
		}
	}
	if remove {
		return s.remove(ssFromDir.Index)
	}
	env := s.getEnv(ssFromDir.Index)
	return env.RemoveFlagFile()
}

func (s *snapshotter) save(de saveable) (ss raftpb.Snapshot,
	env snapshot.SSEnv, err error) {
	env = snapshot.NewSSEnv(s.rootDirFunc,
		s.shardID, s.replicaID, 0, s.replicaID, snapshot.CreatingMode, s.fs)
	if err := env.CreateTempDir(); err != nil {
		return raftpb.Snapshot{}, env, err
	}
	index, err := de.CreateSnapshot(s.shardID, env.GetTempDir())
	if err != nil {
		return raftpb.Snapshot{}, env, err
	}
	return raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: index,
			// FIXME: set term here once it is available from the engine
		},
	}, env, nil
}

func (s *snapshotter) commit(ss raftpb.Snapshot) error {
	env := snapshot.NewSSEnv(s.rootDirFunc,
		s.shardID, s.replicaID, 0, s.replicaID, snapshot.CreatingMode, s.fs)
	if err := env.SaveSSMetadata(&ss.Metadata); err != nil {
		return err
	}
	if err := env.FinalizeSnapshot(&ss); err != nil {
		if errors.Is(err, snapshot.ErrSnapshotOutOfDate) {
			return errSnapshotOutOfDate
		}
		return err
	}
	return env.RemoveFlagFile()
}

func (s *snapshotter) remove(index uint64) error {
	env := s.getEnv(index)
	return env.RemoveFinalDir()
}

func (s *snapshotter) removeFlagFile(index uint64) error {
	env := s.getEnv(index)
	return env.RemoveFlagFile()
}

func (s *snapshotter) getEnv(index uint64) snapshot.SSEnv {
	return snapshot.NewSSEnv(s.rootDirFunc,
		s.shardID, s.replicaID, index, s.replicaID, snapshot.ProcessingMode, s.fs)
}

func (s *snapshotter) dirMatch(dir string) bool {
	return snapshot.SnapshotDirNameRe.Match([]byte(dir))
}

func (s *snapshotter) parseIndex(dir string) uint64 {
	if parts := snapshot.SnapshotDirNamePartsRe.FindStringSubmatch(dir); len(parts) == 2 {
		index, err := strconv.ParseUint(parts[1], 16, 64)
		if err != nil {
			s.logger.Fatal("failed to parse index",
				zap.String("parts", parts[1]))
		}
		return index
	}
	s.logger.Fatal("unknown snapshot fold name",
		zap.String("dirname", dir))
	return 0
}

func (s *snapshotter) isSnapshot(dir string) bool {
	if !s.dirMatch(dir) {
		return false
	}
	fdir := s.fs.PathJoin(s.rootDir, dir)
	return !fileutil.HasFlagFile(fdir, fileutil.SnapshotFlagFilename, s.fs)
}

func (s *snapshotter) isOrphan(dir string) bool {
	if !s.dirMatch(dir) {
		return false
	}
	fdir := s.fs.PathJoin(s.rootDir, dir)
	return fileutil.HasFlagFile(fdir, fileutil.SnapshotFlagFilename, s.fs)
}

func (s *snapshotter) isZombie(dir string) bool {
	return snapshot.GenSnapshotDirNameRe.Match([]byte(dir)) ||
		snapshot.RecvSnapshotDirNameRe.Match([]byte(dir))
}

func (s *snapshotter) saveSnapshot(sm raftpb.SnapshotMetadata) error {
	wc := s.ldb.NewWorkerContext()
	return s.ldb.SaveRaftState(s.shardID, s.replicaID, raft.Ready{
		Snapshot: raftpb.Snapshot{
			Metadata: sm,
		},
	}, wc)
}
