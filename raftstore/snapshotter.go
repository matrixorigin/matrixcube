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
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/snapshot"
	"github.com/matrixorigin/matrixcube/util/fileutil"
	"github.com/matrixorigin/matrixcube/vfs"
)

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
func (s *snapshotter) removeReplicaSnapshot() error {
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
	ss, err := s.ldb.GetSnapshot(s.shardID, s.replicaID)
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
				if err := s.remove(ssFromDir.Index); err != nil {
					return err
				}
			} else {
				env := s.getEnv(ssFromDir.Index)
				if err := env.RemoveFlagFile(); err != nil {
					return err
				}
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
	return s.ldb.SaveSnapshot(s.shardID, s.replicaID, sm)
}
