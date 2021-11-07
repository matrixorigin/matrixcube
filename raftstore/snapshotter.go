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
	"github.com/lni/goutils/random"
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
	CreateSnapshot(shardID uint64, path string) (uint64, uint64, error)
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
		fn := dirInfo.Name()
		dirName := s.fs.PathJoin(s.rootDir, fn)
		if s.isOrphan(fn) {
			s.logger.Info("found an orphan folder",
				zap.String("dirname", fn))
			// looks like a complete snapshot, but the flag file is still in the dir
			if err := s.processOrphans(dirName, noss, ss, removeDir); err != nil {
				return err
			}
		} else if s.isZombie(fn) {
			s.logger.Info("found a zombie folder",
				zap.String("dirname", fn))
			// snapshot dir name with the ".receiving" or ".generating" suffix
			// implies that it is an incomplete snapshot
			if err := removeDir(dirName); err != nil {
				return err
			}
		} else if s.isSnapshot(fn) {
			// fully processed snapshot image with the flag file already removed
			index := s.parseIndex(fn)
			s.logger.Info("found a snapshot folder",
				zap.String("dirname", fn),
				zap.Bool("no-snapshot-in-logdb", noss),
				zap.Uint64("index", index),
				zap.Uint64("snapshot-index", ss.Metadata.Index))
			if noss || index != ss.Metadata.Index {
				if err := removeDir(dirName); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *snapshotter) processOrphans(dirName string,
	noss bool, ss raftpb.Snapshot, removeDir func(string) error) error {
	var ssFromDir raftpb.Snapshot
	if err := fileutil.GetFlagFileContent(dirName,
		fileutil.SnapshotFlagFilename, &ssFromDir, s.fs); err != nil {
		return err
	}
	if raft.IsEmptySnap(ssFromDir) {
		panic("empty snapshot found")
	}
	remove := false
	if noss {
		remove = true
	} else {
		if ss.Metadata.Index != ssFromDir.Metadata.Index {
			remove = true
		}
	}
	if remove {
		return removeDir(dirName)
	}
	return s.removeFlagFile(dirName)
}

func (s *snapshotter) save(de saveable) (ss raftpb.Snapshot,
	env snapshot.SSEnv, err error) {
	s.logger.Info("saving snapshot",
		zap.String("tmpdir", env.GetTempDir()))
	extra := random.LockGuardedRand.Uint64()
	env = s.getCreatingSnapshotEnv(extra)
	if err := env.CreateTempDir(); err != nil {
		return raftpb.Snapshot{}, env, err
	}
	index, term, err := de.CreateSnapshot(s.shardID, env.GetTempDir())
	if err != nil {
		s.logger.Error("data storage failed to create snapshot",
			zap.Error(err))
		return raftpb.Snapshot{}, env, err
	}
	if index == 0 {
		panic("snapshot index is 0")
	}
	if term == 0 {
		panic("snapshot term is 0")
	}
	s.logger.Info("snapshot saved")
	return raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: index,
			Term:  term,
		},
	}, env, nil
}

func (s *snapshotter) commit(ss raftpb.Snapshot, env snapshot.SSEnv) error {
	env.FinalizeIndex(ss.Metadata.Index)
	if err := env.SaveSSMetadata(&ss.Metadata); err != nil {
		return err
	}
	if err := env.FinalizeSnapshot(&ss); err != nil {
		if errors.Is(err, snapshot.ErrSnapshotOutOfDate) {
			return errSnapshotOutOfDate
		}
		return err
	}
	if err := s.saveSnapshot(ss); err != nil {
		return err
	}
	return env.RemoveFlagFile()
}

func (s *snapshotter) removeFlagFile(dirName string) error {
	return fileutil.RemoveFlagFile(dirName, fileutil.SnapshotFlagFilename, s.fs)
}

func (s *snapshotter) getCreatingSnapshotEnv(extra uint64) snapshot.SSEnv {
	return snapshot.NewSSEnv(s.rootDirFunc,
		s.shardID, s.replicaID, extra, s.replicaID, snapshot.CreatingMode, s.fs)
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

func (s *snapshotter) saveSnapshot(ss raftpb.Snapshot) error {
	wc := s.ldb.NewWorkerContext()
	defer wc.Close()
	return s.ldb.SaveRaftState(s.shardID, s.replicaID, raft.Ready{
		Snapshot: ss,
	}, wc)
}
