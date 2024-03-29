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
	"github.com/fagongzi/util/protoc"
	"github.com/lni/goutils/random"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/snapshot"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/util/fileutil"
	"github.com/matrixorigin/matrixcube/vfs"
)

var (
	errSnapshotOutOfDate = errors.New("snapshot being generated is out of date")
)

type saveable interface {
	CreateSnapshot(shardID uint64, path string) error
}

var _ saveable = (storage.DataStorage)(nil)

type recoverable interface {
	ApplySnapshot(shardID uint64, path string) error
	GetInitialStates() ([]metapb.ShardMetadata, error)
}

var _ recoverable = (storage.DataStorage)(nil)

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
		if s.isZombie(fn) {
			s.logger.Info("found a zombie folder",
				zap.String("dirname", fn))
			// snapshot dir name with the ".receiving" or ".generating" suffix
			// implies that it is an incomplete snapshot
			if err := removeDir(dirName); err != nil {
				return err
			}
		} else if s.isSnapshotDirectory(fn) {
			// fully processed snapshot image with the flag file already removed
			index := s.parseIndex(fn)
			s.logger.Info("found a snapshot folder",
				zap.String("dirname", fn),
				zap.Bool("no-snapshot-in-logdb", noss),
				log.IndexField(index),
				log.SnapshotField(ss))
			if noss || index != ss.Metadata.Index {
				if err := removeDir(dirName); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *snapshotter) save(de saveable,
	cs raftpb.ConfState, index uint64, term uint64) (ss raftpb.Snapshot,
	env snapshot.SSEnv, err error) {
	extra := random.LockGuardedRand.Uint64()
	env = s.getCreatingSnapshotEnv(extra)
	s.logger.Info("saving snapshot",
		zap.String("tmpdir", env.GetTempDir()))
	if err := env.CreateTempDir(); err != nil {
		s.logger.Error("failed to create snapshot temp directory",
			zap.Error(err))
		return raftpb.Snapshot{}, env, err
	}
	if err := de.CreateSnapshot(s.shardID, env.GetTempDir()); err != nil {
		s.logger.Error("data storage failed to create snapshot",
			zap.Error(err))
		return raftpb.Snapshot{}, env, err
	}
	env.FinalizeIndex(index)
	return raftpb.Snapshot{
		Data: protoc.MustMarshal(&metapb.SnapshotInfo{Extra: extra}),
		Metadata: raftpb.SnapshotMetadata{
			Index:     index,
			Term:      term,
			ConfState: cs,
		},
	}, env, nil
}

func (s *snapshotter) recover(rc recoverable,
	ss raftpb.Snapshot) (metapb.ShardMetadata, error) {
	env := s.getRecoverSnapshotEnv(ss)
	s.logger.Info("recovering from snapshot",
		zap.String("dir", env.GetFinalDir()))
	// TODO: double check to see whether we do have the snapshot folder on disk
	if err := rc.ApplySnapshot(s.shardID, env.GetFinalDir()); err != nil {
		s.logger.Error("data storage failed to apply snapshot",
			zap.Error(err))
		return metapb.ShardMetadata{}, err
	}
	sms, err := rc.GetInitialStates()
	if err != nil {
		s.logger.Error("failed to get initial states from data storage",
			zap.Error(err))
		return metapb.ShardMetadata{}, err
	}
	for _, sm := range sms {
		if sm.ShardID == s.shardID {
			if sm.LogIndex > ss.Metadata.Index {
				s.logger.Fatal("unexpected metadata log index",
					zap.Uint64("log-index", sm.LogIndex),
					zap.Uint64("snapshot-index", ss.Metadata.Index))
			}
			return sm, nil
		}
	}
	panic("missing shard metadata after recovering from snapshot")
}

func (s *snapshotter) commit(ss raftpb.Snapshot, env snapshot.SSEnv) error {
	env.FinalizeIndex(ss.Metadata.Index)
	if err := env.FinalizeSnapshot(); err != nil {
		if errors.Is(err, snapshot.ErrSnapshotOutOfDate) {
			return errSnapshotOutOfDate
		}
		s.logger.Error("failed to finalize saved snapshot",
			zap.Error(err))
		return err
	}
	return nil
}

func (s *snapshotter) getRecoverSnapshotEnv(ss raftpb.Snapshot) snapshot.SSEnv {
	var si metapb.SnapshotInfo
	protoc.MustUnmarshal(&si, ss.Data)
	env := s.getCreatingSnapshotEnv(si.Extra)
	env.FinalizeIndex(ss.Metadata.Index)
	return env
}

func (s *snapshotter) getCreatingSnapshotEnv(extra uint64) snapshot.SSEnv {
	return snapshot.NewSSEnv(s.rootDirFunc,
		s.shardID, s.replicaID, 0, extra, snapshot.CreatingMode, s.fs)
}

func (s *snapshotter) isSnapshotDirectory(dir string) bool {
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
