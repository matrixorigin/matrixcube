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

package raftstore

import (
	"github.com/cockroachdb/errors"
	"github.com/fagongzi/util/protoc"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/storage"
)

func (r *replica) handleRaftCreateSnapshotRequest() error {
	if !r.lr.GetSnapshotRequested() {
		return nil
	}
	r.logger.Info("requested to create snapshot")
	ss, created, err := r.createSnapshot()
	if err != nil {
		return err
	}
	if created {
		r.logger.Info("snapshot created and registered with the raft instance",
			log.SnapshotField(ss))
	}
	return nil
}

func (r *replica) createSnapshot() (raftpb.Snapshot, bool, error) {
	index, term := r.sm.getAppliedIndexTerm()
	if index == 0 {
		panic("invalid snapshot index")
	}
	logger := r.logger.With(
		zap.Uint64("snapshot-index", index))

	cs := r.sm.getConfState()
	logger.Info("createSnapshot called",
		zap.Uint64("snapshot-term", term),
		log.ReplicaIDsField("voters", cs.Voters),
		log.ReplicaIDsField("learners", cs.Learners))

	ss, ssenv, err := r.snapshotter.save(r.sm.dataStorage, cs, index, term)
	if err != nil {
		if errors.Is(err, storage.ErrAborted) {
			logger.Info("snapshot aborted")
			ssenv.MustRemoveTempDir()
			return raftpb.Snapshot{}, false, nil
		}
		logger.Error("failed to save snapshot",
			zap.Error(err))
		return raftpb.Snapshot{}, false, err
	}
	logger.Info("snapshot save completed")
	if err := r.snapshotter.commit(ss, ssenv); err != nil {
		if errors.Is(err, errSnapshotOutOfDate) {
			// the snapshot final dir already exist on disk
			// same snapshot index and same random uint64
			ssenv.MustRemoveTempDir()
			logger.Fatal("snapshot final dir already exist",
				zap.String("directory", ssenv.GetFinalDir()))
		}
		logger.Error("failed to commit saved snapshot",
			zap.Error(err))
		return raftpb.Snapshot{}, false, err
	}
	logger.Info("snapshot committed")
	if err := r.lr.CreateSnapshot(ss); err != nil {
		if errors.Is(err, raft.ErrSnapOutOfDate) {
			// lr already has a more recent snapshot
			logger.Fatal("aborted registering an out of date snapshot",
				log.SnapshotField(ss))
		}
		logger.Error("failed to register the snapshot with the LogReader",
			zap.Error(err))
		return raftpb.Snapshot{}, false, err
	}
	logger.Info("snapshot created")
	return ss, true, nil
}

func (r *replica) applySnapshot(ss raftpb.Snapshot) error {
	logger := r.logger.With(log.SnapshotField(ss))
	// double check whether we are trying to recover from a dummy snapshot
	if len(ss.Data) > 0 {
		var si meta.SnapshotInfo
		protoc.MustUnmarshal(&si, ss.Data)
		if si.Dummy {
			logger.Fatal("trying to recover from a dummy snapshot")
		}
	}
	md, err := r.snapshotter.recover(r.sm.dataStorage, ss)
	if err != nil {
		logger.Error("failed to recover from the snapshot",
			zap.Error(err))
		return err
	}
	r.appliedIndex = ss.Metadata.Index
	// when applying initial snapshot, we've already applied the ss record into
	// the LogReader beforehand, applying the ss record again here would void
	// the lr.SetRange change.
	if r.initialized {
		r.lr.ApplySnapshot(ss)
	}
	r.sm.updateShard(md.Metadata.Shard)
	// after snapshot applied, the shard range may changed, so we
	// need update key ranges
	r.store.updateShardKeyRange(r.group, md.Metadata.Shard)
	// r.replica is more like a local cached copy of the replica record.
	r.replica = *findReplica(r.getShard(), r.storeID)
	r.sm.updateAppliedIndexTerm(ss.Metadata.Index, ss.Metadata.Term)
	// persistentLogIndex is not guaranteed to be the same as ss.Metadata.Index
	// as the log entry at ss.Metadata.Index, including a few nearby entries
	// are entries not visible to the state machine, e.g. NOOP entries or admin
	// entries. in such cases, we will have to keep both the ss snapshot record
	// and its on disk snapshot image.
	persistentLogIndex, err := r.getPersistentLogIndex()
	if err != nil {
		return err
	}
	r.addAction(action{
		actionType: snapshotCompactionAction,
		snapshotCompaction: snapshotCompactionDetails{
			snapshot:           ss,
			persistentLogIndex: persistentLogIndex,
		},
	})
	if r.aware != nil {
		r.aware.Updated(md.Metadata.Shard)
	}
	logger.Info("metadata updated",
		log.ReasonField("apply snapshot"),
		log.ShardField("metadata", md.Metadata.Shard),
		log.ReplicaField("replica", r.replica))
	return nil
}

// TODO: add a test for snapshotCompaction
func (r *replica) snapshotCompaction(ss raftpb.Snapshot,
	persistentLogIndex uint64) error {
	snapshots, err := r.logdb.GetAllSnapshots(r.shardID)
	if err != nil {
		return err
	}
	for _, cs := range snapshots {
		if cs.Metadata.Index < ss.Metadata.Index {
			if err := r.removeSnapshot(cs, true); err != nil {
				return err
			}
		}
	}
	if persistentLogIndex == ss.Metadata.Index {
		if err := r.removeSnapshot(ss, false); err != nil {
			return err
		}
	}
	return nil
}

func (r *replica) removeSnapshot(ss raftpb.Snapshot, removeFromLogDB bool) error {
	logger := r.logger.With(log.SnapshotField(ss))
	if removeFromLogDB {
		if err := r.logdb.RemoveSnapshot(r.shardID, ss.Metadata.Index); err != nil {
			logger.Error("failed to remove snapshot record from logdb",
				zap.Error(err))
			return err
		}
	}
	env := r.snapshotter.getRecoverSnapshotEnv(ss)
	if env.FinalDirExists() {
		r.logger.Info("removing snapshot dir",
			zap.String("dir", env.GetFinalDir()))
		if err := env.RemoveFinalDir(); err != nil {
			logger.Error("failed to remove snapshot final directory",
				zap.Error(err))
			return err
		}
	}
	return nil
}
