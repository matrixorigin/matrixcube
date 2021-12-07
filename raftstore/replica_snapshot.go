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
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/log"
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
	cs := r.sm.getConfState()
	ss, ssenv, err := r.snapshotter.save(r.sm.dataStorage, cs, index, term)
	if err != nil {
		if errors.Is(err, storage.ErrAborted) {
			r.logger.Info("snapshot aborted")
			ssenv.MustRemoveTempDir()
			return raftpb.Snapshot{}, false, nil
		}
		return raftpb.Snapshot{}, false, err
	}
	r.logger.Info("snapshot save completed")
	if err := r.snapshotter.commit(ss, ssenv); err != nil {
		if errors.Is(err, errSnapshotOutOfDate) {
			// the snapshot final dir already exist on disk
			// same snapshot index and same random uint64
			ssenv.MustRemoveTempDir()
			r.logger.Fatal("snapshot final dir already exist",
				zap.String("snapshot-dirname", ssenv.GetFinalDir()))
		}
		return raftpb.Snapshot{}, false, err
	}
	r.logger.Info("snapshot committed")
	if err := r.lr.CreateSnapshot(ss); err != nil {
		if errors.Is(err, raft.ErrSnapOutOfDate) {
			// lr already has a more recent snapshot
			r.logger.Fatal("aborted registering an out of date snapshot",
				log.SnapshotField(ss))
		}
		return raftpb.Snapshot{}, false, err
	}
	r.logger.Info("snapshot created")
	// TODO: schedule log compacton here
	return ss, true, nil
}

func (r *replica) applySnapshot(ss raftpb.Snapshot) error {
	md, err := r.snapshotter.recover(r.sm.dataStorage, ss)
	if err != nil {
		return err
	}
	r.sm.updateShard(md.Metadata.Shard)
	r.sm.updateAppliedIndexTerm(ss.Metadata.Index, ss.Metadata.Term)
	return r.removeSnapshot(ss, true)
}

func (r *replica) removeSnapshot(ss raftpb.Snapshot, removeFromLogDB bool) error {
	if removeFromLogDB {
		if err := r.logdb.RemoveSnapshot(r.shardID, ss.Metadata.Index); err != nil {
			return err
		}
	}
	env := r.snapshotter.getRecoverSnapshotEnv(ss)
	return env.RemoveFinalDir()
}
