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

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/storage"
)

func (r *replica) createSnapshot() error {
	ss, ssenv, err := r.snapshotter.save(r.sm.dataStorage)
	if err != nil {
		if errors.Is(err, storage.ErrAborted) {
			r.logger.Info("snapshot aborted")
			ssenv.MustRemoveTempDir()
			return nil
		}
		return err
	}
	if err := r.snapshotter.commit(ss); err != nil {
		if errors.Is(err, errSnapshotOutOfDate) {
			// the snapshot final dir already exist on disk
			r.logger.Info("aborted committing an out of date snapshot",
				log.SnapshotField(ss))
			ssenv.MustRemoveTempDir()
			return nil
		}
		return err
	}
	if err := r.lr.CreateSnapshot(ss); err != nil {
		if errors.Is(err, raft.ErrSnapOutOfDate) {
			// lr already has a more recent snapshot
			r.logger.Info("aborted registering an out of date snapshot",
				log.SnapshotField(ss))
			return nil
		}
	}
	// TODO: schedule log compacton here
	return nil
}
