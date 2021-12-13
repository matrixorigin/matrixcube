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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/util/stop"
)

type testReplicaGetter struct {
	sync.RWMutex
	replicas map[uint64]*replica
}

func newTestReplicaGetter() *testReplicaGetter {
	return &testReplicaGetter{
		replicas: make(map[uint64]*replica),
	}
}

func (trg *testReplicaGetter) getReplica(id uint64) (*replica, bool) {
	trg.RLock()
	defer trg.RUnlock()

	v, ok := trg.replicas[id]
	return v, ok
}

func TestInitAppliedIndex(t *testing.T) {
	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	ds := s.DataStorageByGroup(0)
	ds.GetInitialStates()

	pr, err := newReplica(s, Shard{ID: 1}, Replica{ID: 1000}, "test")
	assert.NoError(t, err)
	assert.NoError(t, pr.initAppliedIndex(ds))
	assert.Equal(t, uint64(0), pr.appliedIndex)

	err = ds.SaveShardMetadata([]meta.ShardMetadata{
		{
			ShardID:  2,
			LogIndex: 2,
			Metadata: meta.ShardLocalState{
				Shard: meta.Shard{
					ID: 2,
				},
			},
		},
	})
	rd := raft.Ready{
		Snapshot: raftpb.Snapshot{
			Metadata: raftpb.SnapshotMetadata{
				Index: 2,
				Term:  100,
			},
		},
	}
	assert.NoError(t, pr.logdb.SaveRaftState(2, 2000, rd, pr.logdb.NewWorkerContext()))
	assert.NoError(t, err)
	ds.Sync([]uint64{2})
	pr, err = newReplica(s, Shard{ID: 2}, Replica{ID: 2000}, "test")
	assert.NoError(t, err)
	assert.NoError(t, pr.initAppliedIndex(ds))
	assert.Equal(t, uint64(2), pr.appliedIndex)
}

func newTestReplica(shard Shard, peer Replica, s *store) *replica {
	pr, _ := newReplica(s, shard, peer, "testing")
	pr.readStopper = stop.NewStopper("test")
	pr.setStarted()
	return pr
}

func TestGetLogReaderMarkerState(t *testing.T) {
	tests := []struct {
		snapshotIndex      uint64
		snapshotTerm       uint64
		hasSnapshotRec     bool
		persistentLogIndex uint64
		persistentLogTerm  uint64
		hasLogEntry        bool
		resultIndex        uint64
		resultTerm         uint64
		err                error
	}{
		{0, 0, false, 0, 0, false, 0, 0, nil},
		{100, 2, true, 50, 1, true, 100, 2, nil},
		{100, 2, true, 0, 0, false, 100, 2, nil},
		{100, 2, true, 100, 2, true, 100, 2, nil},
		{100, 2, true, 200, 3, true, 200, 3, nil},
		{0, 0, false, 50, 1, true, 50, 1, nil},
		{0, 0, false, 50, 1, false, 0, 0, raft.ErrUnavailable},
	}

	shardID := uint64(1)
	replicaID := uint64(1)
	for _, tt := range tests {
		func() {
			logger := log.GetDefaultZapLogger()
			memKV := mem.NewStorage()
			defer memKV.Close()
			logdb := logdb.NewKVLogDB(memKV, logger)
			if tt.snapshotIndex > 0 && tt.hasSnapshotRec {
				rd := raft.Ready{
					Snapshot: raftpb.Snapshot{
						Metadata: raftpb.SnapshotMetadata{
							Index: tt.snapshotIndex,
							Term:  tt.snapshotTerm,
						},
					},
				}
				assert.NoError(t, logdb.SaveRaftState(1, 1, rd, logdb.NewWorkerContext()))
			}
			if tt.persistentLogIndex > 0 && tt.hasLogEntry {
				rd := raft.Ready{
					Entries: []raftpb.Entry{{Index: tt.persistentLogIndex, Term: tt.persistentLogTerm}},
				}
				assert.NoError(t, logdb.SaveRaftState(1, 1, rd, logdb.NewWorkerContext()))
			}
			ds := &testDataStorage{
				persistentLogIndex: tt.persistentLogIndex,
			}
			pr := &replica{
				logdb:     logdb,
				shardID:   shardID,
				replicaID: replicaID,
				sm: &stateMachine{
					dataStorage: ds,
				},
			}
			ss, err := pr.getLogReaderMarkerState()
			assert.Equal(t, tt.err, err)
			assert.Equal(t, tt.resultIndex, ss.Metadata.Index)
			assert.Equal(t, tt.resultTerm, ss.Metadata.Term)
		}()
	}
}
