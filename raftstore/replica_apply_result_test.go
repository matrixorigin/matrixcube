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
	"testing"

	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/storage"
	skv "github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestApplySplit(t *testing.T) {
	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	pr := newTestReplica(Shard{ID: 1, Start: []byte{1}, End: []byte{10}}, Replica{ID: 100}, s)

	s.updateShardKeyRange(0, pr.getShard())
	pr.sm.dataStorage.GetInitialStates()
	pr.stats.approximateSize = 200
	pr.stats.approximateKeys = 200

	kv := pr.sm.dataStorage.(storage.KVStorageWrapper).GetKVStorage()
	assert.NoError(t, kv.Set(skv.EncodeDataKey([]byte{1}, nil), []byte("v"), false))

	result := &splitResult{
		newShards: []Shard{
			{ID: 2, Start: []byte{1}, End: []byte{5}, Replicas: []Replica{{ID: 200}}},
			{ID: 3, Start: []byte{5}, End: []byte{10}, Replicas: []Replica{{ID: 300}}},
		},
	}
	s.droppedVoteMsgs.Store(uint64(2), meta.RaftMessage{})
	s.droppedVoteMsgs.Store(uint64(3), meta.RaftMessage{})

	pr.destoryTaskFactory = newTestDestroyReplicaTaskFactory(true)
	pr.applySplit(result)
	_, ok := s.droppedVoteMsgs.Load(uint64(2))
	assert.False(t, ok)
	_, ok = s.droppedVoteMsgs.Load(uint64(3))
	assert.False(t, ok)

	r, ok := s.getReplicaRecord(200)
	assert.True(t, ok)
	assert.Equal(t, Replica{ID: 200}, r)
	r, ok = s.getReplicaRecord(300)
	assert.True(t, ok)
	assert.Equal(t, Replica{ID: 300}, r)

	pr = s.getReplica(2, false)
	assert.NotNil(t, pr)
	assert.Equal(t, uint64(100), pr.stats.approximateSize)
	assert.Equal(t, uint64(100), pr.stats.approximateKeys)
	assert.Equal(t, int64(1), pr.messages.Len())

	pr = s.getReplica(3, false)
	assert.NotNil(t, pr)
	assert.Equal(t, uint64(100), pr.stats.approximateSize)
	assert.Equal(t, uint64(100), pr.stats.approximateKeys)
	assert.Equal(t, int64(1), pr.messages.Len())

	pr, err := s.selectShard(0, []byte{1})
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), pr.getShard().ID)

	pr, err = s.selectShard(0, []byte{5})
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), pr.getShard().ID)

	s.vacuumCleaner.vacuum()

	pr = s.getReplica(1, false)
	assert.Nil(t, pr)
	v, err := kv.Get(skv.EncodeDataKey([]byte{1}, nil))
	assert.NoError(t, err)
	assert.NotEmpty(t, v)
}

func TestUpdateMetricsHints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	pr := newTestReplica(Shard{ID: 1}, Replica{ID: 1}, s)

	pr.updateMetricsHints(applyResult{metrics: applyMetrics{approximateDiffHint: 1, deleteKeysHint: 1, writtenBytes: 1, writtenKeys: 1}})
	assert.Equal(t, uint64(1), pr.stats.approximateSize)
	assert.Equal(t, uint64(1), pr.stats.deleteKeysHint)
	assert.Equal(t, uint64(1), pr.stats.writtenBytes)
	assert.Equal(t, uint64(1), pr.stats.writtenKeys)

	pr.updateMetricsHints(applyResult{metrics: applyMetrics{approximateDiffHint: 3, deleteKeysHint: 1, writtenBytes: 1, writtenKeys: 1}})
	assert.Equal(t, uint64(3), pr.stats.approximateSize)
	assert.Equal(t, uint64(2), pr.stats.deleteKeysHint)
	assert.Equal(t, uint64(2), pr.stats.writtenBytes)
	assert.Equal(t, uint64(2), pr.stats.writtenKeys)

	pr.updateMetricsHints(applyResult{})
	assert.Equal(t, uint64(3), pr.stats.approximateSize)
	assert.Equal(t, uint64(2), pr.stats.deleteKeysHint)
	assert.Equal(t, uint64(2), pr.stats.writtenBytes)
	assert.Equal(t, uint64(2), pr.stats.writtenKeys)

	pr.updateMetricsHints(applyResult{adminResult: &adminResult{splitResult: &splitResult{}}})
	assert.Equal(t, uint64(0), pr.stats.approximateSize)
	assert.Equal(t, uint64(0), pr.stats.deleteKeysHint)
	assert.Equal(t, uint64(2), pr.stats.writtenBytes)
	assert.Equal(t, uint64(2), pr.stats.writtenKeys)
}
