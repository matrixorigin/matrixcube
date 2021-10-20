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

	"github.com/matrixorigin/matrixcube/storage"
	"github.com/stretchr/testify/assert"
)

func TestLoadPersistentLogIndex(t *testing.T) {
	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	ds := s.DataStorageByGroup(0)
	ds.GetInitialStates()

	pr, err := newReplica(s, Shard{ID: 1}, Replica{ID: 1000}, "test")
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), pr.appliedIndex)

	err = ds.SaveShardMetadata([]storage.ShardMetadata{{ShardID: 2, LogIndex: 2, Metadata: make([]byte, 10)}})
	assert.NoError(t, err)
	ds.Sync([]uint64{2})
	pr, err = newReplica(s, Shard{ID: 2}, Replica{ID: 2000}, "test")
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), pr.appliedIndex)
}
