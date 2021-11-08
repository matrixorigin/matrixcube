package raftstore

import (
	"testing"

	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestShardCreateWithSaveMetadata(t *testing.T) {
	testShardCreateWithSaveMetadataWithSync(t, false)
	testShardCreateWithSaveMetadataWithSync(t, true)
}

func TestShardCreateWithStart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, close := newTestStore(t)
	defer close()

	var pr *replica
	db := NewTestDataBuilder()
	newShardCreator(s).
		withReason("TestShardCreateWithSaveMetadata").
		withStartReplica(func(r *replica) {
			pr = r
		}).
		create([]Shard{
			db.CreateShard(1, "1/0"),
		})
	assert.NotNil(t, s.getReplica(1, false))
	assert.NotNil(t, pr)
	assert.Equal(t, db.CreateShard(1, "1/0"), pr.getShard())
}

func testShardCreateWithSaveMetadataWithSync(t *testing.T, sync bool) {
	defer leaktest.AfterTest(t)()
	s, close := newTestStore(t)
	defer close()

	db := NewTestDataBuilder()
	newShardCreator(s).
		withReason("TestShardCreateWithSaveMetadata").
		withSaveMetadata(sync).
		create([]Shard{
			db.CreateShard(1, "1/10"),
		})
	stats, err := s.DataStorageByGroup(0).GetInitialStates()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(stats))
	assert.Equal(t, uint64(1), stats[0].LogIndex)
	assert.Equal(t, uint64(1), stats[0].LogTerm)
	assert.Equal(t, uint64(1), stats[0].ShardID)
	assert.Equal(t, meta.ReplicaState_Normal, stats[0].Metadata.State)
	if sync {
		assert.True(t, s.DataStorageByGroup(0).(storage.StatsKeeper).Stats().SyncCount > 0)
	} else {
		assert.True(t, s.DataStorageByGroup(0).(storage.StatsKeeper).Stats().SyncCount == 0)
	}
}
