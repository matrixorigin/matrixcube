package raftstore

import (
	"testing"

	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3/raftpb"
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
	newReplicaCreator(s).
		withReason("TestShardCreateWithSaveMetadata").
		withStartReplica(false, nil, func(r *replica) {
			pr = r
		}).
		create([]Shard{
			db.CreateShard(1, "1/0"),
		})
	assert.NotNil(t, s.getReplica(1, false))
	assert.NotNil(t, pr)
	assert.Equal(t, db.CreateShard(1, "1/0"), pr.getShard())

	_, err := s.logdb.ReadRaftState(1, s.getReplica(1, false).replicaID, 0)
	assert.Equal(t, logdb.ErrNoSavedLog, err)
}

func testShardCreateWithSaveMetadataWithSync(t *testing.T, sync bool) {
	defer leaktest.AfterTest(t)()
	s, close := newTestStore(t)
	defer close()

	s.meta.SetID(100)
	db := NewTestDataBuilder()
	f := newReplicaCreator(s)
	f.withReason("TestShardCreateWithSaveMetadata").
		withSaveMetadata(sync).
		create([]Shard{
			db.CreateShard(1, "1/10/v/t,2/100/v/t"),
		})

	assert.Equal(t, 1, len(f.getShardsMetadata()))
	assert.Equal(t, meta.ShardMetadata{
		ShardID:  1,
		LogIndex: 1,
		Metadata: meta.ShardLocalState{
			State: meta.ReplicaState_Normal,
			Shard: db.CreateShard(1, "1/10/v/t,2/100/v/t"),
		},
	}, f.getShardsMetadata()[0])
	stats, err := s.DataStorageByGroup(0).GetInitialStates()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(stats))
	assert.Equal(t, uint64(1), stats[0].LogIndex)
	assert.Equal(t, uint64(1), stats[0].ShardID)
	assert.Equal(t, meta.ReplicaState_Normal, stats[0].Metadata.State)

	if sync {
		assert.True(t, s.DataStorageByGroup(0).(storage.StatsKeeper).Stats().SyncCount > 0)
	} else {
		assert.True(t, s.DataStorageByGroup(0).(storage.StatsKeeper).Stats().SyncCount == 0)
	}

	stat, err := s.logdb.ReadRaftState(1, 2, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), stat.FirstIndex)
	assert.Equal(t, uint64(1), stat.EntryCount)
	assert.Equal(t, raftpb.HardState{Commit: 1, Term: 1}, stat.State)
}
