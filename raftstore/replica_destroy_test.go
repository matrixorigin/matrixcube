// Copyright 2020 MatrixOrigin.
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

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/storage"
	skv "github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/util/stop"
	"github.com/matrixorigin/matrixcube/util/task"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDestroyReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r := Replica{ID: 1}
	s, cancel := newTestStore(t)
	defer cancel()
	kv := s.DataStorageByGroup(0).(storage.KVStorageWrapper).GetKVStorage()
	kv.Set(skv.EncodeDataKey([]byte("a1"), nil), []byte("hello-a1"), false)
	kv.Set(skv.EncodeDataKey([]byte("a2"), nil), []byte("hello-a2"), false)

	shard := Shard{
		ID:       1,
		Start:    []byte("a"),
		End:      []byte("b"),
		Replicas: []Replica{r},
	}

	scan := func() int {
		count := 0
		kv.Scan(skv.EncodeShardStart(shard.Start, nil), skv.EncodeShardStart(shard.End, nil), func(key, value []byte) (bool, error) {
			count++
			return true, nil
		}, false)
		return count
	}

	assert.Equal(t, 2, scan())

	pr := &replica{
		shardID:           1,
		replica:           r,
		startedC:          make(chan struct{}),
		closedC:           make(chan struct{}),
		destroyedC:        make(chan struct{}),
		unloadedC:         make(chan struct{}),
		store:             s,
		logger:            s.logger,
		ticks:             task.New(32),
		messages:          task.New(32),
		requests:          task.New(32),
		actions:           task.New(32),
		feedbacks:         task.New(32),
		pendingProposals:  newPendingProposals(),
		incomingProposals: newProposalBatch(s.logger, 10, 1, r),
		pendingReads:      &readIndexQueue{shardID: 1, logger: s.logger},
		readStopper:       stop.NewStopper("TestDestroyReplica"),
	}
	pr.sm = newStateMachine(pr.logger, s.DataStorageByGroup(0), s.logdb, shard, pr.replica, nil, nil)
	s.vacuumCleaner.start()
	defer s.vacuumCleaner.close()
	close(pr.startedC)
	s.addReplica(pr)
	assert.NotNil(t, s.getReplica(1, false))
	s.destroyReplica(pr.shardID, true, true, "testing")
	for {
		if pr.closed() {
			break
		}
	}
	// when repeatedly request the the same replica to be destroyed, such requests
	// should become NOOPs.
	for i := 0; i < 100; i++ {
		s.destroyReplica(pr.shardID, true, true, "testing")
	}
	wc := s.logdb.NewWorkerContext()
	pr.handleEvent(wc)
	pr.waitDestroyed()
	assert.Nil(t, s.getReplica(1, false))
	assert.Equal(t, 0, scan())
	smd, err := pr.sm.dataStorage.GetInitialStates()
	require.NoError(t, err)
	require.Empty(t, smd)
}

func TestReplicaDestroyedState(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := replica{
		logger:     log.GetDefaultZapLogger(),
		destroyedC: make(chan struct{}, 1),
	}
	p.confirmDestroyed()
	select {
	case <-p.destroyedC:
	default:
		t.Fatalf("failed to set destroyed flag")
	}
	p.waitDestroyed()
}

func TestDestroyReplicaAndKeepShardDataAfterRestart(t *testing.T) {
	testDestroyReplicaWithRemoveShardDataAfterRestart(t, false)
}

func TestDestroyReplicaAndRemoveShardDataAfterRestart(t *testing.T) {
	testDestroyReplicaWithRemoveShardDataAfterRestart(t, true)
}

func testDestroyReplicaWithRemoveShardDataAfterRestart(t *testing.T, removeData bool) {
	defer leaktest.AfterTest(t)()

	c := NewSingleTestClusterStore(t, DiskTestCluster, WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
		cfg.Customize.CustomInitShardsFactory = func() []metapb.Shard {
			return []metapb.Shard{
				{
					Start: []byte("k1"),
					End:   []byte("k2"),
				},
				{
					Start: []byte("k2"),
					End:   []byte("k3"),
				},
			}
		}
	}))
	c.Start()
	defer c.Stop()

	c.WaitLeadersByCount(2, testWaitTimeout)
	kv := c.CreateTestKVClient(0)
	assert.NoError(t, kv.Set("k1", "v1", testWaitTimeout))
	assert.NoError(t, kv.Set("k2", "v2", testWaitTimeout))

	sid := c.GetShardByIndex(0, 0).ID
	ds := c.GetStore(0).DataStorageByGroup(0)
	assert.NoError(t, ds.Sync([]uint64{sid}))

	c.RestartWithFunc(func() {
		ds = c.GetStore(0).DataStorageByGroup(0)
		states, err := ds.GetInitialStates()
		assert.NoError(t, err)
		for _, state := range states {
			if state.ShardID == sid {
				state.Metadata.State = metapb.ReplicaState_ReplicaTombstone
				state.Metadata.RemoveData = removeData
				assert.NoError(t, ds.SaveShardMetadata([]metapb.ShardMetadata{state}))
				assert.NoError(t, ds.Sync([]uint64{sid}))
				break
			}
		}
	})
	c.WaitLeadersByCount(1, testWaitTimeout)

	c.WaitRemovedByShardID(sid, testWaitTimeout)

	s := c.GetStore(0).(*store)
	ds = s.DataStorageByGroup(0)
	assert.NoError(t, ds.Sync([]uint64{sid}))

	kvs := ds.(storage.KVStorageWrapper).GetKVStorage()
	v, err := kvs.Get(skv.EncodeDataKey([]byte("k1"), nil))
	assert.NoError(t, err)
	if !removeData {
		assert.Equal(t, "v1", string(v))
	} else {
		assert.Empty(t, v)
	}

	v, err = kvs.Get(skv.EncodeDataKey([]byte("k2"), nil))
	assert.NoError(t, err)
	assert.Equal(t, "v2", string(v))
}
