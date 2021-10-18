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

	"github.com/fagongzi/util/task"
	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixcube/components/keys"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/util/leaktest"
)

func TestDestroyReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	r := Replica{ID: 1}
	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	kv := s.DataStorageByGroup(0).(storage.KVStorageWrapper).GetKVStorage()
	kv.Set(keys.EncodeDataKey(0, []byte("a1"), nil), []byte("hello-a1"), false)
	kv.Set(keys.EncodeDataKey(0, []byte("a2"), nil), []byte("hello-a2"), false)

	shard := Shard{
		ID:       1,
		Start:    []byte("a"),
		End:      []byte("b"),
		Replicas: []Replica{r},
	}

	scan := func() int {
		count := 0
		kv.Scan(keys.EncodeDataKey(0, shard.Start, nil),
			keys.EncodeDataKey(0, shard.End, nil),
			func(key, value []byte) (bool, error) {
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
	}
	pr.sm = newStateMachine(pr.logger,
		s.DataStorageByGroup(0), shard, pr.replica.ID, nil)
	s.vacuumCleaner.start()
	defer s.vacuumCleaner.close()
	close(pr.startedC)
	s.addReplica(pr)
	assert.NotNil(t, s.getReplica(1, false))
	s.destroyReplica(pr.shardID, true, "testing")
	for {
		if pr.closed() {
			break
		}
	}
	pr.handleEvent()
	pr.waitDestroyed()
	assert.Nil(t, s.getReplica(1, false))
	assert.Equal(t, 0, scan())
}

func TestReplicaDestroyedState(t *testing.T) {
	p := replica{
		logger:     config.GetDefaultZapLogger(),
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
