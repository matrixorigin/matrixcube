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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

type testDestoryMetadataStorage struct {
	sync.Mutex
	data     map[uint64]uint64
	c        chan struct{}
	watchPut bool
}

func newTestDestoryMetadataStorage(watchPut bool) *testDestoryMetadataStorage {
	return &testDestoryMetadataStorage{
		data:     make(map[uint64]uint64),
		c:        make(chan struct{}),
		watchPut: watchPut,
	}
}

func (s *testDestoryMetadataStorage) put(shardID, logIndex uint64) error {
	s.Lock()
	defer s.Unlock()

	s.data[shardID] = logIndex
	if s.watchPut {
		s.c <- struct{}{}
	}
	return nil
}

func (s *testDestoryMetadataStorage) get(shardID uint64) (uint64, error) {
	s.Lock()
	defer s.Unlock()

	return s.data[shardID], nil
}

func TestDestoryReplicaAfterLogAppliedStep1(t *testing.T) {
	defer leaktest.AfterTest(t)()

	old := checkerInterval
	checkerInterval = time.Millisecond * 10
	defer func() {
		checkerInterval = old
	}()

	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	pr := newTestReplica(Shard{ID: 1}, Replica{ID: 100}, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := make(chan struct{})
	go pr.destoryReplicaAfterLogApplied(ctx, 100, func(a action) {
		if a.actionType == checkLogCommittedAction {
			assert.NotNil(t, a.actionCallback)
			c <- struct{}{}
		}
	}, newTestDestoryMetadataStorage(false), false, "TestDestoryReplicaAfterLogAppliedStep1")

	select {
	case <-c:
		break
	case <-time.After(time.Second * 100):
		assert.Fail(t, "timeout")
	}
}

func TestDestoryReplicaAfterLogAppliedStep2(t *testing.T) {
	defer leaktest.AfterTest(t)()

	old := checkerInterval
	checkerInterval = time.Millisecond * 10
	defer func() {
		checkerInterval = old
	}()

	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	pr := newTestReplica(Shard{ID: 1}, Replica{ID: 100}, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dms := newTestDestoryMetadataStorage(true)
	go pr.destoryReplicaAfterLogApplied(ctx, 100, func(a action) {
		if a.actionType == checkLogCommittedAction {
			assert.NotNil(t, a.actionCallback)
			go a.actionCallback(nil)
		}
	}, dms, false, "TestDestoryReplicaAfterLogAppliedStep2")

	select {
	case <-dms.c:
		v, err := dms.get(pr.shardID)
		assert.NoError(t, err)
		assert.Equal(t, uint64(100), v)
	case <-time.After(time.Second * 100):
		assert.Fail(t, "timeout")
	}
}

func TestDestoryReplicaAfterLogAppliedStep3(t *testing.T) {
	defer leaktest.AfterTest(t)()

	old := checkerInterval
	checkerInterval = time.Millisecond * 10
	defer func() {
		checkerInterval = old
	}()

	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	pr := newTestReplica(Shard{ID: 1}, Replica{ID: 100}, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := make(chan struct{})
	dms := newTestDestoryMetadataStorage(false)
	dms.put(pr.shardID, 100)
	go pr.destoryReplicaAfterLogApplied(ctx, 100, func(a action) {
		if a.actionType == checkLogAppliedAction {
			assert.NotNil(t, a.actionCallback)
			c <- struct{}{}
		}
	}, dms, false, "TestDestoryReplicaAfterLogAppliedStep3")

	select {
	case <-c:
		break
	case <-time.After(time.Second * 100):
		assert.Fail(t, "timeout")
	}
}

func TestDestoryReplicaAfterLogAppliedStep4(t *testing.T) {
	defer leaktest.AfterTest(t)()

	old := checkerInterval
	checkerInterval = time.Millisecond * 10
	defer func() {
		checkerInterval = old
	}()

	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	pr := newTestReplica(Shard{ID: 1}, Replica{ID: 100}, s)
	s.addReplica(pr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := make(chan struct{})
	dms := newTestDestoryMetadataStorage(false)
	dms.put(pr.shardID, 100)
	go func() {
		pr.destoryReplicaAfterLogApplied(ctx, 100, func(a action) {
			if a.actionType == checkLogAppliedAction {
				go a.actionCallback(nil)
			}
		}, dms, false, "TestDestoryReplicaAfterLogAppliedStep4")
		close(c)
	}()

	select {
	case <-c:
		assert.Equal(t, 1, len(s.vacuumCleaner.mu.pending))
	case <-time.After(time.Second * 100):
		assert.Fail(t, "timeout")
	}
}
