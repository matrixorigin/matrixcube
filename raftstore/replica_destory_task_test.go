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

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

type testDestoryMetadataStorage struct {
	sync.Mutex
	data     map[uint64]*metapb.DestroyingStatus
	c        chan struct{}
	watchPut bool
}

func newTestDestoryMetadataStorage(watchPut bool) *testDestoryMetadataStorage {
	return &testDestoryMetadataStorage{
		data:     make(map[uint64]*metapb.DestroyingStatus),
		c:        make(chan struct{}),
		watchPut: watchPut,
	}
}

func (s *testDestoryMetadataStorage) CreateDestroying(id uint64, index uint64, removeData bool, replicas []uint64) (metapb.ResourceState, error) {
	s.Lock()
	defer s.Unlock()

	status := &metapb.DestroyingStatus{
		Index:      index,
		RemoveData: removeData,
		State:      metapb.ResourceState_Destroying,
		Replicas:   make(map[uint64]bool),
	}
	for _, id := range replicas {
		status.Replicas[id] = false
	}

	s.data[id] = status
	if s.watchPut {
		s.c <- struct{}{}
	}
	return metapb.ResourceState_Destroying, nil
}

func (s *testDestoryMetadataStorage) GetDestroying(id uint64) (*metapb.DestroyingStatus, error) {
	s.Lock()
	defer s.Unlock()

	return s.data[id], nil
}

func (s *testDestoryMetadataStorage) ReportDestroyed(id uint64, replicaID uint64) (metapb.ResourceState, error) {
	s.Lock()
	defer s.Unlock()

	status, ok := s.data[id]
	if !ok {
		return metapb.ResourceState_Destroying, nil
	}

	status.Replicas[replicaID] = true

	n := 0
	for _, v := range status.Replicas {
		if v {
			n++
		}
	}
	if n == len(status.Replicas) {
		status.State = metapb.ResourceState_Destroyed
	}

	return status.State, nil
}

func TestDestoryReplicaAfterLogAppliedStep1(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	pr := newTestReplica(Shard{ID: 1}, Replica{ID: 1}, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dms := newTestDestoryMetadataStorage(false)
	c := make(chan []uint64)
	go pr.destoryReplicaAfterLogApplied(ctx, 100, func(a action) {
		if a.actionType == checkLogCommittedAction {
			assert.NotNil(t, a.actionCallback)
			c <- []uint64{1, 2, 3}
		}
	}, dms, false, "TestDestoryReplicaAfterLogAppliedStep1", time.Millisecond*10)

	select {
	case <-c:
		break
	case <-time.After(time.Second * 100):
		assert.Fail(t, "timeout")
	}
}

func TestDestoryReplicaAfterLogAppliedStep2(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	pr := newTestReplica(Shard{ID: 1}, Replica{ID: 1}, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dms := newTestDestoryMetadataStorage(true)
	go pr.destoryReplicaAfterLogApplied(ctx, 100, func(a action) {
		if a.actionType == checkLogCommittedAction {
			assert.NotNil(t, a.actionCallback)
			go a.actionCallback([]uint64{1, 2, 3})
		}
	}, dms, false, "TestDestoryReplicaAfterLogAppliedStep2", time.Millisecond*10)

	select {
	case <-dms.c:
		v, err := dms.GetDestroying(pr.shardID)
		assert.NoError(t, err)
		assert.Equal(t, uint64(100), v.Index)
		assert.Equal(t, 3, len(v.Replicas))
	case <-time.After(time.Second * 100):
		assert.Fail(t, "timeout")
	}
}

func TestDestoryReplicaAfterLogAppliedStep3(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	pr := newTestReplica(Shard{ID: 1}, Replica{ID: 1}, s)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := make(chan struct{})
	dms := newTestDestoryMetadataStorage(false)
	dms.CreateDestroying(pr.shardID, 100, false, []uint64{1, 2, 3})
	go pr.destoryReplicaAfterLogApplied(ctx, 100, func(a action) {
		if a.actionType == checkLogAppliedAction {
			assert.NotNil(t, a.actionCallback)
			c <- struct{}{}
		}
	}, dms, false, "TestDestoryReplicaAfterLogAppliedStep3", time.Millisecond*10)

	select {
	case <-c:
		break
	case <-time.After(time.Second * 100):
		assert.Fail(t, "timeout")
	}
}

func TestDestoryReplicaAfterLogAppliedStep4(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	pr := newTestReplica(Shard{ID: 1}, Replica{ID: 1}, s)
	s.addReplica(pr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := make(chan struct{})
	dms := newTestDestoryMetadataStorage(false)
	dms.CreateDestroying(pr.shardID, 100, false, []uint64{1, 2, 3})
	go func() {
		pr.destoryReplicaAfterLogApplied(ctx, 100, func(a action) {
			if a.actionType == checkLogAppliedAction {
				go a.actionCallback(nil)
			}
		}, dms, false, "TestDestoryReplicaAfterLogAppliedStep4", time.Millisecond*10)
		close(c)
	}()

	select {
	case <-c:
		v, err := dms.GetDestroying(pr.shardID)
		assert.NoError(t, err)
		assert.Equal(t, uint64(100), v.Index)
		assert.True(t, v.Replicas[1])
	case <-time.After(time.Second * 100):
		assert.Fail(t, "timeout")
	}
}
