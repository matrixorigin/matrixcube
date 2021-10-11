// Copyright 2022 MatrixOrigin.
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestWorkerPoolCanBeCreatedAndClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newWorkerPool(nil, nil, 32)
	p.start()
	p.close()
}

type testReplicaEventHandler struct {
	handled uint64
	shardID uint64
	invoked chan struct{}
	waitC   chan struct{}
}

func (t *testReplicaEventHandler) enableWait() {
	t.invoked = make(chan struct{})
	t.waitC = make(chan struct{})
}

func (t *testReplicaEventHandler) getShardID() uint64 {
	return t.shardID
}

func (t *testReplicaEventHandler) handleEvent() bool {
	if t.invoked != nil {
		close(t.invoked)
		<-t.waitC
	}
	atomic.StoreUint64(&t.handled, 1)
	return false
}

func (t *testReplicaEventHandler) getHandled() bool {
	return atomic.LoadUint64(&t.handled) == 1
}

type testReplicaLoader struct {
	handlers map[uint64]*testReplicaEventHandler
}

func newTestReplicaLoader() *testReplicaLoader {
	return &testReplicaLoader{
		handlers: make(map[uint64]*testReplicaEventHandler),
	}
}

func (f *testReplicaLoader) getReplica(r uint64) (replicaEventHandler, bool) {
	h, ok := f.handlers[r]
	if !ok {
		h = &testReplicaEventHandler{shardID: r}
		f.handlers[r] = h
	}
	return h, true
}

func TestWorkerPoolCanScheduleSimpleJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	l := newTestReplicaLoader()
	h, _ := l.getReplica(10)
	p := newWorkerPool(nil, l, 32)
	p.start()
	defer func() {
		p.close()
		assert.Equal(t, 0, len(p.pending))
		assert.Equal(t, 0, len(p.busy))
	}()
	p.notify(h.getShardID())
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-timer.C:
		t.Errorf("failed t schedule the job")
	default:
		if h.(*testReplicaEventHandler).getHandled() {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWorkerPoolWillNotReturnBusyWorker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newWorkerPool(nil, nil, 32)
	p.start()
	defer p.close()
	assert.Equal(t, 32, len(p.workers))
	assert.Equal(t, 0, len(p.busy))
	w := p.getWorker()
	assert.NotNil(t, w)

	for _, w := range p.workers {
		p.busy[w.workerID] = nil
	}
	w = p.getWorker()
	assert.Nil(t, w)
}

func TestWorkerPoolScheduleNothingWhenNotPendingJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newWorkerPool(nil, nil, 32)
	p.start()
	defer p.close()
	assert.False(t, p.scheduleWorker())
	w := p.getWorker()
	assert.NotNil(t, w)
}

func TestWorkerPoolScheduleNothingWhenNoIdleWorker(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newWorkerPool(nil, nil, 32)
	p.start()
	defer p.close()
	p.pending[20] = nil
	for _, w := range p.workers {
		p.busy[w.workerID] = nil
	}
	w := p.getWorker()
	assert.Nil(t, w)
	assert.False(t, p.scheduleWorker())
}

func TestWorkerPoolWillNotConcurrentlyProcessTheSameShard(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := newWorkerPool(nil, nil, 32)
	p.start()
	defer p.close()
	p.pending[10] = nil
	p.processing[10] = struct{}{}
	assert.False(t, p.canSchedule(&testReplicaEventHandler{shardID: 10}))
}

func TestWorkerPoolSetBusyAndProcessingAsExpected(t *testing.T) {
	defer leaktest.AfterTest(t)()
	l := newTestReplicaLoader()
	p := newWorkerPool(nil, l, 32)
	p.start()
	defer func() {
		p.close()
		assert.Equal(t, 0, len(p.busy))
		assert.Equal(t, 0, len(p.processing))
	}()
	h, _ := l.getReplica(10)
	h.(*testReplicaEventHandler).enableWait()
	p.notify(h.getShardID())
	<-h.(*testReplicaEventHandler).invoked
	assert.Equal(t, 1, len(p.busy))
	assert.Equal(t, 1, len(p.processing))
	close(h.(*testReplicaEventHandler).waitC)
}

func testWorkerPoolConcurrentJobs(t *testing.T, moreJob bool) {
	defer leaktest.AfterTest(t)()
	l := newTestReplicaLoader()
	p := newWorkerPool(nil, l, 32)
	p.start()
	defer func() {
		p.close()
		assert.Equal(t, 0, len(p.busy))
		assert.Equal(t, 0, len(p.processing))
	}()
	var handlers []*testReplicaEventHandler
	for i := uint64(1); i < uint64(33); i++ {
		h, _ := l.getReplica(i)
		h.(*testReplicaEventHandler).enableWait()
		handlers = append(handlers, h.(*testReplicaEventHandler))
		p.notify(h.getShardID())
		<-h.(*testReplicaEventHandler).invoked
	}
	assert.Equal(t, 32, len(p.busy))
	assert.Equal(t, 32, len(p.processing))

	if moreJob {
		p.notify(64)
		p.notify(65)
		count := 0
		p.ready.Range(func(k, v interface{}) bool {
			count++
			return true
		})
		assert.Equal(t, 2, count)
	}

	for _, h := range handlers {
		close(h.waitC)
	}
}

func TestWorkerPoolCanConcurrentlyProcessMultipleJobs(t *testing.T) {
	testWorkerPoolConcurrentJobs(t, false)
}

func TestWorkerPoolWillNotBlockCallToNotify(t *testing.T) {
	testWorkerPoolConcurrentJobs(t, true)
}
