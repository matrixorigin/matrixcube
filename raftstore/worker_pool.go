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
	"reflect"
	"sync"

	"github.com/lni/goutils/syncutil"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/logdb"
	"github.com/matrixorigin/matrixcube/storage"
)

type replicaLoader interface {
	getReplica(uint64) (replicaEventHandler, bool)
}

type storeReplicaLoader struct {
	store *store
}

func (s *storeReplicaLoader) getReplica(shardID uint64) (replicaEventHandler, bool) {
	if r := s.store.getReplica(shardID, false); r != nil {
		return r, true
	}
	return nil, false
}

var _ replicaLoader = (*storeReplicaLoader)(nil)

type replicaEventHandler interface {
	getShardID() uint64
	handleEvent(*logdb.WorkerContext) bool
}

var _ replicaEventHandler = (*replica)(nil)

// replicaWorker is the worker type that actually processes replica raft updates
type replicaWorker struct {
	stopper    *syncutil.Stopper
	wc         *logdb.WorkerContext
	requestC   chan replicaEventHandler
	completedC chan struct{}
	workerID   uint64
}

func newReplicaWorker(workerID uint64,
	stopper *syncutil.Stopper, wc *logdb.WorkerContext) *replicaWorker {
	w := &replicaWorker{
		workerID:   workerID,
		stopper:    stopper,
		requestC:   make(chan replicaEventHandler, 1),
		completedC: make(chan struct{}, 1),
		wc:         wc,
	}
	stopper.RunWorker(func() {
		w.workerMain()
	})
	return w
}

func (w *replicaWorker) workerMain() {
	for {
		select {
		case <-w.stopper.ShouldStop():
			return
		case h := <-w.requestC:
			w.wc.Reset()
			if err := w.handleEvent(h); err != nil {
				panic(err)
			}
			w.completed()
		}
	}
}

func (w *replicaWorker) schedule(h replicaEventHandler) {
	select {
	case w.requestC <- h:
	default:
		panic("trying to schedule to a busy worker")
	}
}

func (w *replicaWorker) completed() {
	w.completedC <- struct{}{}
}

func (w *replicaWorker) handleEvent(h replicaEventHandler) error {
	for {
		if !h.handleEvent(w.wc) {
			break
		}
	}
	return nil
}

// workerPool manages a pool of workers that are used to process all raft
// related updates for all replicas. A dispatcher goroutine is used to
// coordinate all workers, while workers can independently working on different
// raft replicas. Due to restrictions imposed by the raft implementation, the
// same raft replica can not be concurrently processed by different workers.
type workerPool struct {
	logger  *zap.Logger
	loader  replicaLoader
	workers []*replicaWorker
	// workerID -> replica
	busy map[uint64]replicaEventHandler
	// shardID -> replica
	pending map[uint64]replicaEventHandler
	// shardID -> struct{}{}
	processing map[uint64]struct{}
	// shardID -> struct{}{}
	ready         sync.Map
	readyC        chan struct{}
	workerStopper *syncutil.Stopper
	poolStopper   *syncutil.Stopper
}

func newWorkerPool(logger *zap.Logger,
	wc storage.WriteBatchCreator, loader replicaLoader, workerCount uint64) *workerPool {
	p := &workerPool{
		logger:        log.Adjust(logger).Named("worker-pool"),
		loader:        loader,
		busy:          make(map[uint64]replicaEventHandler),
		pending:       make(map[uint64]replicaEventHandler),
		processing:    make(map[uint64]struct{}),
		readyC:        make(chan struct{}, 1),
		workerStopper: syncutil.NewStopper(),
		poolStopper:   syncutil.NewStopper(),
	}
	for workerID := uint64(0); workerID < workerCount; workerID++ {
		workerContext := logdb.NewWorkerContext(wc)
		w := newReplicaWorker(workerID, p.workerStopper, workerContext)
		p.workers = append(p.workers, w)
	}

	return p
}

func (p *workerPool) start() {
	p.poolStopper.RunWorker(func() {
		p.workerPoolMain()
	})
}

func (p *workerPool) notify(shardID uint64) {
	p.ready.Store(shardID, struct{}{})
	select {
	case p.readyC <- struct{}{}:
	default:
	}
}

func (p *workerPool) close() error {
	p.poolStopper.Stop()
	return nil
}

func (p *workerPool) workerPoolMain() {
	cases := make([]reflect.SelectCase, len(p.workers)+2)
	for {
		toSchedule := false
		// 0 - pool stopper stopc
		// 1 - readyC
		// 2 - worker completeC
		cases[0] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(p.poolStopper.ShouldStop()),
		}
		cases[1] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(p.readyC),
		}
		for idx, w := range p.workers {
			cases[2+idx] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(w.completedC),
			}
		}

		chosen, _, _ := reflect.Select(cases)
		if chosen == 0 {
			p.workerStopper.Stop()
			// for testing
			for _, w := range p.workers {
				select {
				case <-w.completedC:
					p.completed(w.workerID)
				default:
				}
			}
			return
		} else if chosen == 1 {
			p.ready.Range(func(key interface{}, value interface{}) bool {
				shardID := key.(uint64)
				if h, ok := p.loader.getReplica(shardID); ok {
					p.addPending(h)
					toSchedule = true
				}
				p.ready.Delete(key)
				return true
			})
		} else if chosen >= 2 && chosen <= 2+len(p.workers)-1 {
			workerID := uint64(chosen - 2)
			toSchedule = true
			p.completed(workerID)
		} else {
			panic("unknown selected channel")
		}

		if toSchedule {
			p.schedule()
		}
	}
}

func (p *workerPool) addPending(h replicaEventHandler) {
	p.pending[h.getShardID()] = h
}

func (p *workerPool) removePending(shardID uint64) {
	delete(p.pending, shardID)
}

func (p *workerPool) completed(workerID uint64) {
	h, ok := p.busy[workerID]
	if !ok {
		p.logger.Fatal("worker is not busy", log.WorkerField(workerID))
	}
	shardID := h.getShardID()
	if _, ok := p.processing[shardID]; ok {
		delete(p.processing, shardID)
	} else {
		p.logger.Fatal("shard not marked as processing",
			log.ShardIDField(shardID))
	}
	p.setIdle(workerID)
}

func (p *workerPool) setIdle(workerID uint64) {
	if _, ok := p.busy[workerID]; ok {
		delete(p.busy, workerID)
	} else {
		p.logger.Fatal("worker not marked as busy",
			log.WorkerField(workerID))
	}
}

func (p *workerPool) setBusy(h replicaEventHandler, workerID uint64) {
	if _, ok := p.busy[workerID]; ok {
		p.logger.Fatal("trying to use a busy worker",
			log.WorkerField(workerID))
	}
	p.busy[workerID] = h
}

func (p *workerPool) startProcessing(h replicaEventHandler) {
	shardID := h.getShardID()
	if _, ok := p.processing[shardID]; ok {
		p.logger.Fatal("trying to process shard in parallel",
			log.ShardIDField(h.getShardID()))
	}
	p.processing[shardID] = struct{}{}
}

func (p *workerPool) getWorker() *replicaWorker {
	for _, w := range p.workers {
		if _, busy := p.busy[w.workerID]; !busy {
			return w
		}
	}
	return nil
}

func (p *workerPool) schedule() {
	for {
		if !p.scheduleWorker() {
			return
		}
	}
}

func (p *workerPool) canSchedule(h replicaEventHandler) bool {
	_, ok := p.processing[h.getShardID()]
	return !ok
}

func (p *workerPool) scheduleWorker() bool {
	if len(p.pending) == 0 {
		return false
	}
	if w := p.getWorker(); w != nil {
		for shardID, h := range p.pending {
			if p.canSchedule(h) {
				p.scheduleJob(h, w)
				p.removePending(shardID)
				return true
			}
		}
	}
	return false
}

func (p *workerPool) scheduleJob(h replicaEventHandler, w *replicaWorker) {
	p.setBusy(h, w.workerID)
	p.startProcessing(h)
	w.schedule(h)
}
