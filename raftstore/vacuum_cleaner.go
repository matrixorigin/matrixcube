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
	"sync"

	"github.com/lni/goutils/syncutil"
)

type vacuumFunc = func(vacuumTask) error

type vacuumTask struct {
	shard        Shard
	replica      *replica
	shardRemoved bool
	reason       string
}

// vacuumCleaner is used to cleanup shard data belongs to shards that have been
// destroyed.
type vacuumCleaner struct {
	stopper *syncutil.Stopper
	notifyC chan struct{}
	vf      vacuumFunc

	mu struct {
		sync.Mutex
		pending []vacuumTask
	}
}

func newVacuumCleaner(f vacuumFunc) *vacuumCleaner {
	return &vacuumCleaner{
		stopper: syncutil.NewStopper(),
		notifyC: make(chan struct{}, 1),
		vf:      f,
	}
}

func (v *vacuumCleaner) start() {
	v.stopper.RunWorker(func() {
		for {
			select {
			case <-v.stopper.ShouldStop():
				return
			case <-v.notifyC:
				if v.vacuum() {
					return
				}
			}
		}
	})
}

func (v *vacuumCleaner) close() {
	v.stopper.Stop()
}

func (v *vacuumCleaner) addTask(t vacuumTask) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.mu.pending = append(v.mu.pending, t)
	select {
	case v.notifyC <- struct{}{}:
	default:
	}
}

func (v *vacuumCleaner) getTasks() []vacuumTask {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.mu.pending) > 0 {
		tasks := v.mu.pending
		v.mu.pending = nil
		return tasks
	}
	return nil
}

// vacuum returns a boolean value indicating whether the vacuum cleaner should
// stop. This is to prevent long delays to close the vacuum cleaner when
// processing large number of vacuum tasks.
func (v *vacuumCleaner) vacuum() bool {
	for {
		if tasks := v.getTasks(); len(tasks) > 0 {
			for _, task := range tasks {
				if err := v.vf(task); err != nil {
					panic(err)
				}
				select {
				case <-v.stopper.ShouldStop():
					return true
				default:
				}
			}
		} else {
			break
		}
	}
	return false
}
