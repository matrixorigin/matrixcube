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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/matrixorigin/matrixcube/util/leaktest"
)

func TestVacuumCleanerCanBeStartedAndClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	vc := newVacuumCleaner(nil)
	vc.start()
	vc.close()
}

type testVacuumTaskProcessor struct {
	mu     sync.Mutex
	shards []Shard
}

func (t *testVacuumTaskProcessor) vacuum(vt vacuumTask) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.shards = append(t.shards, vt.shard)
	return nil
}

func (t *testVacuumTaskProcessor) getProcessedCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.shards)
}

func TestVacuumCanProcessTasks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	p := &testVacuumTaskProcessor{}
	vc := newVacuumCleaner(p.vacuum)
	vc.start()
	defer vc.close()
	shard1 := Shard{ID: 1}
	shard2 := Shard{ID: 2}
	shard3 := Shard{ID: 3}
	vc.addTask(vacuumTask{shard: shard1})
	vc.addTask(vacuumTask{shard: shard2})
	vc.addTask(vacuumTask{shard: shard3})
	for {
		if p.getProcessedCount() == 3 {
			break
		}
	}

	assert.Equal(t, []Shard{shard1, shard2, shard3}, p.shards)
	assert.Equal(t, 0, len(vc.mu.pending))
}

func TestVacuumMethodWillPanicOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	panicFunc := func(vacuumTask) error {
		panic("panic now")
	}
	vc := newVacuumCleaner(panicFunc)
	{
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("failed to trigger panic")
			}
		}()
		vc.mu.pending = append(vc.mu.pending, vacuumTask{})
		vc.vacuum()
	}
}
