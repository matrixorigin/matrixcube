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

package task

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTask(t *testing.T) {
	runner := NewRunner()
	c := make(chan struct{})
	defer close(c)

	err := runner.RunTask(func() {
		c <- struct{}{}
	})

	if err != nil {
		t.Error("run task failed, return a error", err)
		return
	}

	select {
	case <-c:
	case <-time.After(time.Millisecond * 50):
		t.Error("run task failed, task not run after 50ms")
	}

	runner.AddNamedWorker("name-0", func() {})
	yes := false
	var ok int32
	complete := make(chan struct{}, 1)
	runner.RunJobWithNamedWorker("", "name-0", func() error {
		atomic.StoreInt32(&ok, 1)
		return nil
	})

	runner.RunJobWithNamedWorker("", "name-0", func() error {
		if atomic.LoadInt32(&ok) > 0 {
			yes = true
		}
		complete <- struct{}{}
		return nil
	})

	<-complete
	if !yes {
		t.Error("run named task failed, task not liner excution")
	}

	start := make(chan struct{}, 1)
	defer close(start)

	result := false

	_, err = runner.RunCancelableTask("1", func(c context.Context) {
		select {
		case <-c.Done():
			result = true
		case <-start:

		}
	})

	if err != nil {
		t.Error("run cancelable task failed, return a error", err)
		return
	}

	defaultWaitStoppedTimeout = time.Second
	_, err = runner.Stop()
	if err != nil {
		t.Error("stop runner failed, return a error", err)
		return
	}

	start <- struct{}{}
	if !result {
		t.Error("cancelable task excuted after stop")
		return
	}
}

func TestNamedTask(t *testing.T) {
	runner := NewRunner()
	runner.AddNamedWorker("apply", func() {})

	cnt := 0
	complete := make(chan struct{}, 1)
	runner.RunJobWithNamedWorker("", "apply", func() error {
		cnt++
		complete <- struct{}{}
		return nil
	})

	<-complete
	if cnt != 1 {
		t.Errorf("run named job failed. expect=<%d>, actual=<%d>", 1, cnt)
	}

	runner.RunJobWithNamedWorker("", "apply", func() error {
		cnt++
		complete <- struct{}{}
		return nil
	})

	<-complete
	if cnt != 2 {
		t.Errorf("run named job failed. expect=<%d>, actual=<%d>", 2, cnt)
	}
}

func TestStopWithID(t *testing.T) {
	runner := NewRunner()
	c := make(chan struct{})
	defer close(c)

	result := false
	start := make(chan struct{}, 1)
	defer close(start)

	id, err := runner.RunCancelableTask("1", func(c context.Context) {
		select {
		case <-c.Done():
			result = true
		case <-start:

		}
	})
	if err != nil {
		t.Error("run cancelable task failed, return a error", err)
		return
	}

	err = runner.StopCancelableTask(id)
	if err != nil {
		t.Error("stop cancelable task failed, return a error", err)
		return
	}

	for index := 0; index < 10; index++ {
		runner.RunCancelableTask(fmt.Sprintf("w-%d", index), func(c context.Context) {
			select {
			case <-c.Done():
				result = true
			case <-time.After(time.Hour):

			}
		})
	}

	defaultWaitStoppedTimeout = time.Second
	_, err = runner.Stop()
	if err != nil {
		t.Error("stop runner failed, return a error", err)
		return
	}

	start <- struct{}{}
	if !result {
		t.Error("cancelable task excuted after stop")
		return
	}
}

func TestStopWithTimeoutWorkers(t *testing.T) {
	runner := NewRunner()
	_, err := runner.AddNamedWorker("w-0", nil)
	assert.NoError(t, err)
	_, err = runner.AddNamedWorker("w-1", nil)
	assert.NoError(t, err)
	_, err = runner.AddNamedWorker("w-2", nil)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(4)
	runner.RunJobWithNamedWorker("", "w-0", func() error {
		wg.Done()
		time.Sleep(time.Second * 10)
		return nil
	})
	runner.RunJobWithNamedWorker("", "w-1", func() error {
		wg.Done()
		time.Sleep(time.Second * 10)
		return nil
	})
	runner.RunJobWithNamedWorker("", "w-2", func() error {
		wg.Done()
		return nil
	})
	runner.RunCancelableTask("w-3", func(ctx context.Context) {
		wg.Done()
		time.Sleep(time.Second * 10)
	})

	wg.Wait()
	timeoutWorkers, err := runner.doStop(time.Second)
	assert.Error(t, err)
	assert.Equal(t, 3, len(timeoutWorkers))
	sort.Strings(timeoutWorkers)
	assert.Equal(t, "w-0", timeoutWorkers[0])
	assert.Equal(t, "w-1", timeoutWorkers[1])
	assert.Equal(t, "w-3", timeoutWorkers[2])
}
