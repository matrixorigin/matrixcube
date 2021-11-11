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

package stop

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrUnavailable stopper is not running
	ErrUnavailable = errors.New("runner is unavailable")
)

var (
	defaultWaitStoppedTimeout = time.Minute
)

type state int

const (
	running  = state(0)
	stopping = state(1)
	stopped  = state(2)
)

// Stopper a stopper used to to manage all tasks that are executed in a separate goroutine,
// and Stopper can manage these goroutines centrally to avoid leaks.
// When Stopper's Stop method is called, if some tasks do not exit within the specified time,
// the names of these tasks will be returned for analysis.
type Stopper struct {
	stopC   chan struct{}
	cancels sync.Map // id -> cancelFunc
	tasks   sync.Map // id -> name

	atomic struct {
		lastID    uint64
		taskCount int64
	}

	mu struct {
		sync.RWMutex
		state state
	}
}

// NewStopper create a stopper
func NewStopper() *Stopper {
	t := &Stopper{
		stopC: make(chan struct{}),
	}

	t.mu.state = running
	return t
}

// RunTask run a task that can be cancelled. ErrUnavailable returned if stopped is not running
// See also `RunNamedTask`
// Example:
// err := s.RunTask(func(ctx context.Context) {
// 	select {
// 	case <-ctx.Done():
// 	// cancelled
// 	case <-time.After(time.Second):
// 		// do something
// 	}
// })
// if err != nil {
// 	// hanle error
// 	return
// }
func (s *Stopper) RunTask(task func(context.Context)) error {
	return s.RunNamedTask("undefined", task)
}

// RunNamedTask run a task that can be cancelled. ErrUnavailable returned if stopped is not running
// Example:
// err := s.RunNamedTask("named task", func(ctx context.Context) {
// 	select {
// 	case <-ctx.Done():
// 	// cancelled
// 	case <-time.After(time.Second):
// 		// do something
// 	}
// })
// if err != nil {
// 	// hanle error
// 	return
// }
func (s *Stopper) RunNamedTask(name string, task func(context.Context)) error {
	// we use read lock here for avoid race
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.mu.state != running {
		return ErrUnavailable
	}

	id, ctx := s.allocate()
	s.doRunCancelableTask(ctx, id, name, task)
	return nil
}

// Stop stop all task in default timeout. If some tasks do not exit within the specified time,
// the names of these tasks will be returned for analysis.
func (s *Stopper) Stop() ([]string, error) {
	return s.StopWithTimeout(defaultWaitStoppedTimeout)
}

// Stop stop all task in specified timeout. If some tasks do not exit within the specified time,
// the names of these tasks will be returned for analysis.
func (s *Stopper) StopWithTimeout(timeout time.Duration) ([]string, error) {
	s.mu.Lock()
	state := s.mu.state
	s.mu.state = stopping
	s.mu.Unlock()

	switch state {
	case stopped:
		return nil, nil
	case stopping:
		<-s.stopC // wait concurrent stop completed
		return s.runningTasks(), nil
	}

	defer func() {
		close(s.stopC)
	}()

	s.cancels.Range(func(key, value interface{}) bool {
		cancel := value.(context.CancelFunc)
		cancel()
		return true
	})

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			return s.runningTasks(), errors.New("waitting for tasks complete timeout")
		default:
			if s.getTaskCount() == 0 {
				return nil, nil
			}
		}

		time.Sleep(time.Millisecond * 5)
	}
}

func (s *Stopper) runningTasks() []string {
	if s.getTaskCount() == 0 {
		return nil
	}

	var tasks []string
	s.tasks.Range(func(key, value interface{}) bool {
		tasks = append(tasks, value.(string))
		return true
	})
	return tasks
}

func (s *Stopper) setupTask(id uint64, name string) {
	s.tasks.Store(id, name)
	s.addTask(1)
}

func (s *Stopper) shutdownTask(id uint64) {
	s.tasks.Delete(id)
	s.addTask(-1)
}

func (s *Stopper) doRunCancelableTask(ctx context.Context, taskID uint64, name string, task func(context.Context)) {
	s.setupTask(taskID, name)
	go func() {
		if err := recover(); err != nil {
			panic(err)
		}
		defer func() {
			s.shutdownTask(taskID)
		}()

		task(ctx)
	}()
}

func (s *Stopper) allocate() (uint64, context.Context) {
	ctx, cancel := context.WithCancel(context.Background())
	id := s.nextTaskID()
	s.cancels.Store(id, cancel)
	return id, ctx
}

func (s *Stopper) nextTaskID() uint64 {
	return atomic.AddUint64(&s.atomic.lastID, 1)
}

func (s *Stopper) addTask(v int64) {
	atomic.AddInt64(&s.atomic.taskCount, v)
}

func (s *Stopper) getTaskCount() int64 {
	return atomic.LoadInt64(&s.atomic.taskCount)
}
