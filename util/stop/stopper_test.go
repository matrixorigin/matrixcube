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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRunTaskOnNotRunning(t *testing.T) {
	s := NewStopper()
	s.Stop()
	assert.Equal(t, ErrUnavailable, s.RunTask(func(ctx context.Context) {

	}))
}

func TestRunTask(t *testing.T) {
	s := NewStopper()
	defer s.Stop()

	c := make(chan struct{})
	s.RunTask(func(ctx context.Context) {
		close(c)
	})
	select {
	case <-c:
		break
	case <-time.After(time.Second):
		assert.Fail(t, "run task timeout")
	}
}

func TestRunTaskWithTimeout(t *testing.T) {
	s := NewStopper()

	s.RunNamedTask("timeout", func(ctx context.Context) {
		time.Sleep(time.Second)
	})

	names, err := s.StopWithTimeout(time.Millisecond * 10)
	assert.Error(t, err)
	assert.Equal(t, 1, len(names))
	assert.Equal(t, "timeout", names[0])
}
