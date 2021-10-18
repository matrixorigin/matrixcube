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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	q := New(2)
	assert.NoError(t, q.Put(1, 2, 3))
	assert.Equal(t, int64(3), q.Len())

	items := make([]interface{}, 3)
	n, err := q.Get(3, items)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), n)
	assert.Equal(t, int64(0), q.Len())

	assert.NoError(t, q.Put(4, 5, 6))
	assert.Equal(t, int64(3), q.Len())
	items = q.Dispose()
	assert.Equal(t, int64(0), q.Len())
	assert.Equal(t, 3, len(items))
}

func TestQueueWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	q := NewWithContext(2, ctx)
	assert.NoError(t, q.Put(1, 2, 3))
	assert.Equal(t, int64(3), q.Len())

	items := make([]interface{}, 3)
	n, err := q.Get(3, items)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), n)
	assert.Equal(t, int64(0), q.Len())

	assert.NoError(t, q.Put(4, 5, 6))
	cancel()
	n, err = q.Get(3, items)
	assert.Error(t, err)
	assert.Equal(t, int64(0), n)
	assert.Equal(t, int64(0), q.Len())
}
