// Copyright 2020 MatrixOrigin.
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

package prophet

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSystimeMonitor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var jumpForward int32

	trigged := false
	go StartMonitor(ctx,
		func() time.Time {
			if !trigged {
				trigged = true
				return time.Now()
			}

			return time.Now().Add(-2 * time.Second)
		}, func() {
			atomic.StoreInt32(&jumpForward, 1)
		})

	time.Sleep(1 * time.Second)

	assert.Equal(t, int32(1), atomic.LoadInt32(&jumpForward), "TestSystimeMonitor failed")
}
