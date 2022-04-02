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

package hlc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestToMicrosecond(t *testing.T) {
	assert.Equal(t, int64(1), toMicrosecond(1000))
}

func TestPhysicalClockReturnsNanoseconds(t *testing.T) {
	v1 := physicalClock()
	time.Sleep(1 * time.Microsecond)
	v2 := physicalClock()
	assert.True(t, v2-v1 >= 1e3)
}

func TestSkipClockUncertainityPeriodOnRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// TODO: check why the maxOffset below can not be set lower
	c := NewUnixNanoHLCClock(ctx, 4*time.Millisecond)
	v1 := physicalClock()
	SkipClockUncertainityPeriodOnRestart(ctx, c)
	v2 := physicalClock()
	assert.True(t, v2-v1 >= 1e6)
}

func TestNewHLCClock(t *testing.T) {
	c := NewHLCClock(physicalClock, time.Second)
	assert.Equal(t, time.Second, c.maxOffset)
}

func TestNewUnixNanoHLCClock(t *testing.T) {
	v := physicalClock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewUnixNanoHLCClock(ctx, 500*time.Millisecond)
	result := c.Now()
	assert.True(t, v < result.PhysicalTime)
}

func TestMaxClockForwardOffset(t *testing.T) {
	c := NewHLCClock(physicalClock, 2*time.Second)
	assert.Equal(t, time.Second, c.maxClockForwardOffset())
}

func TestKeepPhysicalClock(t *testing.T) {
	c := NewHLCClock(physicalClock, time.Second)
	c.mu.maxLearnedPhysicalTime = 1234
	result := c.keepPhysicalClock(1233)
	assert.Equal(t, int64(1234), result)
	assert.Equal(t, int64(1234), c.mu.maxLearnedPhysicalTime)

	result = c.keepPhysicalClock(1235)
	assert.Equal(t, int64(1234), result)
	assert.Equal(t, int64(1235), c.mu.maxLearnedPhysicalTime)
}

func TestGetPhysicalClock(t *testing.T) {
	pc := func() int64 {
		return 200
	}
	c := NewHLCClock(pc, time.Second)
	c.mu.maxLearnedPhysicalTime = 123
	result := c.getPhysicalClock()
	assert.Equal(t, int64(200), result)
	assert.Equal(t, int64(200), c.mu.maxLearnedPhysicalTime)

	c.mu.maxLearnedPhysicalTime = 300
	result = c.getPhysicalClock()
	assert.Equal(t, int64(200), result)
	assert.Equal(t, int64(300), c.mu.maxLearnedPhysicalTime)
}

func TestNow(t *testing.T) {
	pc := func() int64 {
		return 200
	}
	c := NewHLCClock(pc, time.Second)

	c.mu.ts = Timestamp{PhysicalTime: 100, LogicalTime: 10}
	result := c.Now()
	assert.Equal(t, Timestamp{PhysicalTime: 200}, result)

	c.mu.ts = Timestamp{PhysicalTime: 300, LogicalTime: 10}
	result = c.Now()
	assert.Equal(t, Timestamp{PhysicalTime: 300, LogicalTime: 11}, result)
}

func TestUpdate(t *testing.T) {
	pc := func() int64 {
		return 200
	}
	c := NewHLCClock(pc, time.Second)

	// pt is the max
	c.mu.ts = Timestamp{PhysicalTime: 100, LogicalTime: 10}
	c.Update(Timestamp{PhysicalTime: 120})
	assert.Equal(t, Timestamp{PhysicalTime: 200}, c.mu.ts)

	c.physicalClock = func() int64 { return 50 }
	// m has the same physical time and greater logical time
	c.mu.ts = Timestamp{PhysicalTime: 100, LogicalTime: 10}
	c.Update(Timestamp{PhysicalTime: 100, LogicalTime: 100})
	assert.Equal(t, Timestamp{PhysicalTime: 100, LogicalTime: 100}, c.mu.ts)

	// m has the largest physical time
	c.mu.ts = Timestamp{PhysicalTime: 100, LogicalTime: 10}
	m := Timestamp{PhysicalTime: 120, LogicalTime: 100}
	c.Update(m)
	assert.Equal(t, m, c.mu.ts)

	// m has smaller physical time
	c.mu.ts = Timestamp{PhysicalTime: 100, LogicalTime: 10}
	old := c.mu.ts
	c.Update(Timestamp{PhysicalTime: 99, LogicalTime: 100})
	assert.Equal(t, old, c.mu.ts)
}
