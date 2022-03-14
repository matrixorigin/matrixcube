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

package buf

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSyncPoolAllocSmall(t *testing.T) {
	pool := NewSyncPool(128, 1024, 2)
	mem := pool.Alloc(64)
	assert.Equal(t, 64, len(mem))
	assert.Equal(t, 128, cap(mem))
	pool.Free(mem)
}

func TestSyncPoolAllocLarge(t *testing.T) {
	pool := NewSyncPool(128, 1024, 2)
	mem := pool.Alloc(2048)
	assert.Equal(t, 2048, len(mem))
	assert.Equal(t, 2048, cap(mem))
	pool.Free(mem)
}

func BenchmarkSyncPoolAllocAndFree128(b *testing.B) {
	pool := NewSyncPool(128, 1024, 2)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pool.Free(pool.Alloc(128))
		}
	})
}

func BenchmarkSyncPoolAllocAndFree256(b *testing.B) {
	pool := NewSyncPool(128, 1024, 2)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pool.Free(pool.Alloc(256))
		}
	})
}

func BenchmarkSyncPoolAllocAndFree512(b *testing.B) {
	pool := NewSyncPool(128, 1024, 2)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pool.Free(pool.Alloc(512))
		}
	})
}

func BenchmarkSyncPoolCacheMiss128(b *testing.B) {
	pool := NewSyncPool(128, 1024, 2)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pool.Alloc(128)
		}
	})
}

func Benchmark_SyncPool_CacheMiss_256(b *testing.B) {
	pool := NewSyncPool(128, 1024, 2)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pool.Alloc(256)
		}
	})
}

func Benchmark_SyncPool_CacheMiss_512(b *testing.B) {
	pool := NewSyncPool(128, 1024, 2)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pool.Alloc(512)
		}
	})
}

func Benchmark_Make_128(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		var x []byte
		for pb.Next() {
			x = make([]byte, 128)
		}
		x = x[:0]
	})
}

func Benchmark_Make_256(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		var x []byte
		for pb.Next() {
			x = make([]byte, 256)
		}
		x = x[:0]
	})
}

func Benchmark_Make_512(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		var x []byte
		for pb.Next() {
			x = make([]byte, 512)
		}
		x = x[:0]
	})
}
