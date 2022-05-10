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

package stats

import (
	"sync/atomic"
)

// Stats storage stats
type Stats struct {
	WrittenKeys  uint64
	WrittenBytes uint64
	ReadKeys     uint64
	ReadBytes    uint64
	// SyncCount number of `Sync` method called
	SyncCount uint64
}

// Copy returns another instance for rough statistics.
// https://github.com/matrixorigin/matrixone/issues/2447
func (s *Stats) Copy() Stats {
	return Stats{
		WrittenKeys:  atomic.LoadUint64(&s.WrittenKeys),
		WrittenBytes: atomic.LoadUint64(&s.WrittenBytes),
		ReadKeys:     atomic.LoadUint64(&s.ReadKeys),
		ReadBytes:    atomic.LoadUint64(&s.ReadBytes),
		SyncCount:    atomic.LoadUint64(&s.SyncCount),
	}
}
