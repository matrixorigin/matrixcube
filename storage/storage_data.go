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

package storage

import (
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
)

// DataStorage responsible for maintaining the data storage of a set of shards for the application.
type DataStorage interface {
	StatisticalStorage
	CloseableStorage

	// RemovedShardData remove shard data
	RemovedShardData(shard bhmetapb.Shard, encodedStartKey, encodedEndKey []byte) error

	// SplitCheck Find a key from [start, end), so that the sum of bytes of the value of [start, key) <=size,
	// returns the current bytes in [start,end), and the founded key
	SplitCheck(start []byte, end []byte, size uint64) (currentSize uint64, currentKeys uint64, splitKeys [][]byte, err error)
	// CreateSnapshot create a snapshot file under the giving path
	CreateSnapshot(path string, start, end []byte) error
	// ApplySnapshot apply a snapshort file from giving path
	ApplySnapshot(path string) error
}
