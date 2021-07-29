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
	"github.com/matrixorigin/matrixcube/storage/stats"
	"github.com/matrixorigin/matrixcube/util"
)

// StatisticalStorage statistical storage
type StatisticalStorage interface {
	// Stats storage status
	Stats() stats.Stats
}

// KVStorage is KV based storage
type KVStorage interface {
	// Write write the data in batch
	Write(wb *util.WriteBatch, sync bool) error

	// Set put the key, value pair to the storage
	Set(key []byte, value []byte) error
	// SetWithTTL put the key, value pair to the storage with a ttl in seconds
	SetWithTTL(key []byte, value []byte, ttl int32) error
	// Get returns the value of the key
	Get(key []byte) ([]byte, error)
	// MGet get multi values
	MGet(keys ...[]byte) ([][]byte, error)
	// Delete remove the key from the storage
	Delete(key []byte) error

	// Scan scans the key-value paire in [start, end), and perform with a handler function, if the function
	// returns false, the scan will be terminated, if the `pooledKey` is true, raftstore will call `Free` when
	// scan completed.
	Scan(start, end []byte, handler func(key, value []byte) (bool, error), pooledKey bool) error
	// PrefixScan scans the key-value pairs starts from prefix but only keys for the same prefix,
	// while perform with a handler function, if the function returns false, the scan will be terminated.
	// if the `pooledKey` is true, raftstore will call `Free` when
	// scan completed.
	PrefixScan(prefix []byte, handler func(key, value []byte) (bool, error), pooledKey bool) error
	// Free free the pooled bytes
	Free(pooled []byte)

	// RangeDelete delete data in [start,end).
	RangeDelete(start, end []byte) error
	// Seek returns the first key-value that >= key
	Seek(key []byte) ([]byte, []byte, error)
}
