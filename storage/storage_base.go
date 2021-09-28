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

// CloseableStorage is a storage that can be closed.
type CloseableStorage interface {
	// Close closes the storage.
	Close() error
}

// StatisticalStorage is a storage that can provide stats.
type StatisticalStorage interface {
	// Stats returns the stats of the storage.
	Stats() stats.Stats
}

// BaseStorage is the interface suppose to be implemented by all DataStorage
// types.
type BaseStorage interface {
	StatisticalStorage
	CloseableStorage
	// SplitCheck finds keys within the [start, end) range so that the sum of bytes
	// of each value is no greater than the specified size in bytes. It returns the
	// current bytes and the total number of keys in [start,end), the founded split
	// keys.
	SplitCheck(start, end []byte, size uint64) (currentSize uint64,
		currentKeys uint64, splitKeys [][]byte, err error)
	// CreateSnapshot creates a snapshot stored in the directory specified by the
	// given path.
	CreateSnapshot(path string, start, end []byte) error
	// ApplySnapshot applies the snapshort stored in the given path.
	ApplySnapshot(path string) error
}

// KVStorage is key-value based storage.
type KVStorage interface {
	CloseableStorage
	// NewWriteBatch returns a new write batch.
	NewWriteBatch() util.WriteBatch
	// Write writes the data in batch to the storage.
	Write(wb util.WriteBatch, sync bool) error
	// Set puts the key-value pair to the storage.
	Set(key []byte, value []byte) error
	// SetWithTTL puts the key-value pair to the storage with a TTL in seconds.
	SetWithTTL(key []byte, value []byte, ttl int32) error
	// Get returns the value associated with the key.
	Get(key []byte) ([]byte, error)
	// MGet returns all values associated with the specified keys.
	MGet(keys [][]byte) ([][]byte, error)
	// Delete removes the key-value pair specified by the key.
	Delete(key []byte) error
	// Scan scans the key-value paire in the specified [start, end) range, the
	// specified handler function is invoked on each key-value pair until the
	// handler function returns false. Depending on the copy parameter, the
	// handler function will be provided a cloned key-value pair that can be
	// retained after the return of the handler function or a pair of temporary
	// key-value slices that could change after the return of the handler
	// function.
	Scan(start, end []byte, handler func(key, value []byte) (bool, error), copy bool) error
	// PrefixScan scans all key-value pairs that share the specified prefix, the
	// specified handler function will be invoked on each such key-value pairs
	// until false is returned by the handler function. Depending on the copy
	// parameter, the handler function will be provided a cloned key-value pair
	// that can be retained after the return of the handler function or a pair
	// of temporary key-value slices that could change after the return of the
	// handler function.
	PrefixScan(prefix []byte, handler func(key, value []byte) (bool, error), copy bool) error
	// RangeDelete delete data within the specified [start,end) range.
	RangeDelete(start, end []byte) error
	// Seek returns the first key-value pair that has the key component no less
	// than the specified key.
	Seek(key []byte) ([]byte, []byte, error)
	// Sync synchronize the storage's in-core state with that on disk.
	Sync() error
}

// KVBaseStorage is the interface for Key-Value based BaseStorage
type KVBaseStorage interface {
	BaseStorage
	KVStorage
}
