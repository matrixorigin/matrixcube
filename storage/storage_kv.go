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
	"github.com/matrixorigin/matrixcube/util"
)

// KVStorage is key-value based storage.
type KVStorage interface {
	BaseStorage
	// Write writes the data in batch to the storage.
	Write(wb util.WriteBatch, sync bool) error
	// Set puts the key-value pair to the storage.
	Set(key []byte, value []byte, sync bool) error
	// Get returns the value associated with the key.
	Get(key []byte) ([]byte, error)
	// Delete removes the key-value pair specified by the key.
	Delete(key []byte, sync bool) error
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
	RangeDelete(start, end []byte, sync bool) error
	// Seek returns the first key-value pair that has the key component no less
	// than the specified key.
	Seek(key []byte) ([]byte, []byte, error)
	// Sync synchronize the storage's in-core state with that on disk.
	Sync() error
}
