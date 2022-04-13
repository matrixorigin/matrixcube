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

// View is a point in time view of the KVStore.
type View interface {
	Close() error
	Raw() interface{}
}

// KVStore is the interface for supported key-value based data store operations.
type KVStore interface {
	// GetView returns a point in time view of the KVStore.
	GetView() View
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
	// handler function returns false. Depending on the clone parameter, the
	// handler function will be provided a cloned key-value pair that can be
	// retained after the return of the handler function or a pair of temporary
	// key-value slices that could change after the return of the handler
	// function.
	Scan(start, end []byte,
		handler func(key, value []byte) (bool, error), clone bool) error
	// ScanInView is similar to Scan, it performs the Scan operation on the
	// specified view.
	ScanInView(view View, start, end []byte,
		handler func(key, value []byte) (bool, error), clone bool) error
	// Deprecated: PrefixScan scans all key-value pairs that share the specified prefix, the
	// specified handler function will be invoked on each such key-value pairs
	// until false is returned by the handler function. Depending on the clone
	// parameter, the handler function will be provided a cloned key-value pair
	// that can be retained after the return of the handler function or a pair
	// of temporary key-value slices that could change after the return of the
	// handler function. Use `ScanInViewWithOptions`` instead
	PrefixScan(prefix []byte,
		handler func(key, value []byte) (bool, error), clone bool) error
	// ScanInViewWithOptions scans the key-value paire in the specified [start, end) range. The
	// starting key of the next iteration of Scan is determined by the handler return. The Key
	// and Value passed to the handler are unsafe and will only be in effect for the current call.
	// To save them, the caller needs to copy the values.
	ScanInViewWithOptions(view View, start, end []byte, handler func(key, value []byte) (NextIterOptions, error)) error
	// ReverseScanInViewWithOptions is silimar to ScanInViewWithOptions, but perform reverse Scan
	ReverseScanInViewWithOptions(view View, start, end []byte, handler func(key, value []byte) (NextIterOptions, error)) error
	// RangeDelete delete data within the specified [start,end) range.
	RangeDelete(start, end []byte, sync bool) error
	// SeekLT returns the first key-value pair that the key >= target.
	Seek(target []byte) ([]byte, []byte, error)
	// SeekLT returns the last key-value pair that the key < target.
	SeekLT(target []byte) ([]byte, []byte, error)
	// Sync synchronize the storage's in-core state with that on disk.
	Sync() error
}

// NextIterOptions options for next iteration
type NextIterOptions struct {
	// Stop set true to stop the iteration
	Stop bool
	// SeekGE if set, seek to fisrt key in [SeekGE, max) for next iteration
	SeekGE []byte
	// SeekLT if set, seek to fisrt key in (min, SeekLT) for next iteration
	SeekLT []byte
}

// KVStorage is key-value based storage.
type KVStorage interface {
	Closeable
	WriteBatchCreator
	StatsKeeper
	KVStore
}

// KVMetadataStore is a KV based data store for storing MatrixCube metadata.
type KVMetadataStore interface {
	// not allowed to close the store
	WriteBatchCreator
	KVStore
}

// KVBaseStorage is a KV based base storage.
type KVBaseStorage interface {
	BaseStorage
	KVStore
}
