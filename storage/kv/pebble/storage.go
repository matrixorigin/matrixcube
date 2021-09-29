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

package pebble

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/matrixorigin/matrixcube/components/keys"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/stats"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/vfs"
)

// Storage returns a kv storage based on badger
type Storage struct {
	db    *pebble.DB
	fs    vfs.FS
	stats stats.Stats
}

var _ storage.BaseStorage = (*Storage)(nil)

// NewStorage returns a pebble backed kv store
func NewStorage(dir string, opts *pebble.Options) (*Storage, error) {
	if !hasEventListener(opts.EventListener) {
		opts.EventListener = getEventListener()
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}

	var fs vfs.FS
	if opts.FS != nil {
		if pfs, ok := opts.FS.(*vfs.PebbleFS); ok {
			fs = pfs.GetVFS()
		} else {
			panic("not a pebble fs")
		}
	} else {
		panic("fs not set for pebble")
	}

	return &Storage{
		db: db,
		fs: fs,
	}, nil
}

func (s *Storage) Stats() stats.Stats {
	return s.stats
}

// Set put the key, value pair to the storage
func (s *Storage) Set(key []byte, value []byte) error {
	atomic.AddUint64(&s.stats.WrittenKeys, 1)
	atomic.AddUint64(&s.stats.WrittenBytes, uint64(len(value)+len(key)))
	return s.db.Set(key, value, pebble.NoSync)
}

// Get returns the value of the key
func (s *Storage) Get(key []byte) ([]byte, error) {
	value, closer, err := s.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	if len(value) == 0 {
		return nil, nil
	}
	v := make([]byte, len(value))
	copy(v, value)
	atomic.AddUint64(&s.stats.ReadKeys, 1)
	atomic.AddUint64(&s.stats.ReadBytes, uint64(len(key)+len(value)))
	return v, nil
}

// Delete remove the key from the storage
func (s *Storage) Delete(key []byte) error {
	atomic.AddUint64(&s.stats.WrittenKeys, 1)
	atomic.AddUint64(&s.stats.WrittenBytes, uint64(len(key)))
	return s.db.Delete(key, pebble.NoSync)
}

// RangeDelete remove data in [start,end)
func (s *Storage) RangeDelete(start, end []byte) error {
	atomic.AddUint64(&s.stats.WrittenKeys, 2)
	atomic.AddUint64(&s.stats.WrittenBytes, uint64(len(start)+len(end)))
	return s.db.DeleteRange(start, end, pebble.NoSync)
}

// Scan scans the key-value pairs in [start, end), and perform with a handler function, if the function
// returns false, the scan will be terminated.
// The Handler func will received a cloned the key and value, if the `copy` is true.
func (s *Storage) Scan(start, end []byte, handler func(key, value []byte) (bool, error), copy bool) error {
	iter := s.db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
	defer iter.Close()

	iter.First()
	for iter.Valid() {
		err := iter.Error()
		if err != nil {
			return err
		}
		var ok bool
		if copy {
			ok, err = handler(clone(iter.Key()), clone(iter.Value()))
		} else {
			ok, err = handler(iter.Key(), iter.Value())
		}
		if err != nil {
			return err
		}
		atomic.AddUint64(&s.stats.ReadKeys, 1)
		atomic.AddUint64(&s.stats.ReadBytes, uint64(len(iter.Key())+len(iter.Value())))
		if !ok {
			break
		}
		iter.Next()
	}

	return nil
}

// PrefixScan scans the key-value pairs starts from prefix but only keys for the same prefix,
// while perform with a handler function, if the function returns false, the scan will be terminated.
// The Handler func will received a cloned the key and value, if the `copy` is true.
func (s *Storage) PrefixScan(prefix []byte, handler func(key, value []byte) (bool, error), copy bool) error {
	iter := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix})
	defer iter.Close()
	iter.First()
	for iter.Valid() {
		if err := iter.Error(); err != nil {
			return err
		}
		if ok := bytes.HasPrefix(iter.Key(), prefix); !ok {
			break
		}
		var err error
		var ok bool
		if copy {
			ok, err = handler(clone(iter.Key()), clone(iter.Value()))
		} else {
			ok, err = handler(iter.Key(), iter.Value())
		}
		if err != nil {
			return err
		}

		atomic.AddUint64(&s.stats.ReadKeys, 1)
		atomic.AddUint64(&s.stats.ReadBytes, uint64(len(iter.Key())+len(iter.Value())))
		if !ok {
			break
		}
		iter.Next()
	}
	return nil
}

// SplitCheck Find a key from [start, end), so that the sum of bytes of the value of [start, key) <=size,
// returns the current bytes in [start,end), and the founded key
func (s *Storage) SplitCheck(start []byte, end []byte, size uint64) (uint64, uint64, [][]byte, error) {
	total := uint64(0)
	keys := uint64(0)
	sum := uint64(0)
	appendSplitKey := false
	var splitKeys [][]byte

	iter := s.db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
	defer iter.Close()

	iter.First()
	for iter.Valid() {
		if err := iter.Error(); err != nil {
			return 0, 0, nil, err
		}
		if bytes.Compare(iter.Key(), end) >= 0 {
			break
		}
		if appendSplitKey {
			splitKeys = append(splitKeys, clone(iter.Key()))
			appendSplitKey = false
			sum = 0
		}
		n := uint64(len(iter.Key()) + len(iter.Value()))
		sum += n
		total += n
		keys++
		atomic.AddUint64(&s.stats.ReadKeys, 1)
		if sum >= size {
			appendSplitKey = true
		}
		iter.Next()
	}
	if total > 0 {
		atomic.AddUint64(&s.stats.ReadBytes, total)
	}
	return total, keys, splitKeys, nil
}

// Seek returns the first key-value that >= key
func (s *Storage) Seek(target []byte) ([]byte, []byte, error) {
	var key, value []byte
	iter := s.db.NewIter(&pebble.IterOptions{LowerBound: target})
	defer iter.Close()
	iter.First()
	if iter.Valid() {
		if err := iter.Error(); err != nil {
			return nil, nil, err
		}
		key = clone(iter.Key())
		value = clone(iter.Value())
		atomic.AddUint64(&s.stats.ReadKeys, 1)
		atomic.AddUint64(&s.stats.ReadBytes, uint64(len(iter.Key())+len(iter.Value())))
	}
	return key, value, nil
}

// Sync persist data to disk
func (s *Storage) Sync() error {
	atomic.AddUint64(&s.stats.SyncCount, 1)
	wb := s.db.NewBatch()
	defer wb.Close()
	wb.Set(keys.ForcedSyncKey, keys.ForcedSyncKey, pebble.Sync)
	return s.db.Apply(wb, pebble.Sync)
}

// NewWriteBatch create and returns write batch
func (s *Storage) NewWriteBatch() util.WriteBatch {
	return newWriteBatch(s.db.NewBatch(), &s.stats)
}

// Write write the data in batch
func (s *Storage) Write(uwb util.WriteBatch, sync bool) error {
	wb := uwb.(*writeBatch)
	b := wb.batch
	opts := pebble.NoSync
	if sync {
		opts = pebble.Sync
	}
	return s.db.Apply(b, opts)
}

// CreateSnapshot create a snapshot file under the giving path
func (s *Storage) CreateSnapshot(path string, start, end []byte) error {
	if err := s.fs.MkdirAll(path, 0755); err != nil {
		return err
	}
	file := s.fs.PathJoin(path, "db.data")
	f, err := s.fs.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := writeBytes(f, start); err != nil {
		return err
	}
	if err := writeBytes(f, end); err != nil {
		return err
	}

	snap := s.db.NewSnapshot()
	defer snap.Close()
	iter := snap.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
	defer iter.Close()
	iter.First()
	for iter.Valid() {
		err := iter.Error()
		if err != nil {
			return err
		}

		if bytes.Compare(iter.Key(), end) >= 0 {
			break
		}

		if err := writeBytes(f, iter.Key()); err != nil {
			return err
		}
		if err = writeBytes(f, iter.Value()); err != nil {
			return err
		}
		n := uint64(len(iter.Key()) + len(iter.Value()))
		atomic.AddUint64(&s.stats.ReadKeys, 1)
		atomic.AddUint64(&s.stats.ReadBytes, n)
		atomic.AddUint64(&s.stats.WrittenKeys, 1)
		atomic.AddUint64(&s.stats.WrittenBytes, n)
		iter.Next()
	}

	return nil
}

// ApplySnapshot apply a snapshort file from giving path
func (s *Storage) ApplySnapshot(path string) error {
	f, err := s.fs.Open(s.fs.PathJoin(path, "db.data"))
	if err != nil {
		return err
	}
	defer f.Close()
	start, err := readBytes(f)
	if err != nil {
		return err
	}
	if len(start) == 0 {
		return fmt.Errorf("error format, missing start field")
	}
	end, err := readBytes(f)
	if err != nil {
		return err
	}
	if len(end) == 0 {
		return fmt.Errorf("error format, missing end field")
	}
	if err := s.db.DeleteRange(start, end, pebble.NoSync); err != nil {
		return err
	}
	for {
		key, err := readBytes(f)
		if err != nil {
			return err
		}
		if len(key) == 0 {
			break
		}
		value, err := readBytes(f)
		if err != nil {
			return err
		}
		if len(value) == 0 {
			return fmt.Errorf("error format, missing value field")
		}
		n := uint64(len(key) + len(value))
		atomic.AddUint64(&s.stats.ReadKeys, 1)
		atomic.AddUint64(&s.stats.ReadBytes, n)
		atomic.AddUint64(&s.stats.WrittenKeys, 1)
		atomic.AddUint64(&s.stats.WrittenBytes, n)
		err = s.db.Set(key, value, pebble.NoSync)
		if err != nil {
			return err
		}
	}

	return nil
}

// Close close the storage
func (s *Storage) Close() error {
	return s.db.Close()
}

func clone(value []byte) []byte {
	v := make([]byte, len(value))
	copy(v, value)
	return v
}

func writeBytes(f vfs.File, data []byte) error {
	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, uint32(len(data)))
	if _, err := f.Write(size); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	return nil
}

func readBytes(f vfs.File) ([]byte, error) {
	size := make([]byte, 4)
	n, err := f.Read(size)
	if n == 0 && err == io.EOF {
		return nil, nil
	}

	total := int(binary.BigEndian.Uint32(size))
	written := 0
	data := make([]byte, total)
	for {
		n, err = f.Read(data[written:])
		if err != nil && err != io.EOF {
			return nil, err
		}
		written += n
		if written == total {
			return data, nil
		}
	}
}

func newWriteBatch(batch *pebble.Batch, stats *stats.Stats) util.WriteBatch {
	return &writeBatch{batch: batch, stats: stats}
}

type writeBatch struct {
	batch *pebble.Batch
	stats *stats.Stats
}

func (wb *writeBatch) Delete(key []byte) {
	wb.batch.Delete(key, nil)
	atomic.AddUint64(&wb.stats.WrittenBytes, uint64(len(key)))
}

func (wb *writeBatch) Set(key []byte, value []byte) {
	wb.batch.Set(key, value, nil)
	atomic.AddUint64(&wb.stats.WrittenBytes, uint64(len(key)+len(value)))
}

func (wb *writeBatch) SetWithTTL(key []byte, value []byte, ttl int32) {
	panic("pebble not support set with TTL")
}

func (wb *writeBatch) Reset() {
	wb.batch.Reset()
}

func (wb *writeBatch) Close() {
	wb.batch.Close()
}
