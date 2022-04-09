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
	"sync/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/keys"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/stats"
	"github.com/matrixorigin/matrixcube/util"
	keysutil "github.com/matrixorigin/matrixcube/util/keys"
	"github.com/matrixorigin/matrixcube/vfs"
	"go.uber.org/zap"
)

type view struct {
	ss *pebble.Snapshot
}

func (v *view) Close() error {
	return v.ss.Close()
}

func (v *view) Raw() interface{} {
	return v.ss
}

// Storage returns a kv storage based on badger
type Storage struct {
	db    *pebble.DB
	stats stats.Stats
}

var _ storage.KVStorage = (*Storage)(nil)

// CreateLogDBStorage creates the underlying storage that will be used by the
// LogDB.
func CreateLogDBStorage(rootDir string, fs vfs.FS, logger *zap.Logger) storage.KVStorage {
	path := fs.PathJoin(rootDir, "logdb")
	opts := &pebble.Options{
		FS:                          vfs.NewPebbleFS(fs),
		MemTableSize:                1024 * 1024 * 64,
		MemTableStopWritesThreshold: 8,
		MaxManifestFileSize:         1024 * 1024,
		MaxConcurrentCompactions:    2,
		EventListener:               getEventListener(log.Adjust(logger).Named("pebble")),
		MaxOpenFiles:                1024,
	}
	kv, err := NewStorage(path, logger, opts)
	if err != nil {
		panic(err)
	}
	return kv
}

// NewStorage returns a pebble backed kv store.
func NewStorage(dir string, logger *zap.Logger, opts *pebble.Options) (*Storage, error) {
	if !hasEventListener(opts.EventListener) {
		opts.EventListener = getEventListener(log.Adjust(logger).Named("pebble"))
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}

	return &Storage{
		db: db,
	}, nil
}

func (s *Storage) GetView() storage.View {
	return &view{ss: s.db.NewSnapshot()}
}

// Close close the storage
func (s *Storage) Close() error {
	return s.db.Close()
}

// Write write the data in batch
func (s *Storage) Write(uwb util.WriteBatch, sync bool) error {
	wb := uwb.(*writeBatch)
	return s.db.Apply(wb.batch, toWriteOptions(sync))
}

// Set put the key, value pair to the storage
func (s *Storage) Set(key, value []byte, sync bool) error {
	atomic.AddUint64(&s.stats.WrittenKeys, 1)
	atomic.AddUint64(&s.stats.WrittenBytes, uint64(len(value)+len(key)))
	return s.db.Set(key, value, toWriteOptions(sync))
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
func (s *Storage) Delete(key []byte, sync bool) error {
	atomic.AddUint64(&s.stats.WrittenKeys, 1)
	atomic.AddUint64(&s.stats.WrittenBytes, uint64(len(key)))
	return s.db.Delete(key, toWriteOptions(sync))
}

// RangeDelete remove data in [start,end)
func (s *Storage) RangeDelete(start, end []byte, sync bool) error {
	atomic.AddUint64(&s.stats.WrittenKeys, 2)
	atomic.AddUint64(&s.stats.WrittenBytes, uint64(len(start)+len(end)))

	if len(start) == 0 {
		iter := s.db.NewIter(&pebble.IterOptions{})
		defer iter.Close()

		if iter.First() && iter.Valid() {
			if err := iter.Error(); err != nil {
				return err
			}
			fk := iter.Key()
			startKey := make([]byte, len(fk))
			copy(startKey, fk)
			start = startKey
		}
	}

	if len(end) == 0 {
		iter := s.db.NewIter(&pebble.IterOptions{})
		defer iter.Close()

		if iter.Last() && iter.Valid() {
			if err := iter.Error(); err != nil {
				return err
			}
			lk := iter.Key()
			endKey := make([]byte, len(lk)+1)
			copy(endKey, lk)
			endKey[len(lk)] = 0x1
			end = endKey
		}
	}

	// empty db
	if len(start) == 0 && len(end) == 0 {
		return nil
	}

	return s.db.DeleteRange(start, end, toWriteOptions(sync))
}

// Scan scans the key-value pairs in [start, end), and perform with a handler function, if the function
// returns false, the scan will be terminated.
// The Handler func will received a cloned the key and value, if the `cloneResult` is true.
func (s *Storage) Scan(start, end []byte, handler func(key, value []byte) (bool, error), cloneResult bool) error {
	ios := &pebble.IterOptions{}
	if len(start) > 0 {
		ios.LowerBound = start
	}
	if len(end) > 0 {
		ios.UpperBound = end
	}
	iter := s.db.NewIter(ios)
	defer iter.Close()

	iter.First()
	for iter.Valid() {
		err := iter.Error()
		if err != nil {
			return err
		}
		var ok bool
		if cloneResult {
			ok, err = handler(keysutil.Clone(iter.Key()), keysutil.Clone(iter.Value()))
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

func (s *Storage) ScanInView(view storage.View,
	start, end []byte, handler func(key, value []byte) (bool, error), cloneResult bool) error {
	ios := &pebble.IterOptions{}
	if len(start) > 0 {
		ios.LowerBound = start
	}
	if len(end) > 0 {
		ios.UpperBound = end
	}
	ss := view.Raw().(*pebble.Snapshot)
	iter := ss.NewIter(ios)

	defer iter.Close()

	iter.First()
	for iter.Valid() {
		err := iter.Error()
		if err != nil {
			return err
		}
		var ok bool
		if cloneResult {
			ok, err = handler(keysutil.Clone(iter.Key()), keysutil.Clone(iter.Value()))
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

func (s *Storage) ScanInViewWithOptions(view storage.View, start, end []byte, handler func(key, value []byte) (storage.NextIterOptions, error)) error {
	ios := &pebble.IterOptions{}
	if len(start) > 0 {
		ios.LowerBound = start
	}
	if len(end) > 0 {
		ios.UpperBound = end
	}
	ss := view.Raw().(*pebble.Snapshot)
	iter := ss.NewIter(ios)
	defer iter.Close()

	iter.First()
	for iter.Valid() {
		err := iter.Error()
		if err != nil {
			return err
		}
		opts, err := handler(iter.Key(), iter.Value())
		if err != nil {
			return err
		}
		atomic.AddUint64(&s.stats.ReadKeys, 1)
		atomic.AddUint64(&s.stats.ReadBytes, uint64(len(iter.Key())+len(iter.Value())))
		if opts.Stop {
			break
		}

		if len(opts.SeekGE) > 0 {
			iter.SeekGE(opts.SeekGE)
		} else if len(opts.SeekLT) > 0 {
			iter.SeekLT(opts.SeekLT)
		} else {
			iter.Next()
		}
	}

	return nil
}

// PrefixScan scans the key-value pairs starts from prefix but only keys for the same prefix,
// while perform with a handler function, if the function returns false, the scan will be terminated.
// The Handler func will received a cloned the key and value, if the `clone` is true.
func (s *Storage) PrefixScan(prefix []byte, handler func(key, value []byte) (bool, error), cloneResult bool) error {
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
		if cloneResult {
			ok, err = handler(keysutil.Clone(iter.Key()), keysutil.Clone(iter.Value()))
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

// Seek returns the first key-value that >= key
func (s *Storage) Seek(target []byte) ([]byte, []byte, error) {
	var key, value []byte
	view := s.db.NewSnapshot()
	defer view.Close()

	iter := view.NewIter(&pebble.IterOptions{LowerBound: target})
	defer iter.Close()

	if iter.First() {
		if err := iter.Error(); err != nil {
			return nil, nil, err
		}
		key = keysutil.Clone(iter.Key())
		value = keysutil.Clone(iter.Value())
		atomic.AddUint64(&s.stats.ReadKeys, 1)
		atomic.AddUint64(&s.stats.ReadBytes, uint64(len(iter.Key())+len(iter.Value())))
	}
	return key, value, nil
}

// Seek returns the last key-value that < key
func (s *Storage) SeekLT(target []byte) ([]byte, []byte, error) {
	var key, value []byte
	view := s.db.NewSnapshot()
	defer view.Close()

	iter := view.NewIter(&pebble.IterOptions{UpperBound: target})
	defer iter.Close()
	iter.SeekLT(target)

	if iter.Last() {
		if err := iter.Error(); err != nil {
			return nil, nil, err
		}
		key = keysutil.Clone(iter.Key())
		value = keysutil.Clone(iter.Value())
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
	if err := wb.Set(keys.ForcedSyncKey, keys.ForcedSyncKey, nil); err != nil {
		return err
	}
	return s.db.Apply(wb, pebble.Sync)
}

func (s *Storage) Stats() stats.Stats {
	return stats.Stats{
		WrittenKeys:  atomic.LoadUint64(&s.stats.WrittenKeys),
		WrittenBytes: atomic.LoadUint64(&s.stats.WrittenBytes),
		ReadKeys:     atomic.LoadUint64(&s.stats.ReadKeys),
		ReadBytes:    atomic.LoadUint64(&s.stats.ReadBytes),
		SyncCount:    atomic.LoadUint64(&s.stats.SyncCount),
	}
}

// NewWriteBatch create and returns write batch
func (s *Storage) NewWriteBatch() storage.Resetable {
	return newWriteBatch(s.db.NewBatch(), &s.stats)
}

func toWriteOptions(sync bool) *pebble.WriteOptions {
	if sync {
		return pebble.Sync
	}
	return pebble.NoSync
}

func newWriteBatch(batch *pebble.Batch, stats *stats.Stats) util.WriteBatch {
	return &writeBatch{batch: batch, stats: stats}
}

type writeBatch struct {
	batch *pebble.Batch
	stats *stats.Stats
}

func (wb *writeBatch) Delete(key []byte) {
	if err := wb.batch.Delete(key, nil); err != nil {
		panic(err)
	}
	atomic.AddUint64(&wb.stats.WrittenBytes, uint64(len(key)))
}

func (wb *writeBatch) DeleteRange(fk []byte, lk []byte) {
	if err := wb.batch.DeleteRange(fk, lk, nil); err != nil {
		panic(err)
	}
}

func (wb *writeBatch) Set(key []byte, value []byte) {
	if err := wb.batch.Set(key, value, nil); err != nil {
		panic(err)
	}
	atomic.AddUint64(&wb.stats.WrittenBytes, uint64(len(key)+len(value)))
}

func (wb *writeBatch) Reset() {
	wb.batch.Reset()
}

func (wb *writeBatch) Close() {
	wb.batch.Close()
}
