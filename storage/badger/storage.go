package badger

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/deepfabric/beehive/util"
	"github.com/dgraph-io/badger"
)

// Storage returns a kv storage based on badger
type Storage struct {
	db *badger.DB
}

// NewStorage returns badger kv store on a default options
func NewStorage(dir string) (*Storage, error) {
	return NewStorageWithOptions(badger.DefaultOptions(dir))
}

// NewStorageWithOptions returns badger kv store
func NewStorageWithOptions(opts badger.Options) (*Storage, error) {
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &Storage{
		db: db,
	}, nil
}

// Set put the key, value pair to the storage
func (s *Storage) Set(key []byte, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, value)
		if err != nil {
			return err
		}

		return nil
	})
}

// SetWithTTL put the key, value pair to the storage with a ttl in seconds
func (s *Storage) SetWithTTL(key []byte, value []byte, ttl int) error {
	return s.db.Update(func(txn *badger.Txn) error {
		err := txn.SetEntry(badger.NewEntry(key, value).WithTTL(time.Second * time.Duration(ttl)))
		if err != nil {
			return err
		}

		return nil
	})
}

// BatchSet batch set
func (s *Storage) BatchSet(pairs ...[]byte) error {
	if len(pairs)%2 != 0 {
		return fmt.Errorf("invalid args len: %d", len(pairs))
	}

	return s.db.Update(func(txn *badger.Txn) error {
		for i := 0; i < len(pairs)/2; i++ {
			err := txn.Set(pairs[2*i], pairs[2*i+1])
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// Get returns the value of the key
func (s *Storage) Get(key []byte) ([]byte, error) {
	var value []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}

			return err
		}

		value, err = itemValue(item)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return value, nil
}

// Delete remove the key from the storage
func (s *Storage) Delete(key []byte) error {
	return s.BatchDelete(key)
}

// BatchDelete batch delete
func (s *Storage) BatchDelete(keys ...[]byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			err := txn.Delete(key)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// RangeDelete remove data in [start,end)
func (s *Storage) RangeDelete(start, end []byte) error {
	return s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(start); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			if bytes.Compare(k, end) >= 0 {
				break
			}

			s.BatchDelete(k)
		}
		return nil
	})
}

// Scan scans the key-value paire in [start, end), and perform with a handler function, if the function
// returns false, the scan will be terminated, if the `pooledKey` is true, raftstore will call `Free` when
// scan completed.
func (s *Storage) Scan(start, end []byte, handler func(key, value []byte) (bool, error), pooledKey bool) error {
	return s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(start); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			v, err := itemValue(item)
			if err != nil {
				return err
			}

			if bytes.Compare(k, end) >= 0 {
				break
			}

			next, err := handler(k, v)
			if err != nil {
				return err
			}

			if !next {
				break
			}
		}
		return nil
	})
}

// Free free the pooled bytes
func (s *Storage) Free(pooled []byte) {

}

// SplitCheck Find a key from [start, end), so that the sum of bytes of the value of [start, key) <=size,
// returns the current bytes in [start,end), and the founded key
func (s *Storage) SplitCheck(start []byte, end []byte, size uint64) (uint64, []byte, error) {
	total := uint64(0)
	found := false
	var splitKey []byte

	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(start); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			v, err := itemValue(item)
			if err != nil {
				return err
			}

			if bytes.Compare(k, end) >= 0 {
				break
			}

			total += uint64(len(k) + len(v))
			if !found && total >= size {
				found = true
				splitKey = k
			}
		}
		return nil
	})
	if err != nil {
		return 0, nil, err
	}

	return total, splitKey, err
}

// Seek returns the first key-value that >= key
func (s *Storage) Seek(target []byte) ([]byte, []byte, error) {
	var key, value []byte
	err := s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(target); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			v, err := itemValue(item)
			if err != nil {
				return err
			}

			key = k
			value = v
			break
		}
		return nil
	})

	return key, value, err
}

// NewWriteBatch return a new write batch
func (s *Storage) NewWriteBatch() util.WriteBatch {
	return &writeBatch{}
}

// Write write the data in batch
func (s *Storage) Write(value util.WriteBatch, sync bool) error {
	wb := value.(*writeBatch)
	err := s.db.Update(func(txn *badger.Txn) error {
		var err error
		for idx, op := range wb.opts {
			switch op {
			case delete:
				err = txn.Delete(wb.values[idx])
				idx++
			case set:
				if wb.ttl[idx] == 0 {
					err = txn.Set(wb.values[2*idx], wb.values[2*idx+1])
				} else {
					err = txn.SetEntry(badger.NewEntry(wb.values[2*idx], wb.values[2*idx+1]).WithTTL(time.Second * time.Duration(wb.ttl[idx])))
				}
			}

			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	if sync {
		return s.db.Sync()
	}

	return nil
}

// CreateSnapshot create a snapshot file under the giving path
func (s *Storage) CreateSnapshot(path string, start, end []byte) error {
	err := os.MkdirAll(path, os.ModeDir)
	if err != nil {
		return err
	}

	file := filepath.Join(path, "db.data")
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()

	stream := s.db.NewStream()
	stream.ChooseKey = func(item *badger.Item) bool {
		return bytes.Compare(item.Key(), start) >= 0 &&
			bytes.Compare(item.Key(), end) < 0
	}

	_, err = stream.Backup(f, 0)
	if err != nil {
		return err
	}

	return nil
}

// ApplySnapshot apply a snapshort file from giving path
func (s *Storage) ApplySnapshot(path string) error {
	f, err := os.Open(filepath.Join(path, "db.data"))
	if err != nil {
		return err
	}
	defer f.Close()

	return s.db.Load(f, 16)
}

// Close close the storage
func (s *Storage) Close() error {
	return s.db.Close()
}

func itemValue(item *badger.Item) ([]byte, error) {
	var value []byte
	err := item.Value(func(data []byte) error {
		value = data
		return nil
	})

	return value, err
}

var (
	delete = byte(0)
	set    = byte(1)
)

type writeBatch struct {
	opts   []byte
	values [][]byte
	ttl    []int64
}

// Delete remove the key
func (wb *writeBatch) Delete(key []byte) error {
	wb.opts = append(wb.opts, delete)
	wb.values = append(wb.values, key)
	return nil
}

func (wb *writeBatch) Set(key []byte, value []byte) error {
	return wb.SetWithTTL(key, value, 0)
}

func (wb *writeBatch) SetWithTTL(key []byte, value []byte, ttl int64) error {
	wb.opts = append(wb.opts, set)
	wb.values = append(wb.values, key, value)
	wb.ttl = append(wb.ttl, ttl)
	return nil
}
