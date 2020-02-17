package mem

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/deepfabric/beehive/util"
)

// Storage memory storage
type Storage struct {
	kv *util.KVTree
}

// NewStorage returns a mem data storage
func NewStorage() *Storage {
	return &Storage{
		kv: util.NewKVTree(),
	}
}

// Set put the key, value pair to the storage
func (s *Storage) Set(key []byte, value []byte) error {
	s.kv.Put(key, value)
	return nil
}

// SetWithTTL put the key, value pair to the storage with a ttl in seconds
func (s *Storage) SetWithTTL(key []byte, value []byte, ttl int) error {
	return fmt.Errorf("Memory storage not support SetWithTTL")
}

// BatchSet batch set
func (s *Storage) BatchSet(pairs ...[]byte) error {
	if len(pairs)%2 != 0 {
		return fmt.Errorf("invalid args len: %d", len(pairs))
	}

	for i := 0; i < len(pairs)/2; i++ {
		s.kv.Put(pairs[2*i], pairs[2*i+1])
	}

	return nil
}

// Get returns the value of the key
func (s *Storage) Get(key []byte) ([]byte, error) {
	v := s.kv.Get(key)
	return v, nil
}

// Delete remove the key from the storage
func (s *Storage) Delete(key []byte) error {
	s.kv.Delete(key)
	return nil
}

// BatchDelete batch delete
func (s *Storage) BatchDelete(keys ...[]byte) error {
	for _, key := range keys {
		s.kv.Delete(key)
	}

	return nil
}

// RangeDelete remove data in [start,end)
func (s *Storage) RangeDelete(start, end []byte) error {
	s.kv.RangeDelete(start, end)
	return nil
}

// Scan scans the key-value paire in [start, end), and perform with a handler function, if the function
// returns false, the scan will be terminated, if the `pooledKey` is true, raftstore will call `Free` when
// scan completed.
func (s *Storage) Scan(start, end []byte, handler func(key, value []byte) (bool, error), pooledKey bool) error {
	return s.kv.Scan(start, end, handler)
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
	s.kv.Scan(start, end, func(key, value []byte) (bool, error) {
		total += uint64(len(key) + len(value))
		if !found && total >= size {
			found = true
			splitKey = key
		}
		return true, nil
	})

	return total, splitKey, nil
}

// Seek returns the first key-value that >= key
func (s *Storage) Seek(key []byte) ([]byte, []byte, error) {
	k, v := s.kv.Seek(key)
	return k, v, nil
}

// NewWriteBatch return a new write batch
func (s *Storage) NewWriteBatch() util.WriteBatch {
	return &writeBatch{}
}

// Write write the data in batch
func (s *Storage) Write(wb util.WriteBatch, sync bool) error {
	mwb := wb.(*writeBatch)
	for _, opt := range mwb.opts {
		if opt.isDelete {
			s.Delete(opt.key)
		} else {
			s.Set(opt.key, opt.value)
		}
	}

	return nil
}

// CreateSnapshot create a snapshot file under the giving path
func (s *Storage) CreateSnapshot(path string, start, end []byte) error {
	err := os.MkdirAll(path, os.ModeDir)
	if err != nil {
		return err
	}

	f, err := os.Create(filepath.Join(path, "db.data"))
	if err != nil {
		return err
	}
	defer f.Close()

	err = writeBytes(f, start)
	if err != nil {
		return err
	}

	err = writeBytes(f, end)
	if err != nil {
		return err
	}

	return s.kv.Scan(start, end, func(key, value []byte) (bool, error) {
		err := writeBytes(f, key)
		if err != nil {
			return false, err
		}

		err = writeBytes(f, value)
		if err != nil {
			return false, err
		}

		return true, nil
	})
}

// ApplySnapshot apply a snapshort file from giving path
func (s *Storage) ApplySnapshot(path string) error {
	f, err := os.Open(filepath.Join(path, "db.data"))
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

	s.kv.RangeDelete(start, end)
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

		s.kv.Put(key, value)
	}

	return nil
}

// Close close the storage
func (s *Storage) Close() error {
	return nil
}

func (s *Storage) onTimeout(arg interface{}) {
	s.Delete(arg.([]byte))
}

type opt struct {
	key      []byte
	value    []byte
	ttl      int64
	isDelete bool
}

type writeBatch struct {
	opts []opt
}

func (wb *writeBatch) Delete(key []byte) error {
	wb.opts = append(wb.opts, opt{
		key:      key,
		isDelete: true,
	})
	return nil
}

func (wb *writeBatch) Set(key []byte, value []byte) error {
	wb.opts = append(wb.opts, opt{
		key:   key,
		value: value,
	})
	return nil
}

func (wb *writeBatch) SetWithTTL(key []byte, value []byte, ttl int64) error {
	wb.opts = append(wb.opts, opt{
		key:   key,
		value: value,
		ttl:   ttl,
	})
	return nil
}

func writeBytes(f *os.File, data []byte) error {
	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, uint32(len(data)))
	_, err := f.Write(size)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func readBytes(f *os.File) ([]byte, error) {
	size := make([]byte, 4)
	n, err := f.Read(size)
	if n == 0 && err == io.EOF {
		return nil, nil
	}

	total := int(binary.BigEndian.Uint32(size))
	written := 0
	data := make([]byte, total, total)
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
