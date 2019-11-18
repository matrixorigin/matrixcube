package storage

import (
	"github.com/deepfabric/beehive/util"
)

type opt struct {
	key      []byte
	value    []byte
	isDelete bool
}

type memoryWriteBatch struct {
	opts []opt
}

func (wb *memoryWriteBatch) Delete(key []byte) error {
	wb.opts = append(wb.opts, opt{
		key:      key,
		isDelete: true,
	})
	return nil
}

func (wb *memoryWriteBatch) Set(key []byte, value []byte) error {
	wb.opts = append(wb.opts, opt{
		key:   key,
		value: value,
	})
	return nil
}

// just for test
type memMetadataStorage struct {
	kv *util.KVTree
}

// NewMemMetadataStorage returns a metastorage using memory, just for test
func NewMemMetadataStorage() MetadataStorage {
	return &memMetadataStorage{
		kv: util.NewKVTree(),
	}
}

func (s *memMetadataStorage) NewWriteBatch() util.WriteBatch {
	return &memoryWriteBatch{}
}

func (s *memMetadataStorage) Write(wb util.WriteBatch, sync bool) error {
	mwb := wb.(*memoryWriteBatch)
	for _, opt := range mwb.opts {
		if opt.isDelete {
			s.Delete(opt.key)
		} else {
			s.Set(opt.key, opt.value)
		}
	}

	return nil
}

func (s *memMetadataStorage) Set(key []byte, value []byte) error {
	s.kv.Put(key, value)
	return nil
}

func (s *memMetadataStorage) Get(key []byte) ([]byte, error) {
	v := s.kv.Get(key)
	return v, nil
}

func (s *memMetadataStorage) Delete(key []byte) error {
	s.kv.Delete(key)
	return nil
}

func (s *memMetadataStorage) Scan(start, end []byte, handler func(key, value []byte) (bool, error), pooledKey bool) error {
	return s.kv.Scan(start, end, handler)
}

func (s *memMetadataStorage) Free(pooled []byte) {

}

func (s *memMetadataStorage) RangeDelete(start, end []byte) error {
	s.kv.RangeDelete(start, end)
	return nil
}

func (s *memMetadataStorage) Seek(key []byte) ([]byte, []byte, error) {
	k, v := s.kv.Seek(key)
	return k, v, nil
}
