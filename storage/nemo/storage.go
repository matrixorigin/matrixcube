package nemo

import (
	"fmt"

	"github.com/deepfabric/beehive/util"
	gonemo "github.com/deepfabric/go-nemo"
)

// Storage nemo storage
type Storage struct {
	db        *gonemo.NEMO
	kv        *gonemo.DBNemo
	redisKV   *redisKV
	redisHash *redisHash
	redisSet  *redisSet
	redisZSet *redisZSet
	redisList *redisList
}

// NewStorage create a storage based on nemo
func NewStorage(dataPath string) (*Storage, error) {
	return NewStorageWithOption(dataPath, "")
}

// NewStorageWithOption create a storage based on nemo
func NewStorageWithOption(dataPath, options string) (*Storage, error) {
	var opts *gonemo.Options
	if options != "" {
		opts, _ = gonemo.NewOptions(options)
	} else {
		opts = gonemo.NewDefaultOptions()
	}

	db := gonemo.OpenNemo(opts, dataPath)
	return &Storage{
		db:        db,
		kv:        db.GetKvHandle(),
		redisKV:   &redisKV{db: db},
		redisHash: &redisHash{db: db},
		redisSet:  &redisSet{db: db},
		redisZSet: &redisZSet{db: db},
		redisList: &redisList{db: db},
	}, nil
}

// Set put the key, value pair to the storage
func (s *Storage) Set(key []byte, value []byte) error {
	return s.db.PutWithHandle(s.kv, key, value, false)
}

// SetWithTTL put the key, value pair to the storage with a ttl in seconds
func (s *Storage) SetWithTTL(key []byte, value []byte, ttl int) error {
	return s.RedisKV().Set(key, value, ttl)
}

// BatchSet batch set
func (s *Storage) BatchSet(pairs ...[]byte) error {
	if len(pairs)%2 != 0 {
		return fmt.Errorf("invalid args len: %d", len(pairs))
	}

	wb := gonemo.NewWriteBatch()
	for i := 0; i < len(pairs)/2; i++ {
		wb.WriteBatchPut(pairs[2*i], pairs[2*i+1])
	}
	return s.db.BatchWrite(s.kv, wb, false)
}

// Get returns the value of the key
func (s *Storage) Get(key []byte) ([]byte, error) {
	return s.db.GetWithHandle(s.kv, key)
}

// Delete remove the key from the storage
func (s *Storage) Delete(key []byte) error {
	return s.db.DeleteWithHandle(s.kv, key, false)
}

// BatchDelete batch delete
func (s *Storage) BatchDelete(keys ...[]byte) error {
	wb := gonemo.NewWriteBatch()
	for _, key := range keys {
		wb.WriteBatchDel(key)
	}
	return s.db.BatchWrite(s.kv, wb, false)
}

// RangeDelete remove data in [start,end)
func (s *Storage) RangeDelete(start, end []byte) error {
	return s.db.RangeDelWithHandle(s.kv, start, end)
}

// Scan scans the key-value paire in [start, end), and perform with a handler function, if the function
// returns false, the scan will be terminated, if the `pooledKey` is true, raftstore will call `Free` when
// scan completed.
func (s *Storage) Scan(start, end []byte, handler func(key, value []byte) (bool, error), pooledKey bool) error {
	var key []byte
	var err error
	c := false

	it := s.db.KScanWithHandle(s.db.GetKvHandle(), start, end, true)
	for ; it.Valid(); it.Next() {
		if pooledKey {
			key = it.PooledKey()
		} else {
			key = it.Key()
		}

		c, err = handler(key, it.Value())
		if err != nil || !c {
			break
		}
	}
	it.Free()

	return err
}

// Free free the pooled bytes
func (s *Storage) Free(pooled []byte) {
	gonemo.MemPool.Free(pooled)
}

// SplitCheck Find a key from [start, end), so that the sum of bytes of the value of [start, key) <=size,
// returns the current bytes in [start,end), and the founded key
func (s *Storage) SplitCheck(start []byte, end []byte, size uint64) (uint64, []byte, error) {
	var currentSize uint64
	var targetKey []byte

	it := s.db.NewVolumeIterator(start, end)
	if it.TargetScan(int64(size)) {
		targetKey = it.TargetKey()
	}
	currentSize = uint64(it.TotalVolume())
	it.Free()
	return currentSize, targetKey, nil
}

// Seek returns the first key-value that >= key
func (s *Storage) Seek(target []byte) ([]byte, []byte, error) {
	return s.db.SeekWithHandle(s.kv, target)
}

// NewWriteBatch return a new write batch
func (s *Storage) NewWriteBatch() util.WriteBatch {
	return &writeBatch{
		wb: gonemo.NewWriteBatch(),
	}
}

// Write write the data in batch
func (s *Storage) Write(value util.WriteBatch, sync bool) error {
	return s.db.BatchWrite(s.kv, value.(*writeBatch).wb, sync)
}

// CreateSnapshot create a snapshot file under the giving path
func (s *Storage) CreateSnapshot(path string, start, end []byte) error {
	return s.db.RawScanSaveRange(path, start, end, true)
}

// ApplySnapshot apply a snapshort file from giving path
func (s *Storage) ApplySnapshot(path string) error {
	return s.db.IngestFile(path)
}

// RedisKV returns a redis kv impl
func (s *Storage) RedisKV() RedisKV {
	return s.redisKV
}

// RedisHash returns a redis hash impl
func (s *Storage) RedisHash() RedisHash {
	return s.redisHash
}

// RedisSet returns a redis set impl
func (s *Storage) RedisSet() RedisSet {
	return s.redisSet
}

// RedisZSet returns a redis zset impl
func (s *Storage) RedisZSet() RedisZSet {
	return s.redisZSet
}

// RedisList returns a redis list impl
func (s *Storage) RedisList() RedisList {
	return s.redisList
}

// Close close the storage
func (s *Storage) Close() error {
	s.db.Close()
	return nil
}

type writeBatch struct {
	wb *gonemo.WriteBatch
}

// Delete remove the key
func (wb *writeBatch) Delete(key []byte) error {
	wb.wb.WriteBatchDel(key)
	return nil
}

// Set set the key-value pair
func (wb *writeBatch) Set(key []byte, value []byte) error {
	wb.wb.WriteBatchPut(key, value)
	return nil
}
