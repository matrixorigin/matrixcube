package storage

import (
	"github.com/deepfabric/beehive/util"
)

// MetadataStorage the storage to save raft log, shard and store metadata.
type MetadataStorage interface {
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
	// Free free the pooled bytes
	Free(pooled []byte)

	// RangeDelete delete data in [start,end).
	RangeDelete(start, end []byte) error
	// Seek returns the first key-value that >= key
	Seek(key []byte) ([]byte, []byte, error)

	// Close close the storage
	Close() error
}
