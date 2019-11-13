package storage

// MetadataStorage the storage to save raft log, shard and store metadata.
type MetadataStorage interface {
	// NewWriteBatch return a new write batch
	NewWriteBatch() WriteBatch
	// Write write the data in batch
	Write(wb WriteBatch, sync bool) error

	// Set put the key, value pair to the storage
	Set(key []byte, value []byte) error
	// Get returns the value of the key
	Get(key []byte) ([]byte, error)
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
}

// WriteBatch the write batch
type WriteBatch interface {
	// Delete remove the key
	Delete(key []byte) error
	// Set set the key-value pair
	Set(key []byte, value []byte) error
}
