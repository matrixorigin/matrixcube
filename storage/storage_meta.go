package storage

// MetadataStorage the storage to save raft log, shard and store metadata.
type MetadataStorage interface {
	StatisticalStorage
	KVStorage

	// Close close the storage
	Close() error
}
