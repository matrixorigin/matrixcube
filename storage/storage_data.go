package storage

// DataStorage responsible for maintaining the data storage of a set of shards for the application.
type DataStorage interface {
	MetadataStorage

	// SplitCheck Find a key from [start, end), so that the sum of bytes of the value of [start, key) <=size,
	// returns the current bytes in [start,end), and the founded key
	SplitCheck(start []byte, end []byte, size uint64) (currentSize uint64, splitKey []byte, err error)
	// CreateSnapshot create a snapshot file under the giving path
	CreateSnapshot(path string, start, end []byte) error
	// ApplySnapshot apply a snapshort file from giving path
	ApplySnapshot(path string) error
}
