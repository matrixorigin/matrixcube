package util

// WriteBatch the write batch
type WriteBatch interface {
	// Delete remove the key
	Delete(key []byte) error
	// Set set the key-value pair
	Set(key []byte, value []byte) error
}
