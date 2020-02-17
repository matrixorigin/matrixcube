package util

// WriteBatch the write batch
type WriteBatch interface {
	// Delete remove the key
	Delete(key []byte) error
	// Set set the key-value pair
	Set(key []byte, value []byte) error
	// SetWithTTL set the key-value paire with a ttl in seconds
	SetWithTTL(key []byte, value []byte, ttl int64) error
}
