package storage

// Batch batch opts
type Batch struct {
	// SaveKeys save opts
	SaveKeys []string
	// SaveValues save opts
	SaveValues []string
	// RemoveKeys remove keys
	RemoveKeys []string
}

// KV is an abstract interface for load/save prophet cluster data.
type KV interface {
	// Batch do batch
	Batch(batch *Batch) error
	// Save save key-value paire to storage
	Save(key, value string) error
	// Load load data of key
	Load(key string) (string, error)
	// Remove delete key from storage
	Remove(key string) error
	// LoadRange iterates all key-value pairs in the storage
	LoadRange(key, endKey string, limit int64) ([]string, []string, error)
	// CountRange count all key-value pairs in the storage
	CountRange(key, endKey string) (uint64, error)
	// AllocID allocate a id from kv
	AllocID() (uint64, error)
	// SaveIfNotExists put the value at path
	// returns true, nil, nil if created
	// returns false, exists, nil if not created
	SaveIfNotExists(key string, value string, batch *Batch) (bool, string, error)
	// RemoveIfValueMatched returns true if the expect value is and the exists value are matched
	RemoveIfValueMatched(key string, expect string) (bool, error)
}
