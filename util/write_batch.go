package util

var (
	// OpSet op set
	OpSet int32 = 0
	// OpDelete op delete
	OpDelete int32 = 1
)

// NewWriteBatch returns a write batch
func NewWriteBatch() *WriteBatch {
	return &WriteBatch{}
}

// WriteBatch write batch
type WriteBatch struct {
	Ops    []int32
	Keys   [][]byte
	Values [][]byte
	TTLs   []int32
}

// Delete remove the key
func (wb *WriteBatch) Delete(key []byte) error {
	wb.Ops = append(wb.Ops, OpDelete)
	wb.Keys = append(wb.Keys, key)
	wb.Values = append(wb.Values, nil)
	wb.TTLs = append(wb.TTLs, 0)
	return nil
}

// Set set key, value
func (wb *WriteBatch) Set(key []byte, value []byte) error {
	return wb.SetWithTTL(key, value, 0)
}

// SetWithTTL set key, value with TTL in seconds
func (wb *WriteBatch) SetWithTTL(key []byte, value []byte, ttl int32) error {
	wb.Ops = append(wb.Ops, OpSet)
	wb.Keys = append(wb.Keys, key)
	wb.Values = append(wb.Values, value)
	wb.TTLs = append(wb.TTLs, ttl)
	return nil
}

// Reset reset
func (wb *WriteBatch) Reset() {
	wb.Ops = wb.Ops[:0]
	wb.Keys = wb.Keys[:0]
	wb.Values = wb.Values[:0]
	wb.TTLs = wb.TTLs[:0]
}
