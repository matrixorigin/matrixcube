package keys

// NextKey returns the next key
func NextKey(k []byte) []byte {
	v := make([]byte, len(k)+1)
	copy(v, k)
	return v
}

// Clone clone the value
func Clone(value []byte) []byte {
	v := make([]byte, len(value))
	copy(v, value)
	return v
}
