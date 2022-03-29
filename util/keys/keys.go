package keys

// NextKey returns the next key
func NextKey(k []byte) []byte {
	v := make([]byte, len(k)+1)
	copy(v, k)
	return v
}
