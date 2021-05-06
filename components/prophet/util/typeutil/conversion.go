package typeutil

import (
	"encoding/binary"
	"fmt"
)

// BytesToUint64 converts a byte slice to uint64.
func BytesToUint64(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("invalid len %d for uint64", len(b))
	}

	return binary.BigEndian.Uint64(b), nil
}

// Uint64ToBytes converts uint64 to a byte slice.
func Uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// BoolToUint64 converts bool to uint64.
func BoolToUint64(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

// BoolToInt converts bool to int.
func BoolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
