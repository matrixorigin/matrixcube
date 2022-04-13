package keys

import (
	"bytes"
	"sort"

	"github.com/matrixorigin/matrixcube/util/buf"
)

// NextKey returns the next key of current key
func NextKey(key []byte, buffer *buf.ByteBuf) []byte {
	if len(key) == 0 {
		return []byte{0}
	}

	if buffer == nil {
		v := make([]byte, 1+len(key))
		copy(v[0:], key)
		return v
	}

	buffer.MarkWrite()
	if _, err := buffer.Write(key); err != nil {
		panic(err)
	}
	if err := buffer.WriteByte(0); err != nil {
		panic(err)
	}
	return buffer.WrittenDataAfterMark().Data()
}

// Clone clone the value
func Clone(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

// Join join the values
func Join(v1, v2 []byte) []byte {
	dst := make([]byte, len(v1)+len(v2))
	copy(dst, v1)
	copy(dst[len(v1):], v2)
	return dst
}

// Sort values
func Sort(values [][]byte) {
	sort.Slice(values, func(i, j int) bool {
		return bytes.Compare(values[i], values[j]) < 0
	})
}
