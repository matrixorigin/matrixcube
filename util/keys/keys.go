package keys

import (
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
func Clone(value []byte) []byte {
	v := make([]byte, len(value))
	copy(v, value)
	return v
}
