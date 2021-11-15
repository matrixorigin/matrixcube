package kv

import (
	"testing"

	"github.com/matrixorigin/matrixcube/util/buf"
	"github.com/stretchr/testify/assert"
)

func TestEncodeDataKey(t *testing.T) {
	assert.Equal(t, []byte{dataPrefix, 1}, EncodeDataKey([]byte{1}, nil))
	assert.Equal(t, []byte{dataPrefix, 1}, EncodeDataKey([]byte{1}, buf.NewByteBuf(12)))
}

func TestDecodeDataKey(t *testing.T) {
	assert.Equal(t, []byte{1}, DecodeDataKey([]byte{dataPrefix, 1}))
	assert.Equal(t, []byte{1}, DecodeDataKey([]byte{dataPrefix, 1}))
}
