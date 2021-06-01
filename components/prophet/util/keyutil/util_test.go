package keyutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyUtil(t *testing.T) {
	startKey := []byte("a")
	endKey := []byte("b")
	key := BuildKeyRangeKey(1, startKey, endKey)
	assert.Equal(t, "1-61-62", key)

	assert.Equal(t, uint64(1), GetGroupFromRangeKey(key))
}
