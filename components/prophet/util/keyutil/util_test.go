package keyutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyUtil(t *testing.T) {
	startKey := []byte("a")
	endKey := []byte("b")
	key := BuildKeyRangeKey(startKey, endKey)
	assert.Equal(t, "61-62", key)
}
