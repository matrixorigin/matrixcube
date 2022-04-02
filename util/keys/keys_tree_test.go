package keys

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAdd(t *testing.T) {
	k := []byte("k1")
	tree := NewKeyTree(64)
	tree.Add(k)
	assert.True(t, tree.Contains(k))
}

func TestAddMany(t *testing.T) {
	k1 := []byte("k1")
	k2 := []byte("k2")
	tree := NewKeyTree(64)
	tree.AddMany([][]byte{k1, k2})
	assert.True(t, tree.Contains(k1))
	assert.True(t, tree.Contains(k2))
}

func TestDelete(t *testing.T) {
	k := []byte("k1")
	tree := NewKeyTree(64)
	tree.Add(k)
	tree.Delete(k)
	assert.False(t, tree.Contains(k))
}

func TestDeleteMany(t *testing.T) {
	k1 := []byte("k1")
	k2 := []byte("k2")
	tree := NewKeyTree(64)
	tree.Add(k1)
	tree.Add(k2)
	tree.DeleteMany([][]byte{k1, k2})
	assert.False(t, tree.Contains(k1))
	assert.False(t, tree.Contains(k2))
}

func TestLen(t *testing.T) {
	k := []byte("k1")
	tree := NewKeyTree(64)
	tree.Add(k)
	assert.Equal(t, 1, tree.Len())
	tree.Add(k)
	assert.Equal(t, 1, tree.Len())
	tree.Add([]byte("k2"))
	assert.Equal(t, 2, tree.Len())
}

func TestAscend(t *testing.T) {
	k1 := []byte("k1")
	k2 := []byte("k2")
	k3 := []byte("k3")
	tree := NewKeyTree(64)
	tree.Add(k1)
	tree.Add(k2)
	tree.Add(k3)

	n := 0
	tree.Ascend(func(key []byte) bool {
		n++
		return true
	})
	assert.Equal(t, 3, n)

	n = 0
	tree.Ascend(func(key []byte) bool {
		n++
		return false
	})
	assert.Equal(t, 1, n)
}

func TestAscendRange(t *testing.T) {
	k1 := []byte("k1")
	k2 := []byte("k2")
	k3 := []byte("k3")
	tree := NewKeyTree(64)
	tree.Add(k1)
	tree.Add(k2)
	tree.Add(k3)

	n := 0
	tree.AscendRange(k1, k3, func(key []byte) bool {
		n++
		return true
	})
	assert.Equal(t, 2, n)

	n = 0
	tree.AscendRange(k1, k3, func(key []byte) bool {
		n++
		return false
	})
	assert.Equal(t, 1, n)
}
