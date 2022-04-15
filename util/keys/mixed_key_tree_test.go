package keys

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewKeySetTreeWithPointKeys(t *testing.T) {
	tree := NewMixedKeysTree([][]byte{{0}, {1}, {1}})
	assert.Equal(t, 2, tree.len())
	ok, kt := tree.get([]byte{0})
	assert.True(t, ok)
	assert.Equal(t, pointType, kt)

	ok, kt = tree.get([]byte{1})
	assert.True(t, ok)
	assert.Equal(t, pointType, kt)
}

func TestAddKeyRangeKeys(t *testing.T) {
	tree := NewMixedKeysTree(nil)

	tree.AddKeyRange([]byte{0}, []byte{1})
	tree.AddKeyRange([]byte{2}, []byte{3})
	assert.Equal(t, 4, tree.len())

	ok, kt := tree.get([]byte{0})
	assert.True(t, ok)
	assert.Equal(t, startType, kt)

	ok, kt = tree.get([]byte{1})
	assert.True(t, ok)
	assert.Equal(t, endType, kt)

	ok, kt = tree.get([]byte{2})
	assert.True(t, ok)
	assert.Equal(t, startType, kt)

	ok, kt = tree.get([]byte{3})
	assert.True(t, ok)
	assert.Equal(t, endType, kt)
}

func TestAddKeyRangeWithOtherPointKeys(t *testing.T) {
	tree := NewMixedKeysTree([][]byte{{5}})

	tree.AddKeyRange([]byte{0}, []byte{1})
	tree.AddKeyRange([]byte{2}, []byte{3})
	assert.Equal(t, 5, tree.len())

	ok, kt := tree.get([]byte{0})
	assert.True(t, ok)
	assert.Equal(t, startType, kt)

	ok, kt = tree.get([]byte{1})
	assert.True(t, ok)
	assert.Equal(t, endType, kt)

	ok, kt = tree.get([]byte{2})
	assert.True(t, ok)
	assert.Equal(t, startType, kt)

	ok, kt = tree.get([]byte{3})
	assert.True(t, ok)
	assert.Equal(t, endType, kt)

	ok, kt = tree.get([]byte{5})
	assert.True(t, ok)
	assert.Equal(t, pointType, kt)
}

func TestAddKeyRangeWithOverlapPointKeys(t *testing.T) {
	tree := NewMixedKeysTree([][]byte{{1}, {0}, {2}})
	tree.AddKeyRange([]byte{1}, []byte{2})

	assert.Equal(t, 3, tree.len())

	ok, kt := tree.get([]byte{0})
	assert.True(t, ok)
	assert.Equal(t, pointType, kt)

	ok, kt = tree.get([]byte{1})
	assert.True(t, ok)
	assert.Equal(t, startType, kt)

	ok, kt = tree.get([]byte{2, 0})
	assert.True(t, ok)
	assert.Equal(t, endType, kt)
}

func TestAddKeyRangeWithContinuousRanges(t *testing.T) {
	tree := NewMixedKeysTree(nil)
	tree.AddKeyRange([]byte{0}, []byte{1})
	assert.Equal(t, 2, tree.len())

	tree.AddKeyRange([]byte{2}, []byte{3})
	assert.Equal(t, 4, tree.len())

	tree.AddKeyRange([]byte{1}, []byte{2})
	assert.Equal(t, 2, tree.len())

	ok, kt := tree.get([]byte{0})
	assert.True(t, ok)
	assert.Equal(t, startType, kt)

	ok, kt = tree.get([]byte{3})
	assert.True(t, ok)
	assert.Equal(t, endType, kt)
}

func TestAddKeyRangeWithIncludeRanges(t *testing.T) {
	tree := NewMixedKeysTree(nil)
	tree.AddKeyRange([]byte{0}, []byte{3})
	tree.AddKeyRange([]byte{0}, []byte{2})
	tree.AddKeyRange([]byte{0}, []byte{3})
	tree.AddKeyRange([]byte{2}, []byte{3})
	tree.AddKeyRange([]byte{0}, []byte{3})

	assert.Equal(t, 2, tree.len())
	ok, kt := tree.get([]byte{0})
	assert.True(t, ok)
	assert.Equal(t, startType, kt)
	ok, kt = tree.get([]byte{3})
	assert.True(t, ok)
	assert.Equal(t, endType, kt)
}

func TestAddKeyRangeWithOverlapRanges(t *testing.T) {
	tree := NewMixedKeysTree([][]byte{{8}})
	tree.AddKeyRange([]byte{2}, []byte{4})
	tree.AddKeyRange([]byte{1}, []byte{3})

	assert.Equal(t, 3, tree.len())
	ok, kt := tree.get([]byte{1})
	assert.True(t, ok)
	assert.Equal(t, startType, kt)
	ok, kt = tree.get([]byte{4})
	assert.True(t, ok)
	assert.Equal(t, endType, kt)

	tree = NewMixedKeysTree([][]byte{{8}})
	tree.AddKeyRange([]byte{1}, []byte{3})
	tree.AddKeyRange([]byte{2}, []byte{4})

	assert.Equal(t, 3, tree.len())
	ok, kt = tree.get([]byte{1})
	assert.True(t, ok)
	assert.Equal(t, startType, kt)
	ok, kt = tree.get([]byte{4})
	assert.True(t, ok)
	assert.Equal(t, endType, kt)
}

func TestAddKeyRangeWithOverlapRangesAndPointKeys(t *testing.T) {
	tree := NewMixedKeysTree([][]byte{{3}})
	tree.AddKeyRange([]byte{2}, []byte{4})
	tree.AddKeyRange([]byte{1}, []byte{3})

	assert.Equal(t, 3, tree.len())
	ok, kt := tree.get([]byte{1})
	assert.True(t, ok)
	assert.Equal(t, startType, kt)
	ok, kt = tree.get([]byte{4})
	assert.True(t, ok)
	assert.Equal(t, endType, kt)
	ok, kt = tree.get([]byte{3})
	assert.True(t, ok)
	assert.Equal(t, pointType, kt)
}

func TestContains(t *testing.T) {
	tree := NewMixedKeysTree([][]byte{{3}})
	tree.AddKeyRange([]byte{0}, []byte{1})
	tree.AddKeyRange([]byte{5}, []byte{7})

	assert.True(t, tree.Contains([]byte{3}))
	assert.True(t, tree.Contains([]byte{0}))
	assert.False(t, tree.Contains([]byte{1}))
	assert.False(t, tree.Contains([]byte{4}))
	assert.True(t, tree.Contains([]byte{5}))
	assert.True(t, tree.Contains([]byte{6}))
	assert.False(t, tree.Contains([]byte{7}))
	assert.False(t, tree.Contains([]byte{8}))
}

func TestMixedSeek(t *testing.T) {
	tree := NewMixedKeysTree([][]byte{{3}})
	tree.AddKeyRange([]byte{0}, []byte{1})
	tree.AddKeyRange([]byte{5}, []byte{7})

	assert.Equal(t, []byte{0}, tree.Seek([]byte{0}))
	assert.Equal(t, []byte{3}, tree.Seek([]byte{1}))
	assert.Equal(t, []byte{3}, tree.Seek([]byte{2}))
	assert.Equal(t, []byte{3}, tree.Seek([]byte{3}))
	assert.Equal(t, []byte{5}, tree.Seek([]byte{4}))
	assert.Equal(t, []byte{5}, tree.Seek([]byte{5}))
	assert.Equal(t, []byte{6}, tree.Seek([]byte{6}))
	assert.Empty(t, tree.Seek([]byte{7}))
}

func TestMixedSeekGT(t *testing.T) {
	tree := NewMixedKeysTree([][]byte{{3}})
	tree.AddKeyRange([]byte{0}, []byte{1})
	tree.AddKeyRange([]byte{5}, []byte{5, 0})
	tree.AddKeyRange([]byte{6}, []byte{7})

	assert.Equal(t, []byte{0, 0}, tree.SeekGT([]byte{0}))
	assert.Equal(t, []byte{3}, tree.SeekGT([]byte{1}))
	assert.Equal(t, []byte{3}, tree.SeekGT([]byte{2}))
	assert.Equal(t, []byte{5}, tree.SeekGT([]byte{3}))
	assert.Equal(t, []byte{5}, tree.SeekGT([]byte{4}))
	assert.Equal(t, []byte{6}, tree.SeekGT([]byte{5}))
	assert.Equal(t, []byte{6, 0}, tree.SeekGT([]byte{6}))
	assert.Empty(t, tree.Seek([]byte{7}))
}
