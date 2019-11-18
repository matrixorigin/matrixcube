package badger

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestRangeDelete(t *testing.T) {
	err := os.RemoveAll("/tmp/badger")
	assert.NoError(t, err, "TestRangeDelete failed")

	s, err := NewKVStore("/tmp/badger")
	assert.NoError(t, err, "TestRangeDelete failed")

	key1 := []byte("k1")
	value1 := []byte("value1")

	key2 := []byte("k2")
	value2 := []byte("value2")

	key3 := []byte("k3")
	value3 := []byte("value3")

	assert.NoError(t, s.Set(key1, value1), "TestRangeDelete failed")
	assert.NoError(t, s.Set(key2, value2), "TestRangeDelete failed")
	assert.NoError(t, s.Set(key3, value3), "TestRangeDelete failed")

	err = s.RangeDelete(key1, key3)
	assert.NoError(t, err, "TestRangeDelete failed")

	value, err := s.Get(key1)
	assert.NoError(t, err, "TestRangeDelete failed")
	assert.Equal(t, 0, len(value), "TestRangeDelete failed")

	value, err = s.Get(key2)
	assert.NoError(t, err, "TestRangeDelete failed")
	assert.Equal(t, 0, len(value), "TestRangeDelete failed")

	value, err = s.Get(key3)
	assert.NoError(t, err, "TestRangeDelete failed")
	assert.Equal(t, len(value3), len(value), "TestRangeDelete failed")
}

func TestSplitCheck(t *testing.T) {
	err := os.RemoveAll("/tmp/badger")
	assert.NoError(t, err, "TestRangeDelete failed")

	s, err := NewKVStore("/tmp/badger")
	assert.NoError(t, err, "TestRangeDelete failed")

	key1 := []byte("k1")
	value1 := []byte("v1")

	key2 := []byte("k2")
	value2 := []byte("v2")

	key3 := []byte("k3")
	value3 := []byte("v3")

	assert.NoError(t, s.Set(key1, value1), "TestRangeDelete failed")
	assert.NoError(t, s.Set(key2, value2), "TestRangeDelete failed")
	assert.NoError(t, s.Set(key3, value3), "TestRangeDelete failed")

	total, key, err := s.SplitCheck(key1, []byte("k4"), 4)
	assert.NoError(t, err, "TestSplitCheck failed")
	assert.Equal(t, uint64(12), total, "TestSplitCheck failed")
	assert.Equal(t, "k2", string(key), "TestSplitCheck failed")

	total, key, err = s.SplitCheck(key1, []byte("k4"), 8)
	assert.NoError(t, err, "TestSplitCheck failed")
	assert.Equal(t, uint64(12), total, "TestSplitCheck failed")
	assert.Equal(t, "k3", string(key), "TestSplitCheck failed")

	total, key, err = s.SplitCheck(key1, []byte("k4"), 11)
	assert.NoError(t, err, "TestSplitCheck failed")
	assert.Equal(t, uint64(12), total, "TestSplitCheck failed")
	assert.Equal(t, "k3", string(key), "TestSplitCheck failed")
}

func TestCreateAndApply(t *testing.T) {
	err := os.RemoveAll("/tmp/badger1")
	assert.NoError(t, err, "TestRangeDelete failed")

	err = os.RemoveAll("/tmp/badger2")
	assert.NoError(t, err, "TestRangeDelete failed")

	s1, err := NewKVStore("/tmp/badger1")
	assert.NoError(t, err, "TestRangeDelete failed")

	s2, err := NewKVStore("/tmp/badger2")
	assert.NoError(t, err, "TestRangeDelete failed")

	path := "/tmp"

	size := 1024 * 1024 * 10
	key1 := []byte("key1")
	value1 := make([]byte, size, size)
	for i := 0; i < size; i++ {
		value1[i] = 1
	}

	key2 := []byte("key2")
	value2 := []byte("value2")

	assert.NoError(t, s1.Set(key1, value1), "TestRangeDelete failed")
	assert.NoError(t, s1.Set(key2, value2), "TestRangeDelete failed")

	err = s1.CreateSnapshot(path, key1, key2)
	assert.NoError(t, err, "TestCreateAndApply failed")

	key4 := []byte("key4")
	value4 := []byte("value4")
	assert.NoError(t, s2.Set(key4, value4), "TestRangeDelete failed")

	err = s2.ApplySnapshot(path)
	assert.NoError(t, err, "TestCreateAndApply failed")

	value, err := s2.Get(key1)
	assert.NoError(t, err, "TestCreateAndApply failed")
	assert.Equal(t, len(value1), len(value), "TestCreateAndApply failed")
	for _, v := range value {
		assert.Equal(t, byte(1), v, "TestCreateAndApply failed")
	}

	value, err = s2.Get(key2)
	assert.NoError(t, err, "TestCreateAndApply failed")
	assert.Equal(t, 0, len(value), "TestCreateAndApply failed")

	value, err = s2.Get(key4)
	assert.NoError(t, err, "TestCreateAndApply failed")
	assert.Equal(t, len(value4), len(value), "TestCreateAndApply failed")
}
