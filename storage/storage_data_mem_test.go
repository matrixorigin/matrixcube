package storage

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRangeDelete(t *testing.T) {
	s := NewMemDataStorage().(*memDataStorage)
	key1 := []byte("k1")
	value1 := []byte("value1")

	key2 := []byte("k2")
	value2 := []byte("value2")

	key3 := []byte("k3")
	value3 := []byte("value3")

	s.kv.Put(key1, value1)
	s.kv.Put(key2, value2)
	s.kv.Put(key3, value3)

	err := s.RangeDelete(key1, key3)
	assert.NoError(t, err, "TestRangeDelete failed")

	value := s.kv.Get(key1)
	assert.Equal(t, 0, len(value), "TestRangeDelete failed")

	value = s.kv.Get(key2)
	assert.Equal(t, 0, len(value), "TestRangeDelete failed")

	value = s.kv.Get(key3)
	assert.Equal(t, len(value3), len(value), "TestRangeDelete failed")
}

func TestSplitCheck(t *testing.T) {
	s := NewMemDataStorage().(*memDataStorage)
	key1 := []byte("k1")
	value1 := []byte("v1")

	key2 := []byte("k2")
	value2 := []byte("v2")

	key3 := []byte("k3")
	value3 := []byte("v3")

	s.kv.Put(key1, value1)
	s.kv.Put(key2, value2)
	s.kv.Put(key3, value3)

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
	path := "/tmp"

	s1 := NewMemDataStorage().(*memDataStorage)
	s2 := NewMemDataStorage().(*memDataStorage)

	size := 1024 * 1024 * 10
	key1 := []byte("key1")
	value1 := make([]byte, size, size)
	for i := 0; i < size; i++ {
		value1[i] = 1
	}

	key2 := []byte("key2")
	value2 := []byte("value2")

	s1.kv.Put(key1, value1)
	s1.kv.Put(key2, value2)

	err := s1.CreateSnapshot(path, key1, key2)
	assert.NoError(t, err, "TestCreateAndApply failed")

	err = s2.ApplySnapshot(path)
	assert.NoError(t, err, "TestCreateAndApply failed")

	value := s2.kv.Get(key1)
	assert.Equal(t, len(value1), len(value), "TestCreateAndApply failed")
	for _, v := range value {
		assert.Equal(t, byte(1), v, "TestCreateAndApply failed")
	}

	value = s2.kv.Get(key2)
	assert.Equal(t, 0, len(value), "TestCreateAndApply failed")
}
