package storage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/deepfabric/beehive/storage/mem"
	"github.com/deepfabric/beehive/storage/pebble"
	"github.com/stretchr/testify/assert"
)

var (
	dataDactories = map[string]func(*testing.T) DataStorage{
		"memory": createDataMem,
		// "pebble": createDataPebble,
	}
)

func createDataMem(t *testing.T) DataStorage {
	return mem.NewStorage()
}

func createDataPebble(t *testing.T) DataStorage {
	path := fmt.Sprintf("/tmp/pebble/%d", time.Now().UnixNano())
	os.RemoveAll(path)
	os.MkdirAll(path, os.ModeDir)
	s, err := pebble.NewStorage(path)
	assert.NoError(t, err, "createDataPebble failed")
	return s
}

func TestRangeDelete(t *testing.T) {
	for name, factory := range dataDactories {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			key1 := []byte("k1")
			value1 := []byte("value1")

			key2 := []byte("k2")
			value2 := []byte("value2")

			key3 := []byte("k3")
			value3 := []byte("value3")

			assert.NoError(t, s.Set(key1, value1), "TestRangeDelete failed")
			assert.NoError(t, s.Set(key2, value2), "TestRangeDelete failed")
			assert.NoError(t, s.Set(key3, value3), "TestRangeDelete failed")

			err := s.RangeDelete(key1, key3)
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
		})
	}
}

func TestSplitCheck(t *testing.T) {
	for name, factory := range dataDactories {
		t.Run(name, func(t *testing.T) {
			s := factory(t)
			totalSize := uint64(16)
			totalKeys := uint64(4)
			key1 := []byte("k1")
			value1 := []byte("v1")

			key2 := []byte("k2")
			value2 := []byte("v2")

			key3 := []byte("k3")
			value3 := []byte("v3")

			key4 := []byte("k4")
			value4 := []byte("v4")

			end := []byte("k5")

			assert.NoError(t, s.Set(key1, value1))
			assert.NoError(t, s.Set(key2, value2))
			assert.NoError(t, s.Set(key3, value3))
			assert.NoError(t, s.Set(key4, value4))

			// [key1, key5), after split ranges: [key1, key2), [key2, key3), [key3, key4), [key4, key5)
			total, keys, splitKeys, err := s.SplitCheck(key1, end, 4)
			assert.NoError(t, err)
			assert.Equal(t, totalSize, total)
			assert.Equal(t, totalKeys, keys)
			assert.Equal(t, 3, len(splitKeys))
			assert.Equal(t, key2, splitKeys[0])
			assert.Equal(t, key3, splitKeys[1])
			assert.Equal(t, key4, splitKeys[2])

			// [key1, key5), after split ranges: [key1, key3), [key3, key5)
			total, keys, splitKeys, err = s.SplitCheck(key1, end, 8)
			assert.NoError(t, err)
			assert.Equal(t, totalSize, total)
			assert.Equal(t, totalKeys, keys)
			assert.Equal(t, 1, len(splitKeys))
			assert.Equal(t, key3, splitKeys[0])

			// [key1, key5), after split ranges: [key1, key4), [key4, key5)
			total, keys, splitKeys, err = s.SplitCheck(key1, end, 12)
			assert.NoError(t, err)
			assert.Equal(t, totalSize, total)
			assert.Equal(t, totalKeys, keys)
			assert.Equal(t, 1, len(splitKeys))
			assert.Equal(t, key4, splitKeys[0])

			// [key1, key5), after split ranges: [key1, key5)
			total, keys, splitKeys, err = s.SplitCheck(key1, end, 16)
			assert.NoError(t, err)
			assert.Equal(t, totalSize, total)
			assert.Equal(t, totalKeys, keys)
			assert.Equal(t, 0, len(splitKeys))
		})
	}
}

func TestCreateAndApply(t *testing.T) {
	for name, factory := range dataDactories {
		t.Run(name, func(t *testing.T) {
			s1 := factory(t)
			s2 := factory(t)

			path := fmt.Sprintf("/tmp/%s-snap", name)
			os.RemoveAll(path)
			err := os.MkdirAll(path, os.ModeDir)
			assert.NoError(t, err, "TestCreateAndApply failed")

			key1 := []byte("key1")
			value1 := []byte("value1")

			key2 := []byte("key2")
			value2 := []byte("value2")

			assert.NoError(t, s1.Set(key1, value1), "TestCreateAndApply failed")
			assert.NoError(t, s1.Set(key2, value2), "TestCreateAndApply failed")

			err = s1.CreateSnapshot(path, key1, key2)
			assert.NoError(t, err, "TestCreateAndApply failed")

			key4 := []byte("key4")
			value4 := []byte("value4")
			assert.NoError(t, s2.Set(key4, value4), "TestCreateAndApply failed")

			err = s2.ApplySnapshot(path)
			assert.NoError(t, err, "TestCreateAndApply failed")

			value, err := s2.Get(key2)
			assert.NoError(t, err, "TestCreateAndApply failed")
			assert.Equal(t, 0, len(value), "TestCreateAndApply failed")

			value, err = s2.Get(key4)
			assert.NoError(t, err, "TestCreateAndApply failed")
			assert.Equal(t, len(value4), len(value), "TestCreateAndApply failed")

			value, err = s2.Get(key1)
			assert.NoError(t, err, "TestCreateAndApply failed")
			assert.Equal(t, string(value1), string(value), "TestCreateAndApply failed")
		})
	}
}
