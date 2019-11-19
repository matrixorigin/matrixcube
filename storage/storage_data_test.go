package storage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/deepfabric/beehive/storage/badger"
	"github.com/deepfabric/beehive/storage/nemo"
	"github.com/stretchr/testify/assert"
)

type testStorage interface {
	DataStorage
	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
}

var (
	dataDactories = map[string]func(*testing.T) testStorage{
		"memory": createDataMem,
		"badger": createDataBadger,
		"nemo":   createDataNemo,
	}
)

func createDataMem(t *testing.T) testStorage {
	return NewMemDataStorage().(*memDataStorage)
}

func createDataBadger(t *testing.T) testStorage {
	path := fmt.Sprintf("/tmp/badger/%d", time.Now().UnixNano())
	os.RemoveAll(path)
	os.MkdirAll(path, os.ModeDir)
	s, err := badger.NewKVStore(path)
	assert.NoError(t, err, "createBadger failed")
	return s
}

func createDataNemo(t *testing.T) testStorage {
	path := fmt.Sprintf("/tmp/nemo/%d", time.Now().UnixNano())
	s, err := nemo.NewStorage(path)
	assert.NoError(t, err, "createNemo failed")
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
			key1 := []byte("k1")
			value1 := []byte("v1")

			key2 := []byte("k2")
			value2 := []byte("v2")

			key3 := []byte("k3")
			value3 := []byte("v3")

			assert.NoError(t, s.Set(key1, value1), "TestRangeDelete failed")
			assert.NoError(t, s.Set(key2, value2), "TestRangeDelete failed")
			assert.NoError(t, s.Set(key3, value3), "TestRangeDelete failed")

			total, key, err := s.SplitCheck(key1, []byte("k4"), 1024)
			assert.NoError(t, err, "TestSplitCheck failed")
			assert.Equal(t, uint64(12), total, "TestSplitCheck failed")
			assert.Equal(t, "", string(key), "TestSplitCheck failed")

			total, key, err = s.SplitCheck(key1, []byte("k4"), 4)
			assert.NoError(t, err, "TestSplitCheck failed")
			assert.Equal(t, uint64(12), total, "TestSplitCheck failed")
			assert.Equal(t, "k1", string(key), "TestSplitCheck failed")

			total, key, err = s.SplitCheck(key1, []byte("k4"), 8)
			assert.NoError(t, err, "TestSplitCheck failed")
			assert.Equal(t, uint64(12), total, "TestSplitCheck failed")
			assert.Equal(t, "k2", string(key), "TestSplitCheck failed")

			total, key, err = s.SplitCheck(key1, []byte("k4"), 11)
			assert.NoError(t, err, "TestSplitCheck failed")
			assert.Equal(t, uint64(12), total, "TestSplitCheck failed")
			assert.Equal(t, "k3", string(key), "TestSplitCheck failed")
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
