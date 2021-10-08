// Copyright 2020 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	cpebble "github.com/cockroachdb/pebble"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/storage/kv/pebble"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
)

var (
	factories = map[string]func(vfs.FS, *testing.T) storage.MetadataStorage{
		"memory": createMem,
		"pebble": createPebble,
	}
)

func createMem(fs vfs.FS, t *testing.T) storage.MetadataStorage {
	return mem.NewStorage(fs)
}

func createPebble(fs vfs.FS, t *testing.T) storage.MetadataStorage {
	path := filepath.Join(util.GetTestDir(), "pebble", fmt.Sprintf("%d", time.Now().UnixNano()))
	fs.RemoveAll(path)
	fs.MkdirAll(path, 0755)
	opts := &cpebble.Options{FS: vfs.NewPebbleFS(fs)}
	s, err := pebble.NewStorage(path, opts)
	assert.NoError(t, err, "createPebble failed")
	return s
}

func TestWriteBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			s := factory(fs, t)
			defer s.Close()
			r := s.NewWriteBatch()
			wb := r.(util.WriteBatch)

			key1 := []byte("k1")
			value1 := []byte("v1")

			key2 := []byte("k2")
			value2 := []byte("v2")

			key3 := []byte("k3")
			value3 := []byte("v3")

			key4 := []byte("k4")
			value4 := []byte("v4")

			wb.Set(key1, value1)
			wb.Set(key4, value4)
			wb.Delete(key4)
			wb.Set(key2, value2)
			wb.Set(key3, value3)

			err := s.Write(wb, true)
			assert.NoError(t, err, "TestWriteBatch failed")

			value, err := s.Get(key1)
			assert.NoError(t, err, "TestWriteBatch failed")
			assert.Equal(t, string(value1), string(value), "TestWriteBatch failed")

			value, err = s.Get(key2)
			assert.NoError(t, err, "TestWriteBatch failed")
			assert.Equal(t, string(value2), string(value), "TestWriteBatch failed")

			value, err = s.Get(key3)
			assert.NoError(t, err, "TestWriteBatch failed")
			assert.Equal(t, string(value3), string(value), "TestWriteBatch failed")

			value, err = s.Get(key4)
			assert.NoError(t, err, "TestWriteBatch failed")
			assert.Equal(t, "", string(value), "TestWriteBatch failed")
		})
	}
}

func TestSetAndGet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			s := factory(fs, t)
			defer s.Close()
			key1 := []byte("k1")
			value1 := []byte("v1")

			key2 := []byte("k2")
			value2 := []byte("v2")

			key3 := []byte("k3")
			value3 := []byte("v3")

			key4 := []byte("k4")

			s.Set(key1, value1, false)
			s.Set(key2, value2, false)
			s.Set(key3, value3, false)

			value, err := s.Get(key1)
			assert.NoError(t, err, "TestSetAndGet failed")
			assert.Equal(t, string(value1), string(value), "TestSetAndGet failed")

			value, err = s.Get(key2)
			assert.NoError(t, err, "TestSetAndGet failed")
			assert.Equal(t, string(value2), string(value), "TestSetAndGet failed")

			value, err = s.Get(key3)
			assert.NoError(t, err, "TestSetAndGet failed")
			assert.Equal(t, string(value3), string(value), "TestSetAndGet failed")

			value, err = s.Get(key4)
			assert.NoError(t, err, "TestSetAndGet failed")
			assert.Equal(t, "", string(value), "TestSetAndGet failed")
		})
	}
}

func TestDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			s := factory(fs, t)
			defer s.Close()
			key1 := []byte("k1")
			value1 := []byte("v1")

			key2 := []byte("k2")
			value2 := []byte("v2")

			s.Set(key1, value1, false)
			s.Set(key2, value2, false)

			s.Delete(key2, false)

			value, err := s.Get(key1)
			assert.NoError(t, err, "TestDelete failed")
			assert.Equal(t, string(value1), string(value), "TestDelete failed")

			value, err = s.Get(key2)
			assert.NoError(t, err, "TestDelete failed")
			assert.Equal(t, "", string(value), "TestDelete failed")
		})
	}
}

func TestMetaRangeDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			s := factory(fs, t)
			defer s.Close()
			key1 := []byte("k1")
			value1 := []byte("v1")

			key2 := []byte("k2")
			value2 := []byte("v2")

			key3 := []byte("k3")
			value3 := []byte("v3")

			s.Set(key1, value1, false)
			s.Set(key2, value2, false)
			s.Set(key3, value3, false)

			err := s.RangeDelete(key1, key3, false)
			assert.NoError(t, err, "TestMetaRangeDelete failed")

			value, err := s.Get(key1)
			assert.NoError(t, err, "TestMetaRangeDelete failed")
			assert.Equal(t, "", string(value), "TestMetaRangeDelete failed")

			value, err = s.Get(key2)
			assert.NoError(t, err, "TestMetaRangeDelete failed")
			assert.Equal(t, "", string(value), "TestMetaRangeDelete failed")

			value, err = s.Get(key3)
			assert.NoError(t, err, "TestMetaRangeDelete failed")
			assert.Equal(t, string(value3), string(value), "TestMetaRangeDelete failed")
		})
	}
}

func TestSeek(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			s := factory(fs, t)
			defer s.Close()
			key1 := []byte("k1")
			value1 := []byte("v1")

			key3 := []byte("k3")
			value3 := []byte("v3")

			s.Set(key1, value1, false)
			s.Set(key3, value3, false)

			key, value, err := s.Seek(key1)
			assert.NoError(t, err, "TestSeek failed")
			assert.Equal(t, string(key1), string(key), "TestSeek failed")
			assert.Equal(t, string(value1), string(value), "TestSeek failed")

			key, value, err = s.Seek([]byte("k2"))
			assert.NoError(t, err, "TestSeek failed")
			assert.Equal(t, string(key3), string(key), "TestSeek failed")
			assert.Equal(t, string(value3), string(value), "TestSeek failed")
		})
	}
}

func TestScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			s := factory(fs, t)
			defer s.Close()
			key1 := []byte("k1")
			value1 := []byte("v1")

			key2 := []byte("k2")
			value2 := []byte("v2")

			key3 := []byte("k3")
			value3 := []byte("v3")

			s.Set(key1, value1, false)
			s.Set(key2, value2, false)
			s.Set(key3, value3, false)

			count := 0
			err := s.Scan(key1, key3, func(key, value []byte) (bool, error) {
				count++
				return true, nil
			}, false)
			assert.NoError(t, err, "TestScan failed")
			assert.Equal(t, 2, count, "TestScan failed")

			count = 0
			err = s.Scan(key1, key3, func(key, value []byte) (bool, error) {
				count++
				if count == 1 {
					return false, nil
				}
				return true, nil
			}, false)
			assert.NoError(t, err, "TestScan failed")
			assert.Equal(t, 1, count, "TestScan failed")
		})
	}
}

func TestRangeDelete(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			s := factory(fs, t)
			defer s.Close()
			key1 := []byte("k1")
			value1 := []byte("value1")

			key2 := []byte("k2")
			value2 := []byte("value2")

			key3 := []byte("k3")
			value3 := []byte("value3")

			assert.NoError(t, s.Set(key1, value1, false), "TestRangeDelete failed")
			assert.NoError(t, s.Set(key2, value2, false), "TestRangeDelete failed")
			assert.NoError(t, s.Set(key3, value3, false), "TestRangeDelete failed")

			err := s.RangeDelete(key1, key3, false)
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

func TestPrefixScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			s := factory(fs, t)
			defer s.Close()
			prefix := "/m/db"
			for i := 1; i <= 3; i++ {
				key := []byte(fmt.Sprintf("%v/%v/%d", prefix, "defaultdb", i))
				err := s.Set(key, []byte{byte(0)}, false)
				assert.NoError(t, err)
			}
			err := s.PrefixScan([]byte(fmt.Sprintf("%v/%v", prefix, "defaultdb")),
				func(key, value []byte) (bool, error) {
					println(string(key), value[0])
					return true, nil
				}, false)
			assert.NoError(t, err)
		})
	}
}
