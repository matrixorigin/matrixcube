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

package storage

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	cpebble "github.com/cockroachdb/pebble"
	"github.com/matrixorigin/matrixcube/storage/mem"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
)

var (
	factories = map[string]func(vfs.FS, *testing.T) MetadataStorage{
		"memory": createMem,
		"pebble": createPebble,
	}
)

func createMem(fs vfs.FS, t *testing.T) MetadataStorage {
	return mem.NewStorage(fs)
}

func createPebble(fs vfs.FS, t *testing.T) MetadataStorage {
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
			wb := util.NewWriteBatch()

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

			s.Set(key1, value1)
			s.Set(key2, value2)
			s.Set(key3, value3)

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

func TestSetAndMGet(t *testing.T) {
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

			s.Set(key1, value1)
			s.Set(key2, value2)
			s.Set(key3, value3)

			values, err := s.MGet(key1, key2, key3, key4)
			assert.NoError(t, err, "TestSetAndGet failed")
			assert.Equal(t, 4, len(values), "TestSetAndGet failed")

			assert.Equal(t, string(value1), string(values[0]), "TestSetAndGet failed")
			assert.Equal(t, string(value2), string(values[1]), "TestSetAndGet failed")
			assert.Equal(t, string(value3), string(values[2]), "TestSetAndGet failed")
			assert.Equal(t, 0, len(values[3]), "TestSetAndGet failed")
		})
	}
}

func TestSetAndGetWithTTL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			if name == "pebble" {
				return
			}

			s := factory(fs, t)
			defer s.Close()
			key1 := []byte("k1")
			value1 := []byte("v1")

			s.SetWithTTL(key1, value1, 1)

			value, err := s.Get(key1)
			assert.NoError(t, err, "TestSetAndGetWithTTL failed")
			assert.Equal(t, string(value1), string(value), "TestSetAndGetWithTTL failed")

			time.Sleep(time.Millisecond * 1200)
			value, err = s.Get(key1)
			assert.NoError(t, err, "TestSetAndGetWithTTL failed")
			assert.Equal(t, 0, len(value), "TestSetAndGetWithTTL failed")

			c := 0
			err = s.Scan(key1, []byte("k2"), func(key, value []byte) (bool, error) {
				c++
				return true, nil
			}, false)
			assert.NoError(t, err, "TestSetAndGetWithTTL failed")
			assert.Equal(t, 0, c, "TestSetAndGetWithTTL failed")
		})
	}
}

func TestWritebatchWithTTL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			if name == "pebble" {
				return
			}

			s := factory(fs, t)
			defer s.Close()
			key1 := []byte("k1")
			value1 := []byte("v1")
			wb := util.NewWriteBatch()
			wb.SetWithTTL(key1, value1, 1)

			err := s.Write(wb, false)
			assert.NoError(t, err, "TestWritebatchWithTTL failed")

			value, err := s.Get(key1)
			assert.NoError(t, err, "TestWritebatchWithTTL failed")
			assert.Equal(t, string(value1), string(value), "TestWritebatchWithTTL failed")

			time.Sleep(time.Millisecond * 1200)
			value, err = s.Get(key1)
			assert.NoError(t, err, "TestWritebatchWithTTL failed")
			assert.Equal(t, 0, len(value), "TestWritebatchWithTTL failed")

			c := 0
			err = s.Scan(key1, []byte("k2"), func(key, value []byte) (bool, error) {
				c++
				return true, nil
			}, false)
			assert.NoError(t, err, "TestWritebatchWithTTL failed")
			assert.Equal(t, 0, c, "TestWritebatchWithTTL failed")
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

			s.Set(key1, value1)
			s.Set(key2, value2)

			s.Delete(key2)

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

			s.Set(key1, value1)
			s.Set(key2, value2)
			s.Set(key3, value3)

			err := s.RangeDelete(key1, key3)
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

			s.Set(key1, value1)
			s.Set(key3, value3)

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

			s.Set(key1, value1)
			s.Set(key2, value2)
			s.Set(key3, value3)

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
