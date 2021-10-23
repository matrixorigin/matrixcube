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
	s, err := pebble.NewStorage(path, fs, opts)
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

func TestScan(t *testing.T) {
	tests := []struct {
		keys   [][]byte
		start  []byte
		end    []byte
		result [][]byte
	}{
		{
			[][]byte{[]byte("key1"), []byte("key2"), []byte("key3")},
			[]byte("key1"),
			[]byte("key3"),
			[][]byte{[]byte("key1"), []byte("key2")},
		},

		{
			[][]byte{[]byte("key1"), []byte("key2"), []byte("key3")},
			nil,
			[]byte("key3"),
			[][]byte{[]byte("key1"), []byte("key2")},
		},

		{
			[][]byte{[]byte("key1"), []byte("key2"), []byte("key3")},
			[]byte("key2"),
			nil,
			[][]byte{[]byte("key2"), []byte("key3")},
		},

		{
			[][]byte{[]byte("key1"), []byte("key2"), []byte("key3")},
			nil,
			nil,
			[][]byte{[]byte("key1"), []byte("key2"), []byte("key3")},
		},

		{
			nil,
			nil,
			nil,
			nil,
		},

		{
			nil,
			[]byte("key1"),
			nil,
			nil,
		},

		{
			nil,
			nil,
			[]byte("key1"),
			nil,
		},
	}

	for _, tt := range tests {
		func() {
			defer leaktest.AfterTest(t)()
			fs := vfs.GetTestFS()
			defer vfs.ReportLeakedFD(fs, t)
			for name, factory := range factories {
				t.Run(name, func(t *testing.T) {
					s := factory(fs, t)
					defer s.Close()
					for _, key := range tt.keys {
						assert.NoError(t, s.Set(key, []byte("v"), true))
					}
					var result [][]byte
					assert.NoError(t, s.Scan(tt.start, tt.end, func(k, v []byte) (bool, error) {
						result = append(result, k)
						return true, nil
					}, true))
					assert.Equal(t, tt.result, result)
				})
			}
		}()
	}
}

func TestRangeDelete(t *testing.T) {
	tests := []struct {
		keys   [][]byte
		start  []byte
		end    []byte
		result [][]byte
	}{
		{
			[][]byte{[]byte("key1"), []byte("key2"), []byte("key3")},
			[]byte("key1"),
			[]byte("key3"),
			[][]byte{[]byte("key3")},
		},

		{
			[][]byte{[]byte("key1"), []byte("key2"), []byte("key3")},
			nil,
			[]byte("key3"),
			[][]byte{[]byte("key3")},
		},

		{
			[][]byte{[]byte("key1"), []byte("key2"), []byte("key3")},
			[]byte("key2"),
			nil,
			[][]byte{[]byte("key1")},
		},

		{
			[][]byte{[]byte("key1"), []byte("key2"), []byte("key3")},
			nil,
			nil,
			nil,
		},

		{
			nil,
			nil,
			nil,
			nil,
		},

		{
			nil,
			[]byte("key1"),
			nil,
			nil,
		},

		{
			nil,
			nil,
			[]byte("key1"),
			nil,
		},
	}

	for _, tt := range tests {
		func() {
			defer leaktest.AfterTest(t)()
			fs := vfs.GetTestFS()
			defer vfs.ReportLeakedFD(fs, t)
			for name, factory := range factories {
				t.Run(name, func(t *testing.T) {
					s := factory(fs, t)
					defer s.Close()
					for _, key := range tt.keys {
						assert.NoError(t, s.Set(key, []byte("v"), true))
					}
					assert.NoError(t, s.RangeDelete(tt.start, tt.end, true))
					var result [][]byte
					assert.NoError(t, s.Scan(nil, nil, func(k, v []byte) (bool, error) {
						result = append(result, k)
						return true, nil
					}, true))
					assert.Equal(t, tt.result, result)
				})
			}
		}()
	}
}
