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
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/storage/kv/pebble"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
)

var (
	dataDactories = map[string]func(vfs.FS, *testing.T) storage.BaseStorage{
		"memory": createDataMem,
		"pebble": createDataPebble,
	}
)

func createDataMem(fs vfs.FS, t *testing.T) storage.BaseStorage {
	return mem.NewStorage(fs)
}

func createDataPebble(fs vfs.FS, t *testing.T) storage.BaseStorage {
	path := filepath.Join(util.GetTestDir(), "pebble", fmt.Sprintf("%d", time.Now().UnixNano()))
	fs.RemoveAll(path)
	fs.MkdirAll(path, 0755)
	opts := &cpebble.Options{FS: vfs.NewPebbleFS(fs)}
	s, err := pebble.NewStorage(path, fs, opts)
	assert.NoError(t, err, "createDataPebble failed")
	return s
}

func TestSplitCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	for name, factory := range dataDactories {
		t.Run(name, func(t *testing.T) {
			s := factory(fs, t)
			defer s.Close()
			kv := s.(storage.KVStorage)
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

			assert.NoError(t, kv.Set(key1, value1, false))
			assert.NoError(t, kv.Set(key2, value2, false))
			assert.NoError(t, kv.Set(key3, value3, false))
			assert.NoError(t, kv.Set(key4, value4, false))

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
	defer leaktest.AfterTest(t)()
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)
	for name, factory := range dataDactories {
		t.Run(name, func(t *testing.T) {
			s1 := factory(fs, t)
			defer s1.Close()
			kv1 := s1.(storage.KVStorage)
			s2 := factory(fs, t)
			defer s2.Close()
			kv2 := s2.(storage.KVStorage)
			path := fmt.Sprintf("%s-snap", name)
			path = filepath.Join(util.GetTestDir(), path)
			fs.RemoveAll(path)
			err := fs.MkdirAll(path, 0755)
			assert.NoError(t, err, "TestCreateAndApply failed")

			key1 := []byte("key1")
			value1 := []byte("value1")

			key2 := []byte("key2")
			value2 := []byte("value2")

			assert.NoError(t, kv1.Set(key1, value1, false), "TestCreateAndApply failed")
			assert.NoError(t, kv1.Set(key2, value2, false), "TestCreateAndApply failed")

			shard := meta.Shard{Start: key1, End: key2}
			_, err = s1.CreateSnapshot(shard, path)
			assert.NoError(t, err, "TestCreateAndApply failed")

			key4 := []byte("key4")
			value4 := []byte("value4")
			assert.NoError(t, kv2.Set(key4, value4, false), "TestCreateAndApply failed")

			err = s2.ApplySnapshot(shard, path)
			assert.NoError(t, err, "TestCreateAndApply failed")

			value, err := kv2.Get(key2)
			assert.NoError(t, err, "TestCreateAndApply failed")
			assert.Equal(t, 0, len(value), "TestCreateAndApply failed")

			value, err = kv2.Get(key4)
			assert.NoError(t, err, "TestCreateAndApply failed")
			assert.Equal(t, len(value4), len(value), "TestCreateAndApply failed")

			value, err = kv2.Get(key1)
			assert.NoError(t, err, "TestCreateAndApply failed")
			assert.Equal(t, string(value1), string(value), "TestCreateAndApply failed")
		})
	}
}
