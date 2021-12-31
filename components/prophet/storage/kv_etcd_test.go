// Copyright 2020 PingCAP, Inc.
// Modifications copyright (C) 2021 MatrixOrigin.
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
	"testing"
	time "time"

	"github.com/matrixorigin/matrixcube/components/prophet/election"
	"github.com/matrixorigin/matrixcube/components/prophet/mock"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/stretchr/testify/assert"
)

func TestEtcdKV(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	e, err := election.NewElector(client)
	assert.NoError(t, err, "TestLoadResources failed")
	ls := e.CreateLeadship("prophet", "node1", "node1", true, func(string) bool { return true }, func(string) bool { return true })
	defer ls.Stop()

	ls.ElectionLoop()
	time.Sleep(time.Millisecond * 200)

	kv := NewEtcdKV("/root", client, ls)

	key1 := "key/1"
	key2 := "key/2"
	key3 := "key/3"

	assert.NoError(t, kv.Save(key1, key1))

	value, err := kv.Load(key1)
	assert.NoError(t, err)
	assert.Equal(t, key1, value)

	value, err = kv.Load(key2)
	assert.NoError(t, err)
	assert.Empty(t, value)

	assert.NoError(t, kv.Remove(key1))
	value, err = kv.Load(key1)
	assert.NoError(t, err)
	assert.Empty(t, value)

	kv.Remove(key1)
	kv.Remove(key2)
	kv.Remove(key3)
	keys, values, err := kv.LoadRange(key1, key3, 1)
	assert.NoError(t, err)
	assert.Empty(t, keys)
	assert.Empty(t, values)
	kv.Save(key1, key1)
	kv.Save(key2, key2)
	kv.Save(key3, key3)
	keys, values, err = kv.LoadRange(key1, key3, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(keys))
	assert.Equal(t, key1, keys[0])
	assert.Equal(t, 1, len(values))
	keys, values, err = kv.LoadRange(key1, key3, 3)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(keys))
	assert.Equal(t, 2, len(values))

	cnt, err := kv.CountRange(key1, key3)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), cnt)
	cnt, err = kv.CountRange("key/", util.GetPrefixRangeEnd("key/"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), cnt)

	kv.Remove(key1)
	kv.Remove(key2)
	kv.Remove(key3)
	ok, old, err := kv.SaveIfNotExists(key1, key1, nil)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Empty(t, old)
	ok, old, err = kv.SaveIfNotExists(key1, key1, nil)
	assert.NoError(t, err)
	assert.False(t, ok)
	assert.Equal(t, key1, old)

	kv.Remove(key1)
	kv.Remove(key2)
	kv.Remove(key3)
	kv.Save(key3, key3)
	b := &Batch{}
	b.SaveKeys = append(b.SaveKeys, key2)
	b.SaveValues = append(b.SaveValues, key2)
	b.RemoveKeys = append(b.RemoveKeys, key3)
	ok, old, err = kv.SaveIfNotExists(key1, key1, b)
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Empty(t, old)
	value, err = kv.Load(key2)
	assert.NoError(t, err)
	assert.Equal(t, key2, value)
	value, err = kv.Load(key3)
	assert.NoError(t, err)
	assert.Empty(t, value)

	kv.Save(key1, key1)
	ok, err = kv.RemoveIfValueMatched(key1, key2)
	assert.NoError(t, err)
	assert.False(t, ok)
	ok, err = kv.RemoveIfValueMatched(key1, key1)
	assert.NoError(t, err)
	assert.True(t, ok)
	value, err = kv.Load(key1)
	assert.NoError(t, err)
	assert.Empty(t, value)

	n := uint64(idBatch) + 1
	for i := uint64(1); i <= n; i++ {
		id, err := kv.AllocID()
		assert.NoError(t, err)
		assert.Equal(t, i, id)
	}
}
