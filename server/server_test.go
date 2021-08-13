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

package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/fagongzi/log"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/pebble"
	"github.com/stretchr/testify/assert"
)

func TestClusterStartAndStop(t *testing.T) {
	c, closer := createDiskDataStorageCluster(t)
	defer closer()

	c.RaftCluster.WaitShardByCount(t, 1, time.Second*10)

	app := c.Applications[0]
	resp, err := app.Exec(&testRequest{
		Op:    "SET",
		Key:   "key",
		Value: "value",
	}, 10*time.Second)
	assert.NoError(t, err)
	assert.Equal(t, "OK", string(resp))

	value, err := app.Exec(&testRequest{
		Op:  "GET",
		Key: "key",
	}, 10*time.Second)
	assert.NoError(t, err)
	assert.Equal(t, "value", string(value))
}

func TestIssue84(t *testing.T) {
	// issue 84, lost event notification after cluster restart
	// See https://github.com/matrixorigin/matrixcube/issues/84
	c, closer := createDiskDataStorageCluster(t)
	app := c.Applications[0]
	cmdSet := testRequest{
		Op:    "SET",
		Key:   "key",
		Value: "value",
	}
	resp, err := app.Exec(&cmdSet, 10*time.Second)

	assert.NoError(t, err)
	assert.Equal(t, "OK", string(resp))
	closer()

	// restart
	fn := func(i int) {
		c, closer := createDiskDataStorageCluster(t, raftstore.WithTestClusterRecreate(false))
		defer closer()

		st := time.Now()
		app = c.Applications[0]
		value, err := app.Exec(&testRequest{
			Op:  "GET",
			Key: "key",
		}, 10*time.Second)
		assert.NoErrorf(t, err, "cost %+v, %p", time.Since(st), c.RaftCluster.GetWatcher(0))
		assert.Equal(t, "value", string(value))
		if err != nil {
			log.Fatalf("failed %d", i)
		}
	}

	for i := 0; i < 1; i++ {
		fn(i)
	}
}

func createDiskDataStorageCluster(t *testing.T, opts ...raftstore.TestClusterOption) (*TestApplicationCluster, func()) {
	var storages []storage.DataStorage
	var metaStorages []storage.MetadataStorage
	opts = append(opts, raftstore.WithTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
		s, err := pebble.NewStorage(fmt.Sprintf("%s/pebble-data", cfg.DataPath))
		assert.NoError(t, err)
		storages = append(storages, s)
		cfg.Storage.DataStorageFactory = func(group, shardID uint64) storage.DataStorage {
			return s
		}
		cfg.Storage.ForeachDataStorageFunc = func(cb func(storage.DataStorage)) {
			for _, s := range storages {
				cb(s)
			}
		}

		sm, err := pebble.NewStorage(fmt.Sprintf("%s/pebble-meta", cfg.DataPath))
		assert.NoError(t, err)
		cfg.Storage.MetaStorage = sm
		metaStorages = append(metaStorages, sm)
	}))

	c := NewTestApplicationCluster(t, func(i int, store raftstore.Store) Cfg {
		h := &testHandler{
			store: store,
		}
		store.RegisterWriteFunc(1, h.set)
		store.RegisterReadFunc(2, h.get)
		return Cfg{
			Addr:    fmt.Sprintf("127.0.0.1:808%d", i),
			Store:   store,
			Handler: h,
		}
	}, opts...)

	c.Start()
	c.RaftCluster.WaitShardByCount(t, 1, time.Second*10)
	return c, func() {
		c.Stop()
		for _, s := range storages {
			assert.NoError(t, s.(*pebble.Storage).Close())
		}

		for _, s := range metaStorages {
			assert.NoError(t, s.(*pebble.Storage).Close())
		}
	}
}
