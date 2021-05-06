package raftstore

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/mem"
)

var (
	tmpDir = "/tmp/beehive"
)

func recreateTestTempDir() {
	os.RemoveAll(tmpDir)
	os.MkdirAll(tmpDir, os.ModeDir)
}

func newTestStore() (*store, func()) {
	recreateTestTempDir()

	dataStorage := mem.NewStorage()
	cfg := &config.Config{}
	cfg.DataPath = tmpDir
	cfg.Prophet.StorageNode = true
	cfg.Storage.MetaStorage = mem.NewStorage()
	cfg.Storage.DataStorageFactory = func(group, shardID uint64) storage.DataStorage {
		return dataStorage
	}
	cfg.Storage.ForeachDataStorageFunc = func(cb func(storage.DataStorage)) {
		cb(dataStorage)
	}

	s := NewStore(cfg).(*store)
	return s, func() {
		s.Stop()
	}
}

func newTestClusterStore(t *testing.T) *testCluster {
	recreateTestTempDir()

	c := &testCluster{t: t}
	for i := 0; i < 3; i++ {
		dataStorage := mem.NewStorage()
		cfg := &config.Config{}
		cfg.DataPath = fmt.Sprintf("%s/node-%d", tmpDir, i)
		cfg.RaftAddr = fmt.Sprintf("127.0.0.1:1000%d", i)
		cfg.ClientAddr = fmt.Sprintf("127.0.0.1:2000%d", i)

		cfg.Prophet.Name = fmt.Sprintf("node-%d", i)
		cfg.Prophet.StorageNode = true
		cfg.Prophet.RPCAddr = fmt.Sprintf("127.0.0.1:3000%d", i)
		if i != 0 {
			cfg.Prophet.EmbedEtcd.Join = "http://127.0.0.1:40000"
		}
		cfg.Prophet.EmbedEtcd.ClientUrls = fmt.Sprintf("http://127.0.0.1:4000%d", i)
		cfg.Prophet.EmbedEtcd.PeerUrls = fmt.Sprintf("http://127.0.0.1:5000%d", i)

		cfg.Storage.MetaStorage = mem.NewStorage()
		cfg.Storage.DataStorageFactory = func(group, shardID uint64) storage.DataStorage {
			return dataStorage
		}
		cfg.Storage.ForeachDataStorageFunc = func(cb func(storage.DataStorage)) {
			cb(dataStorage)
		}

		c.stores = append(c.stores, NewStore(cfg).(*store))
	}

	return c
}

type testCluster struct {
	t      *testing.T
	stores []*store
}

func (c *testCluster) start() {
	for idx, s := range c.stores {
		if idx == 2 {
			time.Sleep(time.Second * 5)
		}

		s.Start()
	}
}

func (c *testCluster) stop() {
	for _, s := range c.stores {
		s.Stop()
	}
}

func (c *testCluster) getPRCount(idx int) int {
	cnt := 0
	c.stores[idx].replicas.Range(func(k, v interface{}) bool {
		cnt++
		return true
	})
	return cnt
}
