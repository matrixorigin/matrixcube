package raftstore

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/deepfabric/beehive/pb/bhmetapb"
	"github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/beehive/storage/mem"
	"github.com/stretchr/testify/assert"
)

var ports uint64 = 10000
var lock sync.Mutex

func createTestStore(name string) *store {
	lock.Lock()
	defer lock.Unlock()

	data := filepath.Join("/tmp/beehive/raftstore", name)
	os.RemoveAll(data)
	os.MkdirAll(data, os.ModeDir)

	p1 := ports
	ports++

	p2 := ports
	ports++

	m := mem.NewStorage()
	return NewStore(Cfg{
		Name:            name,
		RaftAddr:        fmt.Sprintf("127.0.0.1:%d", p1),
		RPCAddr:         fmt.Sprintf("127.0.0.1:%d", p2),
		MetadataStorage: m,
		DataStorages:    []storage.DataStorage{m},
	}, WithDataPath(data)).(*store)
}

func TestAddShardsWithHandle(t *testing.T) {
	s := createTestStore("s1")
	s.Start()

	c := make(chan struct{})
	s.opts.shardAddHandleFunc = func(shard bhmetapb.Shard) error {
		c <- struct{}{}
		return nil
	}

	err := s.AddShards(bhmetapb.Shard{
		Group:         1,
		LeastReplicas: 1,
	})
	assert.NoError(t, err, "TestAddShardsWithHandle failed")

	select {
	case <-c:
		return
	case <-time.After(time.Second * 12):
		assert.Fail(t, "TestAddShardsWithHandle failed timeout")
	}
}

func TestAddShards(t *testing.T) {
	s := createTestStore("s1")
	s.Start()

	err := s.AddShards(bhmetapb.Shard{
		Group: 1,
	})
	assert.NoError(t, err, "TestAddShard failed")

	err = s.AddShards(bhmetapb.Shard{
		Group: 1,
	})
	assert.NoError(t, err, "TestAddShard failed")

	err = s.AddShards(bhmetapb.Shard{
		Group: 1,
	}, bhmetapb.Shard{
		Group: 2,
	})
	assert.NoError(t, err, "TestAddShard failed")

	c := 0
	s.foreachPR(func(*peerReplica) bool {
		c++
		return true
	})
	assert.Equal(t, 3, c, "TestAddShard failed")
}

func TestHasGap(t *testing.T) {
	assert.True(t, hasGap(bhmetapb.Shard{}, bhmetapb.Shard{}), "TestHasGap failed")
	assert.True(t, hasGap(bhmetapb.Shard{Start: []byte("a")}, bhmetapb.Shard{Start: []byte("b")}), "TestHasGap failed")
	assert.False(t, hasGap(bhmetapb.Shard{Start: []byte("a"), End: []byte("b")}, bhmetapb.Shard{Start: []byte("b")}), "TestHasGap failed")
	assert.True(t, hasGap(bhmetapb.Shard{Start: []byte("a"), End: []byte("c")}, bhmetapb.Shard{Start: []byte("b"), End: []byte("c")}), "TestHasGap failed")
	assert.False(t, hasGap(bhmetapb.Shard{}, bhmetapb.Shard{Group: 2}), "TestHasGap failed")
}
