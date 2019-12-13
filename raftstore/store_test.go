package raftstore

import (
	"fmt"
	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/beehive/storage/mem"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"sync"
	"testing"
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
		Name:             name,
		RaftAddr:         fmt.Sprintf("127.0.0.1:%d", p1),
		RPCAddr:          fmt.Sprintf("127.0.0.1:%d", p2),
		MetadataStorages: []storage.MetadataStorage{m},
		DataStorages:     []storage.DataStorage{m},
	}, WithDataPath(data)).(*store)
}

func TestAddShard(t *testing.T) {
	s := createTestStore("s1")
	s.Start()

	c := 0
	s.foreachPR(func(*peerReplica) bool {
		c++
		return true
	})
	assert.Equal(t, 1, c, "TestAddShard failed")

	err := s.AddShard(metapb.Shard{
		Group: 1,
	})
	assert.NoError(t, err, "TestAddShard failed")

}
