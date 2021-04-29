package raftstore

import (
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/mem"
)

// Name       string            `toml:"name" json:"name"`
// 	DataDir    string            `toml:"data-dir"`
// 	RPCAddr    string            `toml:"rpc-addr"`
// 	RPCTimeout typeutil.Duration `toml:"rpc-timeout"`

// 	// etcd configuration
// 	StorageNode  bool            `toml:"storage-node"`
// 	ExternalEtcd []string        `toml:"external-etcd"`
// 	EmbedEtcd    EmbedEtcdConfig `toml:"embed-etcd"`

// 	// LeaderLease time, if leader doesn't update its TTL
// 	// in etcd after lease time, etcd will expire the leader key
// 	// and other servers can campaign the leader again.
// 	// Etcd only supports seconds TTL, so here is second too.
// 	LeaderLease int64 `toml:"lease" json:"lease"`

// 	Schedule      ScheduleConfig      `toml:"schedule"`
// 	Replication   ReplicationConfig   `toml:"replication"`
// 	LabelProperty LabelPropertyConfig `toml:"label-property"`

var (
	prophetCfg = `

name = "node1"
data-dir = ""



	
# The node name in the cluster
name = "node1"

# The RPC address to serve requests
raftAddr = "127.0.0.1:10001"

# The RPC address to serve requests
rpcAddr = "127.0.0.1:10002"

groups = 4

[prophet]
# The application and prophet RPC address, send heartbeats, alloc id, watch event, etc. required
rpcAddr = "127.0.0.1:9527"

# Store cluster metedata
storeMetadata = true

# The embed etcd client address, required while storeMetadata is true
clientAddr = "127.0.0.1:2371"

# The embed etcd peer address, required while storeMetadata is true
peerAddr = "127.0.0.1:2381"
	`
)

func newTestStore() (Store, func()) {
	dataStorage := mem.NewStorage()
	cfg := &config.Config{}
	cfg.Prophet.StorageNode = true
	cfg.Storage.MetaStorage = mem.NewStorage()
	cfg.Storage.DataStorageFactory = func(group, shardID uint64) storage.DataStorage {
		return dataStorage
	}
	cfg.Storage.ForeachDataStorageFunc = func(cb func(storage.DataStorage)) {
		cb(dataStorage)
	}

	s := NewStore(cfg)
	return s, func() {
		s.Stop()
	}
}
