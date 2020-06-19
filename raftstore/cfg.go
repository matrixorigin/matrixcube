package raftstore

import (
	"github.com/deepfabric/beehive/storage"
)

// Cfg the configuration of the raftstore
type Cfg struct {
	// Name the node name in the cluster
	Name string
	// RaftAddr raft addr for exchange raft message
	RaftAddr string
	// RPCAddr the RPC address to serve requests
	RPCAddr string
	// MetadataStorage storage that to store local raft state, log, and sharding metadata
	MetadataStorage storage.MetadataStorage
	// DataStorages storages that to store application's data. Beehive will
	// select the corresponding storage according to shard id
	DataStorages []storage.DataStorage
}
