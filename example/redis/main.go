package main

import (
	"flag"

	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/pb/redispb"
	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/beehive/server"
	"github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/beehive/util"
	"github.com/deepfabric/prophet"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/protoc"
)

var (
	name     = flag.String("name", "n1", "node name")
	addr     = flag.String("addr", "127.0.0.1:6379", "svr")
	raftAddr = flag.String("raft-addr", "127.0.0.1:10000", "svr")
	rpcAddr  = flag.String("rpc-addr", "127.0.0.1:10001", "svr")
	data     = flag.String("data", "", "data")
)

var (
	kv = util.NewKVTree()
)

func main() {
	log.InitLog()

	prophet.SetLogger(log.NewLoggerWithPrefix("[prophet]"))

	metaStore := storage.NewMemMetadataStorage()
	dataStore := storage.NewMemDataStorageWithKV(kv)

	cfg := server.Cfg{
		RedisAddr: *addr,
		RaftCfg: raftstore.Cfg{
			Name:             *name,
			RaftAddr:         *raftAddr,
			RPCAddr:          *rpcAddr,
			MetadataStorages: []storage.MetadataStorage{metaStore},
			DataStorages:     []storage.DataStorage{dataStore},
		},
		Options: []raftstore.Option{raftstore.WithDataPath(*data)},
	}

	s := server.NewRedisServer(cfg)
	s.AddReadCmd("get", 1, get)
	s.AddWriteCmd("set", 2, set)
	err := s.Start()
	if err != nil {
		log.Fatalf("failed with %+v")
	}

	select {}
}

func set(shard uint64, req *raftcmdpb.Request) *raftcmdpb.Response {
	args := &redispb.RedisArgs{}
	protoc.MustUnmarshal(args, req.Cmd)

	kv.Put(req.Key, args.Args[0])

	value := redispb.RedisResponse{}
	value.StatusResult = []byte("OK")

	return &raftcmdpb.Response{
		Value: protoc.MustMarshal(&value),
	}
}

func get(shard uint64, req *raftcmdpb.Request) *raftcmdpb.Response {
	value := kv.Get(req.Key)

	resp := redispb.RedisResponse{}
	resp.BulkResult = value
	resp.HasEmptyBulkResult = len(value) == 0

	return &raftcmdpb.Response{
		Value: protoc.MustMarshal(&resp),
	}
}
