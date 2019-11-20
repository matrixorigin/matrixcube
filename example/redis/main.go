package main

import (
	"flag"
	"fmt"
	"path/filepath"
	"time"

	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/beehive/redis"
	"github.com/deepfabric/beehive/server"
	"github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/beehive/storage/nemo"
	"github.com/deepfabric/prophet"
	redisProtoc "github.com/fagongzi/goetty/protocol/redis"
	"github.com/fagongzi/log"
)

var (
	name     = flag.String("name", "n1", "node name")
	addr     = flag.String("addr", "127.0.0.1:6379", "svr")
	raftAddr = flag.String("raft-addr", "127.0.0.1:10000", "svr")
	rpcAddr  = flag.String("rpc-addr", "127.0.0.1:10001", "svr")
	data     = flag.String("data", "", "data")
)

func main() {
	log.InitLog()
	prophet.SetLogger(log.NewLoggerWithPrefix("[prophet]"))

	nemoStorage, err := nemo.NewStorage(filepath.Join(*data, "nemo"))
	if err != nil {
		log.Fatalf("create nemo failed with %+v", err)
	}

	store := raftstore.NewStore(raftstore.Cfg{
		Name:             *name,
		RaftAddr:         *raftAddr,
		RPCAddr:          *rpcAddr,
		MetadataStorages: []storage.MetadataStorage{nemoStorage},
		DataStorages:     []storage.DataStorage{nemoStorage},
	}, raftstore.WithDataPath(*data))

	app := server.NewApplication(server.Cfg{
		Addr:    *addr,
		Store:   store,
		Handler: redis.NewHandler(store),
	})
	err = app.Start()
	if err != nil {
		log.Fatalf("failed with %+v", err)
	}

	time.Sleep(time.Second * 10)

	for {
		var cmd redisProtoc.Command
		cmd = append(cmd, []byte("set"), []byte("appkey"), []byte(fmt.Sprintf("%d", time.Now().Unix())))
		value, err := app.Exec(cmd, time.Second)
		log.Infof("%+v,%+v", value, err)
		time.Sleep(time.Second)
	}
}
