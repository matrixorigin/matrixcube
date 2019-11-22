package main

import (
	"flag"
	"path/filepath"
	"time"

	"github.com/deepfabric/beehive"
	"github.com/deepfabric/beehive/redis"
	"github.com/deepfabric/beehive/server"
	"github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/beehive/storage/nemo"
	"github.com/deepfabric/prophet"
	"github.com/fagongzi/log"
)

var (
	addr = flag.String("addr", "127.0.0.1:6379", "svr")
	data = flag.String("data", "", "data path")
	wait = flag.Int("wait", 0, "wait")
)

func main() {
	log.InitLog()
	prophet.SetLogger(log.NewLoggerWithPrefix("[prophet]"))

	if *wait > 0 {
		time.Sleep(time.Second * time.Duration(*wait))
	}

	nemoStorage, err := nemo.NewStorage(filepath.Join(*data, "nemo"))
	if err != nil {
		log.Fatalf("create nemo failed with %+v", err)
	}

	store, err := beehive.CreateRaftStoreFromFile(*data, []storage.MetadataStorage{nemoStorage},
		[]storage.DataStorage{nemoStorage})
	if err != nil {
		log.Fatalf("failed parse with %+v", err)
	}

	app := server.NewApplication(server.Cfg{
		Addr:    *addr,
		Store:   store,
		Handler: redis.NewHandler(store),
	})
	err = app.Start()
	if err != nil {
		log.Fatalf("failed with %+v", err)
	}

	select {}
}
