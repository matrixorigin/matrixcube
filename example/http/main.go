package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/deepfabric/beehive"
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/beehive/server"
	"github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/beehive/storage/mem"
	"github.com/deepfabric/prophet"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
)

var (
	addr = flag.String("addr", "127.0.0.1", "svr")
	data = flag.String("data", "", "data path")
	wait = flag.Int("wait", 0, "wait")
)

func main() {
	log.InitLog()
	prophet.SetLogger(log.NewLoggerWithPrefix("[prophet]"))

	if *wait > 0 {
		time.Sleep(time.Second * time.Duration(*wait))
	}

	memStorage := mem.NewStorage()
	store, err := beehive.CreateRaftStoreFromFile(*data, []storage.MetadataStorage{memStorage},
		[]storage.DataStorage{memStorage})
	if err != nil {
		log.Fatalf("failed parse with %+v", err)
	}

	app := server.NewApplication(server.Cfg{
		Store:          store,
		Handler:        newHTTPHandler(memStorage, store),
		ExternalServer: true,
	})
	err = app.Start()
	if err != nil {
		log.Fatalf("failed with %+v", err)
	}

	s := newServer(app)
	log.Fatalf("start http server failed at %s with %+v",
		*addr,
		http.ListenAndServe(*addr, s))
}

type request struct {
	Op    string `json:"json:op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type httpServer struct {
	app *server.Application
}

func newServer(app *server.Application) *httpServer {
	return &httpServer{
		app: app,
	}
}

func (h *httpServer) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	request := &request{}

	op := req.URL.Path[1:]

	switch op {
	case "set":
		request.Op = op
		request.Key = req.URL.Query().Get("key")
		request.Value = req.URL.Query().Get("value")
	case "delete":
		request.Op = op
		request.Key = req.URL.Query().Get("key")
	case "get":
		request.Op = op
		request.Key = req.URL.Query().Get("key")
	default:
		rw.WriteHeader(404)
		return
	}

	value, err := h.app.Exec(request, time.Second*10)
	if err != nil {
		rw.Write([]byte(err.Error()))
		rw.WriteHeader(500)
		return
	}

	rw.Write(value)
}

type httpHandler struct {
	kv    *mem.Storage
	store raftstore.Store

	cmds  map[string]uint64
	types map[string]raftcmdpb.CMDType
}

func newHTTPHandler(kv *mem.Storage, store raftstore.Store) server.Handler {
	h := &httpHandler{
		store: store,
		kv:    kv,
		cmds:  make(map[string]uint64),
		types: make(map[string]raftcmdpb.CMDType),
	}

	h.AddWriteFunc("set", 1, h.set)
	h.AddWriteFunc("delete", 2, h.delete)
	h.AddReadFunc("get", 3, h.get)

	return h
}

func (h *httpHandler) BuildRequest(req *raftcmdpb.Request, msg interface{}) error {
	cmd := msg.(*request)

	cmdName := strings.ToUpper(cmd.Op)
	if _, ok := h.cmds[cmdName]; !ok {
		return fmt.Errorf("%s not support", cmd.Op)
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	req.Key = []byte(cmd.Key)
	req.CustemType = h.cmds[cmdName]
	req.Type = h.types[cmdName]
	req.Cmd = data
	return nil
}

func (h *httpHandler) Codec() (goetty.Decoder, goetty.Encoder) {
	return nil, nil
}

func (h *httpHandler) AddReadFunc(cmd string, cmdType uint64, cb raftstore.ReadCommandFunc) {
	h.cmds[strings.ToUpper(cmd)] = cmdType
	h.types[strings.ToUpper(cmd)] = raftcmdpb.Read
	h.store.RegisterReadFunc(cmdType, cb)
}

func (h *httpHandler) AddWriteFunc(cmd string, cmdType uint64, cb raftstore.WriteCommandFunc) {
	h.cmds[strings.ToUpper(cmd)] = cmdType
	h.types[strings.ToUpper(cmd)] = raftcmdpb.Write
	h.store.RegisterWriteFunc(cmdType, cb)
}

func (h *httpHandler) set(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()

	cmd := request{}
	err := json.Unmarshal(req.Cmd, &cmd)
	if err != nil {
		resp.Value = []byte(err.Error())
		return 0, 0, resp
	}

	err = h.kv.Set(req.Key, []byte(cmd.Value))
	if err != nil {
		resp.Value = []byte(err.Error())
		return 0, 0, resp
	}

	writtenBytes := uint64(len(req.Key) + len(cmd.Value))
	changedBytes := int64(writtenBytes)

	resp.Value = []byte("OK")
	return writtenBytes, changedBytes, resp
}

func (h *httpHandler) delete(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()

	cmd := request{}
	err := json.Unmarshal(req.Cmd, &cmd)
	if err != nil {
		resp.Value = []byte(err.Error())
		return 0, 0, resp
	}

	err = h.kv.Delete(req.Key)
	if err != nil {
		resp.Value = []byte(err.Error())
		return 0, 0, resp
	}

	writtenBytes := uint64(0)
	changedBytes := -int64(len(req.Key) + len(cmd.Value))

	resp.Value = []byte("OK")
	return writtenBytes, changedBytes, resp
}

func (h *httpHandler) get(shard uint64, req *raftcmdpb.Request, buf *goetty.ByteBuf) *raftcmdpb.Response {
	resp := pb.AcquireResponse()

	cmd := request{}
	err := json.Unmarshal(req.Cmd, &cmd)
	if err != nil {
		resp.Value = []byte(err.Error())
		return resp
	}

	value, err := h.kv.Get(req.Key)
	if err != nil {
		resp.Value = []byte(err.Error())
		return resp
	}

	resp.Value = value
	return resp
}
