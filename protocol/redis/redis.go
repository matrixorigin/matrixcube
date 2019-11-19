package redis

import (
	"strings"

	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/pb/redispb"
	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/beehive/server"
	"github.com/deepfabric/beehive/storage/nemo"
	"github.com/fagongzi/goetty"
	redisProtoc "github.com/fagongzi/goetty/protocol/redis"
	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/protoc"
)

var (
	invalidCommand = []byte("invalid command")
)

type handler struct {
	store raftstore.Store
	cmds  map[string]uint64
	types map[string]raftcmdpb.CMDType
}

// NewHandler returns a redis server handler
func NewHandler(store raftstore.Store) server.Handler {
	return &handler{
		store: store,
		cmds:  make(map[string]uint64),
		types: make(map[string]raftcmdpb.CMDType),
	}
}

func (h *handler) BuildRequest(req *raftcmdpb.Request, data interface{}) error {
	cmd := data.(redisProtoc.Command)

	cmdName := strings.ToUpper(hack.SliceToString(cmd.Cmd()))
	if _, ok := h.cmds[cmdName]; !ok {
		return ErrCMDNotSupport
	}

	if len(cmd.Args()) == 0 {
		return ErrCMDNotSupport
	}

	req.Key = cmd.Args()[0]
	req.CustemType = h.cmds[cmdName]
	req.Type = h.types[cmdName]
	if len(cmd.Args()) > 1 {
		args := redispb.RedisArgs{}
		args.Args = cmd.Args()[1:]
		req.Cmd = protoc.MustMarshal(&args)
	}

	return nil
}

func (h *handler) Codec() (goetty.Decoder, goetty.Encoder) {
	return decoder, encoder
}

func (h *handler) AddReadFunc(cmd string, cmdType uint64, cb raftstore.CommandFunc) {
	h.cmds[strings.ToUpper(cmd)] = cmdType
	h.types[strings.ToUpper(cmd)] = raftcmdpb.Read
	h.store.RegisterReadFunc(cmdType, cb)
}

func (h *handler) AddWriteFunc(cmd string, cmdType uint64, cb raftstore.CommandFunc) {
	h.cmds[strings.ToUpper(cmd)] = cmdType
	h.types[strings.ToUpper(cmd)] = raftcmdpb.Write
	h.store.RegisterWriteFunc(cmdType, cb)
}

func (h *handler) getRedisKV(shard uint64) nemo.RedisKV {
	// return h.store.RedisKV()
	return nil
}
