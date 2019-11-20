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
	invalidCommandResp = protoc.MustMarshal(&redispb.RedisResponse{Type: redispb.ErrorResp, ErrorResult: []byte("invalid command")})
	statusResp         = protoc.MustMarshal(&redispb.RedisResponse{Type: redispb.StatusResp, StatusResult: []byte("OK")})
	emptyBulkResp      = protoc.MustMarshal(&redispb.RedisResponse{Type: redispb.BulkResp})
)

type handler struct {
	store raftstore.Store
	cmds  map[string]uint64
	types map[string]raftcmdpb.CMDType
}

// NewHandler returns a redis server handler
func NewHandler(store raftstore.Store) server.Handler {
	h := &handler{
		store: store,
		cmds:  make(map[string]uint64),
		types: make(map[string]raftcmdpb.CMDType),
	}

	h.initSupportCMDs()
	return h
}

func (h *handler) initSupportCMDs() {
	h.AddWriteFunc("set", set, h.set)
	h.AddWriteFunc("incrBy", incrBy, h.incrBy)
	h.AddWriteFunc("incr", incr, h.incr)
	h.AddWriteFunc("decrby", decrby, h.decrby)
	h.AddWriteFunc("decr", decr, h.decr)
	h.AddWriteFunc("getset", getset, h.getset)
	h.AddWriteFunc("append", append, h.append)
	h.AddWriteFunc("setnx", setnx, h.setnx)
	h.AddWriteFunc("linsert", linsert, h.linsert)
	h.AddWriteFunc("lpop", lpop, h.lpop)
	h.AddWriteFunc("lpush", lpush, h.lpush)
	h.AddWriteFunc("lpushx", lpushx, h.lpushx)
	h.AddWriteFunc("lrem", lrem, h.lrem)
	h.AddWriteFunc("lset", lset, h.lset)
	h.AddWriteFunc("ltrim", ltrim, h.ltrim)
	h.AddWriteFunc("rpop", rpop, h.rpop)
	h.AddWriteFunc("rpush", rpush, h.rpush)
	h.AddWriteFunc("rpushx", rpushx, h.rpushx)
	h.AddWriteFunc("sadd", sadd, h.sadd)
	h.AddWriteFunc("srem", srem, h.srem)
	h.AddWriteFunc("scard", scard, h.scard)
	h.AddWriteFunc("spop", spop, h.spop)
	h.AddWriteFunc("zadd", zadd, h.zadd)
	h.AddWriteFunc("zcard", zcard, h.zcard)
	h.AddWriteFunc("zincrby", zincrby, h.zincrby)
	h.AddWriteFunc("zrem", zrem, h.zrem)
	h.AddWriteFunc("zremrangebylex", zremrangebylex, h.zremrangebylex)
	h.AddWriteFunc("zremrangebyrank", zremrangebyrank, h.zremrangebyrank)
	h.AddWriteFunc("zremrangebyscore", zremrangebyscore, h.zremrangebyscore)
	h.AddWriteFunc("zscore", zscore, h.zscore)
	h.AddWriteFunc("hset", hset, h.hset)
	h.AddWriteFunc("hdel", hdel, h.hdel)
	h.AddWriteFunc("hmset", hmset, h.hmset)
	h.AddWriteFunc("hsetnx", hsetnx, h.hsetnx)
	h.AddWriteFunc("hincrby", hincrby, h.hincrby)

	h.AddReadFunc("get", get, h.get)
	h.AddReadFunc("strlen", strlen, h.strlen)
	h.AddReadFunc("lindex", lindex, h.lindex)
	h.AddReadFunc("llen", llen, h.llen)
	h.AddReadFunc("lrange", lrange, h.lrange)
	h.AddReadFunc("smembers", smembers, h.smembers)
	h.AddReadFunc("sismember", sismember, h.sismember)
	h.AddReadFunc("zcount", zcount, h.zcount)
	h.AddReadFunc("zlexcount", zlexcount, h.zlexcount)
	h.AddReadFunc("zrange", zrange, h.zrange)
	h.AddReadFunc("zrangebylex", zrangebylex, h.zrangebylex)
	h.AddReadFunc("zrangebyscore", zrangebyscore, h.zrangebyscore)
	h.AddReadFunc("zrank", zrank, h.zrank)
	h.AddReadFunc("hget", hget, h.hget)
	h.AddReadFunc("hexists", hexists, h.hexists)
	h.AddReadFunc("hkeys", hkeys, h.hkeys)
	h.AddReadFunc("hvals", hvals, h.hvals)
	h.AddReadFunc("hgetall", hgetall, h.hgetall)
	h.AddReadFunc("hscanget", hscanget, h.hscanget)
	h.AddReadFunc("hlen", hlen, h.hlen)
	h.AddReadFunc("hmget", hmget, h.hmget)
	h.AddReadFunc("hstrlen", hstrlen, h.hstrlen)
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

func (h *handler) AddReadFunc(cmd string, cmdType uint64, cb raftstore.ReadCommandFunc) {
	h.cmds[strings.ToUpper(cmd)] = cmdType
	h.types[strings.ToUpper(cmd)] = raftcmdpb.Read
	h.store.RegisterReadFunc(cmdType, cb)
}

func (h *handler) AddWriteFunc(cmd string, cmdType uint64, cb raftstore.WriteCommandFunc) {
	h.cmds[strings.ToUpper(cmd)] = cmdType
	h.types[strings.ToUpper(cmd)] = raftcmdpb.Write
	h.store.RegisterWriteFunc(cmdType, cb)
}

func (h *handler) getRedisKV(shard uint64) nemo.RedisKV {
	return h.store.DataStorage(shard).(*nemo.Storage).RedisKV()
}

func (h *handler) getRedisSet(shard uint64) nemo.RedisSet {
	return h.store.DataStorage(shard).(*nemo.Storage).RedisSet()
}

func (h *handler) getRedisZSet(shard uint64) nemo.RedisZSet {
	return h.store.DataStorage(shard).(*nemo.Storage).RedisZSet()
}

func (h *handler) getRedisHash(shard uint64) nemo.RedisHash {
	return h.store.DataStorage(shard).(*nemo.Storage).RedisHash()
}

func (h *handler) getRedisList(shard uint64) nemo.RedisList {
	return h.store.DataStorage(shard).(*nemo.Storage).RedisList()
}

func errorResp(err error) []byte {
	return protoc.MustMarshal(&redispb.RedisResponse{
		Type:        redispb.ErrorResp,
		ErrorResult: []byte(err.Error()),
	})
}
