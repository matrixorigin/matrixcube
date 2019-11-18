package server

import (
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/pb/redispb"
	"github.com/deepfabric/beehive/proxy"
	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/beehive/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/protocol/redis"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/uuid"
)

var (
	logger = log.NewLoggerWithPrefix("[beehive-redis-server]")
)

// RedisServer redis server
type RedisServer struct {
	cfg      Cfg
	redis    *goetty.Server
	sessions sync.Map // id -> *util.Session

	store       raftstore.Store
	shardsProxy proxy.ShardsProxy
	cmds        map[string]uint64
	types       map[string]raftcmdpb.CMDType
}

// NewRedisServer returns a redis server
func NewRedisServer(cfg Cfg) *RedisServer {
	s := &RedisServer{
		cfg:   cfg,
		cmds:  make(map[string]uint64),
		types: make(map[string]raftcmdpb.CMDType),
	}
	s.redis = goetty.NewServer(cfg.RedisAddr,
		goetty.WithServerDecoder(redis.NewRedisDecoder()),
		goetty.WithServerEncoder(s))
	s.store = raftstore.NewStore(s.cfg.RaftCfg, s.cfg.Options...)
	return s
}

// Start start the redis server
func (s *RedisServer) Start() error {
	s.store.Start()
	sp, err := proxy.NewShardsProxyWithStore(s.store, s.done, s.doneError)
	if err != nil {
		return err
	}

	s.shardsProxy = sp
	errorC := make(chan error)
	go func() {
		errorC <- s.redis.Start(s.doConnection)
	}()

	select {
	case <-s.redis.Started():
		return nil
	case err := <-errorC:
		return err
	}
}

// Stop stop redis server
func (s *RedisServer) Stop() {
	s.redis.Stop()
}

// AddReadCmd add read cmd
func (s *RedisServer) AddReadCmd(cmd string, cmdType uint64, cb raftstore.CommandFunc) {
	s.cmds[strings.ToUpper(cmd)] = cmdType
	s.types[strings.ToUpper(cmd)] = raftcmdpb.Read
	s.store.RegisterReadFunc(cmdType, cb)
}

// AddWriteCmd add write cmd
func (s *RedisServer) AddWriteCmd(cmd string, cmdType uint64, cb raftstore.CommandFunc) {
	s.cmds[strings.ToUpper(cmd)] = cmdType
	s.types[strings.ToUpper(cmd)] = raftcmdpb.Write
	s.store.RegisterWriteFunc(cmdType, cb)
}

func (s *RedisServer) doConnection(conn goetty.IOSession) error {
	rs := util.NewSession(conn, nil)
	s.sessions.Store(rs.ID, rs)
	defer func() {
		rs.Close()
		s.sessions.Delete(rs.ID)
	}()

	for {
		r, err := conn.Read()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			logger.Errorf("read from cli %s failed, errors\n %+v",
				rs.Addr,
				err)
			return err
		}

		cmd := r.(redis.Command)
		req := pb.AcquireRequest()

		if len(cmd.Args()) == 0 {
			resp := &raftcmdpb.Response{}
			resp.Error.Message = "not support"
			rs.OnResp(resp)
			continue
		}

		req.Key = cmd.Args()[0]

		if len(cmd.Args()) > 1 {
			args := redispb.RedisArgs{}
			args.Args = cmd.Args()[1:]
			req.Cmd = protoc.MustMarshal(&args)
		}

		cmdName := strings.ToUpper(hack.SliceToString(cmd.Cmd()))
		req.CustemType = s.cmds[cmdName]
		req.Type = s.types[cmdName]
		req.ID = uuid.NewV4().Bytes()
		req.SID = rs.ID.(int64)

		err = s.shardsProxy.Dispatch(req)
		if err != nil {
			resp := &raftcmdpb.Response{}
			resp.Error.Message = err.Error()
			rs.OnResp(resp)
		}
	}
}

func (s *RedisServer) done(resp *raftcmdpb.Response) {
	if value, ok := s.sessions.Load(resp.SID); ok {
		value.(*util.Session).OnResp(resp)
	}
}

func (s *RedisServer) doneError(resp *raftcmdpb.Request, err error) {
	if value, ok := s.sessions.Load(resp.SID); ok {
		resp := &raftcmdpb.Response{}
		resp.Error.Message = err.Error()
		value.(*util.Session).OnResp(resp)
	}
}

// Encode redis encode
func (s *RedisServer) Encode(data interface{}, out *goetty.ByteBuf) error {
	value := data.(*raftcmdpb.Response)
	if value.Error.Message != "" {
		redis.WriteError(hack.StringToSlice(value.Error.Message), out)
		return nil
	}

	resp := redispb.RedisResponse{}
	protoc.MustUnmarshal(&resp, value.Value)

	if len(resp.BulkResult) > 0 || resp.HasEmptyBulkResult {
		redis.WriteBulk(resp.BulkResult, out)
		return nil
	}

	if len(resp.FvPairArrayResult) > 0 || resp.HasEmptyFVPairArrayResult {
		writeFVPairArray(resp.FvPairArrayResult, out)
		return nil
	}

	if resp.IntegerResult > 0 {
		redis.WriteInteger(resp.IntegerResult, out)
		return nil
	}

	if len(resp.ScorePairArrayResult) > 0 || resp.HasEmptyScorePairArrayResult {
		writeScorePairArray(resp.ScorePairArrayResult, resp.Withscores, out)
		return nil
	}

	if len(resp.SliceArrayResult) > 0 || resp.HasEmptySliceArrayResult {
		redis.WriteSliceArray(resp.SliceArrayResult, out)
		return nil
	}

	if len(resp.StatusResult) > 0 {
		redis.WriteStatus(resp.StatusResult, out)
		return nil
	}

	return nil
}

func writeFVPairArray(lst []redispb.FVPair, buf *goetty.ByteBuf) {
	buf.WriteByte('*')
	if len(lst) == 0 {
		buf.Write(redis.NullArray)
		buf.Write(redis.Delims)
	} else {
		buf.Write(hack.StringToSlice(strconv.Itoa(len(lst) * 2)))
		buf.Write(redis.Delims)

		for i := 0; i < len(lst); i++ {
			redis.WriteBulk(lst[i].Field, buf)
			redis.WriteBulk(lst[i].Value, buf)
		}
	}
}

func writeScorePairArray(lst []redispb.ScorePair, withScores bool, buf *goetty.ByteBuf) {
	buf.WriteByte('*')
	if len(lst) == 0 {
		buf.Write(redis.NullArray)
		buf.Write(redis.Delims)
	} else {
		if withScores {
			buf.Write(hack.StringToSlice(strconv.Itoa(len(lst) * 2)))
			buf.Write(redis.Delims)
		} else {
			buf.Write(hack.StringToSlice(strconv.Itoa(len(lst))))
			buf.Write(redis.Delims)
		}

		for i := 0; i < len(lst); i++ {
			redis.WriteBulk(lst[i].Member, buf)

			if withScores {
				redis.WriteBulk(format.Float64ToString(lst[i].Score), buf)
			}
		}
	}
}
