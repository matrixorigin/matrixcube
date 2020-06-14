package server

import (
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/proxy"
	"github.com/deepfabric/beehive/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/uuid"
)

var (
	logger = log.NewLoggerWithPrefix("[beehive-app]")
)

// Application a tcp application server
type Application struct {
	cfg         Cfg
	server      *goetty.Server
	sessions    sync.Map // id -> *util.Session
	shardsProxy proxy.ShardsProxy

	libaryCB sync.Map // id -> application cb
}

// NewApplication returns a tcp application server
func NewApplication(cfg Cfg) *Application {
	s := &Application{
		cfg: cfg,
	}

	if !cfg.ExternalServer {
		decoder, encoder := cfg.Handler.Codec()
		s.server = goetty.NewServer(cfg.Addr,
			goetty.WithServerDecoder(decoder),
			goetty.WithServerEncoder(encoder))
	}
	return s
}

// Start start the application server
func (s *Application) Start() error {
	s.cfg.Store.Start()
	sp, err := proxy.NewShardsProxyWithStore(s.cfg.Store, s.done, s.doneError)
	if err != nil {
		return err
	}

	s.shardsProxy = sp
	if s.cfg.ExternalServer {
		logger.Infof("using external server, ignore embed tcp server")
		return nil
	}

	logger.Infof("start embed tcp server")
	errorC := make(chan error)
	go func() {
		errorC <- s.server.Start(s.doConnection)
	}()

	select {
	case <-s.server.Started():
		return nil
	case err := <-errorC:
		return err
	}
}

// Stop stop redis server
func (s *Application) Stop() {
	if s.cfg.ExternalServer {
		return
	}
	s.server.Stop()
}

// Exec exec the request command
func (s *Application) Exec(cmd interface{}, timeout time.Duration) ([]byte, error) {
	return s.ExecWithGroup(cmd, 0, timeout)
}

// ExecWithGroup exec the request command
func (s *Application) ExecWithGroup(cmd interface{}, group uint64, timeout time.Duration) ([]byte, error) {
	completeC := make(chan interface{}, 1)
	closed := uint32(0)
	cb := func(cmd interface{}, resp []byte, err error) {
		if atomic.CompareAndSwapUint32(&closed, 0, 1) {
			if err != nil {
				completeC <- err
			} else {
				completeC <- resp
			}
			close(completeC)
		}
	}

	s.AsyncExecWithGroupAndTimeout(cmd, group, cb, timeout, nil)
	value := <-completeC
	switch value.(type) {
	case error:
		return nil, value.(error)
	default:
		return value.([]byte), nil
	}
}

// AsyncExec async exec the request command
func (s *Application) AsyncExec(cmd interface{}, cb func(interface{}, []byte, error), arg interface{}) {
	s.AsyncExecWithTimeout(cmd, cb, 0, arg)
}

// AsyncExecWithTimeout async exec the request, if the err is ErrTimeout means the request is timeout
func (s *Application) AsyncExecWithTimeout(cmd interface{}, cb func(interface{}, []byte, error), timeout time.Duration, arg interface{}) {
	s.AsyncExecWithGroupAndTimeout(cmd, 0, cb, timeout, arg)
}

// AsyncExecWithGroupAndTimeout async exec the request, if the err is ErrTimeout means the request is timeout
func (s *Application) AsyncExecWithGroupAndTimeout(cmd interface{}, group uint64, cb func(interface{}, []byte, error), timeout time.Duration, arg interface{}) {
	req := pb.AcquireRequest()
	req.ID = uuid.NewV4().Bytes()
	req.Group = group
	req.StopAt = time.Now().Add(timeout).Unix()

	err := s.cfg.Handler.BuildRequest(req, cmd)
	if err != nil {
		cb(arg, nil, err)
		pb.ReleaseRequest(req)
		return
	}

	s.libaryCB.Store(hack.SliceToString(req.ID), ctx{
		arg: arg,
		cb:  cb,
	})
	if timeout > 0 {
		util.DefaultTimeoutWheel().Schedule(timeout, s.execTimeout, req.ID)
	}

	err = s.shardsProxy.Dispatch(req)
	if err != nil {
		pb.ReleaseRequest(req)
		s.libaryCB.Delete(hack.SliceToString(req.ID))
		cb(arg, nil, err)
	}
}

// AsyncBroadcast broadcast to all current shards, and aggregate responses
func (s *Application) AsyncBroadcast(cmd interface{}, group uint64, cb func(interface{}, [][]byte, error), timeout time.Duration, arg interface{}, mustLeader bool) {
	max, shards, forwards, err := s.buildBroadcast(0, group, mustLeader)
	if err != nil {
		cb(arg, nil, err)
		return
	}

	if len(shards) == 0 {
		cb(arg, nil, nil)
		return
	}

	c := acquireBroadcastCtx()
	c.cb = cb
	c.arg = arg
	c.total = len(shards)
	c.completed = 0
	c.maxShard = max
	c.mustLeader = mustLeader
	c.group = group
	c.cmd = cmd
	c.timeout = timeout

	s.doBroadcast(c, max, shards, forwards)
}

func (s *Application) execTimeout(arg interface{}) {
	id := hack.SliceToString(arg.([]byte))
	if value, ok := s.libaryCB.Load(id); ok {
		s.libaryCB.Delete(id)
		value.(asyncCtx).resp(nil, proxy.ErrTimeout, false)
	}
}

func (s *Application) doConnection(conn goetty.IOSession) error {
	rs := util.NewSession(conn, nil)
	s.sessions.Store(rs.ID, rs)
	defer func() {
		rs.Close()
		s.sessions.Delete(rs.ID)
	}()

	for {
		cmd, err := conn.Read()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			logger.Errorf("read from cli %s failed, errors\n %+v",
				rs.Addr,
				err)
			return err
		}

		req := pb.AcquireRequest()
		req.ID = uuid.NewV4().Bytes()
		req.SID = rs.ID.(int64)

		err = s.cfg.Handler.BuildRequest(req, cmd)
		if err != nil {
			resp := &raftcmdpb.Response{}
			resp.Error.Message = err.Error()
			rs.OnResp(resp)
			pb.ReleaseRequest(req)
			continue
		}

		err = s.shardsProxy.Dispatch(req)
		if err != nil {
			resp := &raftcmdpb.Response{}
			resp.Error.Message = err.Error()
			rs.OnResp(resp)
		}
	}
}

func (s *Application) done(resp *raftcmdpb.Response) {
	if logger.DebugEnabled() {
		logger.Debugf("%s application received response",
			hex.EncodeToString(resp.ID))
	}

	// libary call
	if resp.SID == 0 {
		id := hack.SliceToString(resp.ID)
		if value, ok := s.libaryCB.Load(hack.SliceToString(resp.ID)); ok {
			s.libaryCB.Delete(id)
			value.(asyncCtx).resp(resp.Value, nil, resp.ContinueBroadcast)

			if resp.ContinueBroadcast {
				s.continueBroadcast(value)
				return
			}
		} else {
			if logger.DebugEnabled() {
				logger.Debugf("%s application received response, missing ctx",
					hex.EncodeToString(resp.ID))
			}
		}

		return
	}

	if value, ok := s.sessions.Load(resp.SID); ok {
		value.(*util.Session).OnResp(resp)
	} else {
		if logger.DebugEnabled() {
			logger.Debugf("%s application received response, missing session",
				hex.EncodeToString(resp.ID))
		}
	}
}

func (s *Application) doneError(resp *raftcmdpb.Request, err error) {
	// libary call
	if resp.SID == 0 {
		id := hack.SliceToString(resp.ID)
		if value, ok := s.libaryCB.Load(hack.SliceToString(resp.ID)); ok {
			s.libaryCB.Delete(id)
			value.(asyncCtx).resp(nil, err, false)
		}

		return
	}

	if value, ok := s.sessions.Load(resp.SID); ok {
		resp := &raftcmdpb.Response{}
		resp.Error.Message = err.Error()
		value.(*util.Session).OnResp(resp)
	}
}

func (s *Application) continueBroadcast(arg interface{}) {
	ctx := arg.(*broadcastCtx)
	max, shards, forwards, err := s.buildBroadcast(ctx.maxShard, ctx.group, ctx.mustLeader)
	if err != nil || len(shards) == 0 {
		util.DefaultTimeoutWheel().Schedule(time.Second, s.continueBroadcast, arg)
		return
	}

	ctx.Lock()
	ctx.total += len(shards)
	ctx.maxShard = max
	ctx.Unlock()

	s.doBroadcast(ctx, max, shards, forwards)
}

func (s *Application) buildBroadcast(after uint64, group uint64, mustLeader bool) (uint64, []uint64, []string, error) {
	var err error
	var shards []uint64
	var forwards []string
	max := after
	s.shardsProxy.Router().Every(group, mustLeader, func(id uint64, store string) {
		if store == "" {
			err = fmt.Errorf("missing forward store of shard %d", id)
			return
		}

		if id > max {
			max = id
		}

		if id > after {
			shards = append(shards, id)
			forwards = append(forwards, store)
		}
	})
	return max, shards, forwards, err
}

func (s *Application) doBroadcast(ctx *broadcastCtx, max uint64, shards []uint64, forwards []string) {
	var err error
	var requests []*raftcmdpb.Request
	for _, shard := range shards {
		req := pb.AcquireRequest()
		req.ID = uuid.NewV4().Bytes()
		req.Group = ctx.group
		req.StopAt = time.Now().Add(ctx.timeout).Unix()
		req.ToShard = shard
		req.AllowFollower = !ctx.mustLeader

		err = s.cfg.Handler.BuildRequest(req, ctx.cmd)
		if err != nil {
			break
		}

		requests = append(requests, req)
	}

	if err != nil {
		for _, req := range requests {
			pb.ReleaseRequest(req)
		}
		ctx.cb(ctx.arg, nil, err)
		return
	}

	for idx, req := range requests {
		if req.ToShard == max {
			req.LastBroadcast = true
		}

		s.libaryCB.Store(hack.SliceToString(req.ID), ctx)
		if ctx.timeout > 0 {
			util.DefaultTimeoutWheel().Schedule(ctx.timeout, s.execTimeout, req.ID)
		}

		err = s.shardsProxy.DispatchTo(req, req.ToShard, forwards[idx])
		if err != nil {
			// retry later
			util.DefaultTimeoutWheel().Schedule(time.Second, s.retryForward, req)
		}
	}
}

func (s *Application) retryForward(arg interface{}) {
	req := arg.(*raftcmdpb.Request)
	to := ""
	if req.AllowFollower {
		to = s.shardsProxy.Router().RandomPeerAddress(req.ToShard)
	} else {
		to = s.shardsProxy.Router().LeaderAddress(req.ToShard)
	}

	err := s.shardsProxy.DispatchTo(req, req.ToShard, to)
	if err != nil {
		// retry later
		util.DefaultTimeoutWheel().Schedule(time.Second, s.retryForward, req)
	}
}

type asyncCtx interface {
	resp([]byte, error, bool)
}

type ctx struct {
	arg interface{}
	cb  func(interface{}, []byte, error)
}

func (c ctx) resp(resp []byte, err error, appendOnly bool) {
	c.cb(c.arg, resp, err)
}

var (
	ctxPool sync.Pool
)

func acquireBroadcastCtx() *broadcastCtx {
	value := ctxPool.Get()
	if value == nil {
		return &broadcastCtx{}
	}
	return value.(*broadcastCtx)
}

func releaseBroadcastCtx(value *broadcastCtx) {
	value.reset()
	ctxPool.Put(value)
}

type broadcastCtx struct {
	sync.Mutex

	total      int
	completed  int
	mustLeader bool
	maxShard   uint64
	group      uint64
	err        error
	timeout    time.Duration
	cmd        interface{}
	values     [][]byte

	arg interface{}
	cb  func(interface{}, [][]byte, error)
}

func (c *broadcastCtx) resp(resp []byte, err error, appendOnly bool) {
	c.Lock()
	defer c.Unlock()

	c.completed++
	if err != nil {
		c.err = err
	}
	c.values = append(c.values, resp)

	if c.completed == c.total && !appendOnly {
		c.cb(c.arg, c.values, c.err)
		releaseBroadcastCtx(c)
	}
}

func (c *broadcastCtx) reset() {
	c.total = 0
	c.completed = 0
	c.err = nil
	c.values = nil
	c.cb = nil
	c.arg = nil
	c.cmd = nil
	c.timeout = 0
	c.group = 0
	c.mustLeader = false
	c.maxShard = 0
}
