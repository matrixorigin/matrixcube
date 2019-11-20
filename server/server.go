package server

import (
	"errors"
	"io"
	"sync"
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
	logger = log.NewLoggerWithPrefix("[beehive-redis-server]")

	// ErrTimeout timeout error
	ErrTimeout = errors.New("Exec timeout")
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

	decoder, encoder := cfg.Handler.Codec()
	s.server = goetty.NewServer(cfg.Addr,
		goetty.WithServerDecoder(decoder),
		goetty.WithServerEncoder(encoder))
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
	s.server.Stop()
}

// Exec exec the request command
func (s *Application) Exec(cmd interface{}, timeout time.Duration) ([]byte, error) {
	completeC := make(chan interface{}, 1)

	cb := func(resp []byte, err error) {
		if err != nil {
			completeC <- err
		} else {
			completeC <- resp
		}
	}

	s.AsyncExecWithTimeout(cmd, cb, timeout)
	value := <-completeC
	switch value.(type) {
	case error:
		return nil, value.(error)
	default:
		return value.([]byte), nil
	}
}

// AsyncExec async exec the request command
func (s *Application) AsyncExec(cmd interface{}, cb func([]byte, error)) {
	s.AsyncExecWithTimeout(cmd, cb, 0)
}

// AsyncExecWithTimeout async exec the request, if the err is ErrTimeout means the request is timeout
func (s *Application) AsyncExecWithTimeout(cmd interface{}, cb func([]byte, error), timeout time.Duration) {
	req := pb.AcquireRequest()
	err := s.cfg.Handler.BuildRequest(req, cmd)
	if err != nil {
		cb(nil, err)
		pb.ReleaseRequest(req)
		return
	}

	req.ID = uuid.NewV4().Bytes()
	s.libaryCB.Store(hack.SliceToString(req.ID), cb)
	if timeout > 0 {
		util.DefaultTimeoutWheel().Schedule(timeout, s.execTimeout, req.ID)
	}

	err = s.shardsProxy.Dispatch(req)
	if err != nil {
		cb(nil, err)
		pb.ReleaseRequest(req)
		s.libaryCB.Delete(hack.SliceToString(req.ID))
	}
}

func (s *Application) execTimeout(arg interface{}) {
	id := hack.SliceToString(arg.([]byte))
	if cb, ok := s.libaryCB.Load(id); ok {
		s.libaryCB.Delete(id)
		cb.(func([]byte, error))(nil, ErrTimeout)
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
		err = s.cfg.Handler.BuildRequest(req, cmd)
		if err != nil {
			resp := &raftcmdpb.Response{}
			resp.Error.Message = err.Error()
			rs.OnResp(resp)
			pb.ReleaseRequest(req)
			continue
		}

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

func (s *Application) done(resp *raftcmdpb.Response) {
	// libary call
	if resp.SID == 0 {
		if cb, ok := s.libaryCB.Load(hack.SliceToString(resp.ID)); ok {
			cb.(func([]byte, error))(resp.Value, nil)
		}

		return
	}

	if value, ok := s.sessions.Load(resp.SID); ok {
		value.(*util.Session).OnResp(resp)
	}
}

func (s *Application) doneError(resp *raftcmdpb.Request, err error) {
	// libary call
	if resp.SID == 0 {
		if cb, ok := s.libaryCB.Load(hack.SliceToString(resp.ID)); ok {
			cb.(func([]byte, error))(nil, err)
		}

		return
	}

	if value, ok := s.sessions.Load(resp.SID); ok {
		resp := &raftcmdpb.Response{}
		resp.Error.Message = err.Error()
		value.(*util.Session).OnResp(resp)
	}
}
