package server

import (
	"encoding/hex"
	"errors"
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

func (s *Application) execTimeout(arg interface{}) {
	id := hack.SliceToString(arg.([]byte))
	if value, ok := s.libaryCB.Load(id); ok {
		s.libaryCB.Delete(id)
		value.(ctx).resp(nil, ErrTimeout)
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
			value.(ctx).resp(resp.Value, nil)
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
			value.(ctx).resp(nil, err)
		}

		return
	}

	if value, ok := s.sessions.Load(resp.SID); ok {
		resp := &raftcmdpb.Response{}
		resp.Error.Message = err.Error()
		value.(*util.Session).OnResp(resp)
	}
}

type ctx struct {
	arg interface{}
	cb  func(interface{}, []byte, error)
}

func (c ctx) resp(resp []byte, err error) {
	c.cb(c.arg, resp, err)
}
