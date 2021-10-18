// Copyright 2020 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/hack"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/util/uuid"
	"go.uber.org/zap"
)

// Application a tcp application server
type Application struct {
	cfg         Cfg
	server      goetty.NetApplication
	shardsProxy raftstore.ShardsProxy
	libaryCB    sync.Map // id -> application cb
	dispatcher  func(req rpc.Request, cmd interface{}, proxy raftstore.ShardsProxy) error

	logger *zap.Logger
}

// NewApplication returns a tcp application server
func NewApplication(cfg Cfg) *Application {
	return NewApplicationWithDispatcher(cfg, nil)
}

// NewApplication returns a tcp application server
func NewApplicationWithDispatcher(cfg Cfg, dispatcher func(req rpc.Request, cmd interface{}, proxy raftstore.ShardsProxy) error) *Application {
	s := &Application{
		cfg:        cfg,
		dispatcher: dispatcher,
		logger:     cfg.Store.GetConfig().Logger.With(log.ListenAddressField(cfg.Addr)),
	}

	if !cfg.ExternalServer {
		encoder, decoder := cfg.Handler.Codec()
		app, err := goetty.NewTCPApplication(cfg.Addr, s.onMessage,
			goetty.WithAppSessionOptions(goetty.WithCodec(encoder, decoder),
				goetty.WithEnableAsyncWrite(16),
				goetty.WithLogger(s.logger)))
		if err != nil {
			s.logger.Panic("fail to create internal server",
				zap.Error(err))
		}
		s.server = app
	}
	return s
}

// Start start the application server
func (s *Application) Start() error {
	s.logger.Info("begin to start server")

	s.cfg.Store.Start()
	s.shardsProxy = s.cfg.Store.GetShardsProxy()
	s.shardsProxy.SetCallback(s.done, s.doneError)
	if s.cfg.ExternalServer {
		s.logger.Info("using external server")
		return nil
	}

	s.logger.Info("begin to start internal server")
	return s.server.Start()
}

// Stop stop redis server
func (s *Application) Stop() {
	if s.cfg.ExternalServer {
		return
	}
	s.server.Stop()
}

// ShardProxy returns the shard proxy
func (s *Application) ShardProxy() raftstore.ShardsProxy {
	return s.shardsProxy
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
	switch v := value.(type) {
	case error:
		return nil, v
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
	req := rpc.Request{}
	req.ID = uuid.NewV4().Bytes()
	req.Group = group
	req.StopAt = time.Now().Add(timeout).Unix()

	err := s.cfg.Handler.BuildRequest(&req, cmd)
	if err != nil {
		if ce := s.logger.Check(zap.DebugLevel, "fail to build request"); ce != nil {
			ce.Write(log.RequestIDField(req.ID), zap.Error(err))
		}
		cb(arg, nil, err)
		return
	}

	if ce := s.logger.Check(zap.DebugLevel, "begin to send request"); ce != nil {
		ce.Write(log.RequestIDField(req.ID))
	}

	s.libaryCB.Store(hack.SliceToString(req.ID), ctx{
		arg: arg,
		cb:  cb,
	})
	if timeout > 0 {
		util.DefaultTimeoutWheel().Schedule(timeout, s.execTimeout, req.ID)
	}

	if s.dispatcher != nil {
		err = s.dispatcher(req, cmd, s.shardsProxy)
	} else {
		err = s.shardsProxy.Dispatch(req)
	}
	if err != nil {
		s.libaryCB.Delete(hack.SliceToString(req.ID))
		cb(arg, nil, err)
	}
}

func (s *Application) execTimeout(arg interface{}) {
	id := hack.SliceToString(arg.([]byte))
	if value, ok := s.libaryCB.Load(id); ok {
		s.libaryCB.Delete(id)
		value.(asyncCtx).resp(nil,
			fmt.Errorf("exec timeout for request %s", hex.EncodeToString(arg.([]byte))))
	}
}

func (s *Application) onMessage(conn goetty.IOSession, cmd interface{}, seq uint64) error {
	req := rpc.Request{}
	req.ID = uuid.NewV4().Bytes()
	req.SID = int64(conn.ID())

	err := s.cfg.Handler.BuildRequest(&req, cmd)
	if err != nil {
		resp := &rpc.Response{}
		resp.Error.Message = err.Error()
		conn.WriteAndFlush(resp)
		return nil
	}

	if s.dispatcher != nil {
		err = s.dispatcher(req, cmd, s.shardsProxy)
	} else {
		err = s.shardsProxy.Dispatch(req)
	}

	if err != nil {
		resp := &rpc.Response{}
		resp.Error.Message = err.Error()
		conn.WriteAndFlush(resp)
	}
	return nil
}

func (s *Application) done(resp rpc.Response) {
	if ce := s.logger.Check(zap.DebugLevel, "response received"); ce != nil {
		ce.Write(log.RequestIDField(resp.ID))
	}

	// libary call
	if resp.SID == 0 {
		id := hack.SliceToString(resp.ID)
		if value, ok := s.libaryCB.Load(hack.SliceToString(resp.ID)); ok {
			s.libaryCB.Delete(id)
			value.(asyncCtx).resp(resp.Value, nil)
		} else {
			if ce := s.logger.Check(zap.DebugLevel, "response skipped"); ce != nil {
				ce.Write(log.RequestIDField(resp.ID), log.ReasonField("missing ctx"))
			}
		}

		return
	}

	if conn, _ := s.server.GetSession(uint64(resp.SID)); conn != nil {
		conn.WriteAndFlush(resp)
	} else {
		if ce := s.logger.Check(zap.DebugLevel, "response skipped"); ce != nil {
			ce.Write(log.RequestIDField(resp.ID), log.ReasonField("missing session"))
		}
	}
}

func (s *Application) doneError(resp *rpc.Request, err error) {
	if resp == nil && nil != err {
		s.logger.Error("fail to response", zap.Error(err))
		return
	}

	if ce := s.logger.Check(zap.DebugLevel, "error response received"); ce != nil {
		ce.Write(log.RequestIDField(resp.ID), zap.Error(err))
	}

	// libary call
	if resp.SID == 0 {
		id := hack.SliceToString(resp.ID)
		if value, ok := s.libaryCB.Load(hack.SliceToString(resp.ID)); ok {
			s.libaryCB.Delete(id)
			value.(asyncCtx).resp(nil, err)
		}

		return
	}

	if conn, _ := s.server.GetSession(uint64(resp.SID)); conn != nil {
		resp := &rpc.Response{}
		resp.Error.Message = err.Error()
		conn.WriteAndFlush(resp)
	}
}

func (s *Application) buildBroadcast(after uint64, group uint64, mustLeader bool) (uint64, []uint64, []string, error) {
	var err error
	var shards []uint64
	var forwards []string
	max := after
	s.shardsProxy.Router().Every(group, mustLeader, func(shard meta.Shard, store meta.Store) bool {
		id := shard.ID
		if store.ClientAddr == "" {
			err = fmt.Errorf("missing forward store of shard %d", id)
			return false
		}

		if id > max {
			max = id
		}

		if id > after {
			shards = append(shards, id)
			forwards = append(forwards, store.ClientAddr)
		}
		return true
	})
	return max, shards, forwards, err
}

func (s *Application) retryForward(arg interface{}) {
	req := arg.(rpc.Request)
	to := s.shardsProxy.Router().LeaderReplicaStore(req.ToShard).ClientAddr
	err := s.shardsProxy.DispatchTo(req, s.shardsProxy.Router().GetShard(req.ToShard), to)
	if err != nil {
		// retry later
		util.DefaultTimeoutWheel().Schedule(time.Second, s.retryForward, req)
	}
}

type asyncCtx interface {
	resp([]byte, error)
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
