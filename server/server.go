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

	"github.com/fagongzi/util/hack"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/util"
	"github.com/matrixorigin/matrixcube/util/uuid"
	"go.uber.org/zap"
)

// CustomRequest customized request with request type and request content
type CustomRequest struct {
	// Group used to indicate which group of Shards to send
	Group uint64
	// Key the key used to indicate which shard to send
	Key []byte
	// CustomType type of custom request
	CustomType uint64
	// Cmd serialized custom request content
	Cmd []byte
	// Read read request
	Read bool
	// Write write request
	Write bool
	// Args custom args for async execute callback
	Args interface{}
}

// Application a tcp application server
type Application struct {
	cfg             Cfg
	shardsProxy     raftstore.ShardsProxy
	callbackContext sync.Map // id -> application cb
	dispatcher      func(req rpc.Request, cmd CustomRequest, proxy raftstore.ShardsProxy) error
	logger          *zap.Logger
}

// NewApplication returns a tcp application server
func NewApplication(cfg Cfg) *Application {
	return NewApplicationWithDispatcher(cfg, nil)
}

// NewApplication returns a tcp application server
func NewApplicationWithDispatcher(cfg Cfg, dispatcher func(req rpc.Request, cmd CustomRequest, proxy raftstore.ShardsProxy) error) *Application {
	return &Application{
		cfg:        cfg,
		dispatcher: dispatcher,
	}
}

// Start start the application server
func (s *Application) Start() error {
	s.logger = s.cfg.Store.GetConfig().Logger

	s.logger.Info("begin to start application")
	if !s.cfg.storeStarted {
		s.cfg.Store.Start()
	}
	s.shardsProxy = s.cfg.Store.GetShardsProxy()
	s.shardsProxy.SetCallback(s.done, s.doneError)
	s.shardsProxy.SetRetryController(s)
	s.logger.Info("application started")
	return nil
}

// Stop stop redis server
func (s *Application) Stop() {
	if !s.cfg.storeStarted {
		s.cfg.Store.Stop()
	}
	s.logger.Info("application stopped")
}

// ShardProxy returns the shard proxy
func (s *Application) ShardProxy() raftstore.ShardsProxy {
	return s.shardsProxy
}

// ExecWithGroup exec the request command
func (s *Application) Exec(cmd CustomRequest, timeout time.Duration) ([]byte, error) {
	completeC := make(chan interface{}, 1)
	closed := uint32(0)
	cb := func(_ CustomRequest, resp []byte, err error) {
		if atomic.CompareAndSwapUint32(&closed, 0, 1) {
			if err != nil {
				completeC <- err
			} else {
				completeC <- resp
			}
			close(completeC)
		}
	}

	s.AsyncExec(cmd, cb, timeout)
	value := <-completeC
	switch v := value.(type) {
	case error:
		return nil, v
	default:
		return value.([]byte), nil
	}
}

// AsyncExec async exec the request, if the err is ErrTimeout means the request is timeout
func (s *Application) AsyncExec(cmd CustomRequest, cb func(CustomRequest, []byte, error), timeout time.Duration) {
	req := rpc.Request{}
	req.ID = uuid.NewV4().Bytes()
	req.CustomType = cmd.CustomType
	req.Group = cmd.Group
	req.Key = cmd.Key
	req.Cmd = cmd.Cmd
	req.StopAt = time.Now().Add(timeout).Unix()
	if cmd.Read {
		req.Type = rpc.CmdType_Read
	}
	if cmd.Write {
		req.Type = rpc.CmdType_Write
	}

	if ce := s.logger.Check(zap.DebugLevel, "begin to send request"); ce != nil {
		ce.Write(log.RequestIDField(req.ID))
	}

	s.callbackContext.Store(hack.SliceToString(req.ID), ctx{
		cmd: cmd,
		req: req,
		cb:  cb,
	})
	if timeout > 0 {
		util.DefaultTimeoutWheel().Schedule(timeout, s.execTimeout, req.ID)
	}

	var err error
	if s.dispatcher != nil {
		err = s.dispatcher(req, cmd, s.shardsProxy)
	} else {
		err = s.shardsProxy.Dispatch(req)
	}
	if err != nil {
		s.callbackContext.Delete(hack.SliceToString(req.ID))
		cb(cmd, nil, err)
	}
}

func (s *Application) execTimeout(arg interface{}) {
	id := hack.SliceToString(arg.([]byte))
	if value, ok := s.callbackContext.Load(id); ok {
		s.callbackContext.Delete(id)
		value.(ctx).resp(nil,
			fmt.Errorf("exec timeout for request %s", hex.EncodeToString(arg.([]byte))))
	}
}

func (s *Application) Retry(requestID []byte) (rpc.Request, bool) {
	id := hack.SliceToString(requestID)
	if value, ok := s.callbackContext.Load(id); ok {
		return value.(ctx).req, true
	}

	return rpc.Request{}, false
}

func (s *Application) done(resp rpc.Response) {
	if ce := s.logger.Check(zap.DebugLevel, "response received"); ce != nil {
		ce.Write(log.RequestIDField(resp.ID))
	}

	id := hack.SliceToString(resp.ID)
	if value, ok := s.callbackContext.Load(hack.SliceToString(resp.ID)); ok {
		s.callbackContext.Delete(id)
		value.(ctx).resp(resp.Value, nil)
	} else {
		if ce := s.logger.Check(zap.DebugLevel, "response skipped"); ce != nil {
			ce.Write(log.RequestIDField(resp.ID), log.ReasonField("missing ctx"))
		}
	}
}

func (s *Application) doneError(requestID []byte, err error) {
	if ce := s.logger.Check(zap.DebugLevel, "error response received"); ce != nil {
		ce.Write(log.RequestIDField(requestID), zap.Error(err))
	}

	id := hack.SliceToString(requestID)
	if value, ok := s.callbackContext.Load(id); ok {
		s.callbackContext.Delete(id)
		value.(ctx).resp(nil, err)
	}
}

type ctx struct {
	req rpc.Request
	cmd CustomRequest
	cb  func(CustomRequest, []byte, error)
}

func (c ctx) resp(resp []byte, err error) {
	if c.cb != nil {
		c.cb(c.cmd, resp, err)
	}
}
