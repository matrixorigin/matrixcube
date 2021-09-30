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

package raftstore

import (
	"errors"
	"sync"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/codec"
	"github.com/fagongzi/goetty/codec/length"
	"github.com/fagongzi/util/task"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	closeFlag = &struct{}{}

	errConnect            = errors.New("not connected")
	defaultConnectTimeout = time.Second * 10
)

type defaultBackendFactory struct {
	logger  *zap.Logger
	s       *store
	local   backend
	encoder codec.Encoder
	decoder codec.Decoder
}

func newBackendFactory(logger *zap.Logger, s *store) backendFactory {
	v := &rpcCodec{clientSide: true}
	encoder, decoder := length.NewWithSize(v, v, 0, 0, 0, int(s.cfg.Raft.MaxEntryBytes)*2)
	return &defaultBackendFactory{
		logger:  logger,
		s:       s,
		encoder: encoder,
		decoder: decoder,
		local:   newLocalBackend(s.OnRequest),
	}
}

func (f *defaultBackendFactory) create(addr string, success SuccessCallback, failure FailureCallback) (backend, error) {
	if addr == f.s.Meta().ClientAddr {
		return f.local, nil
	}

	return newRPCBackend(f.logger, success, failure, addr, goetty.NewIOSession(goetty.WithCodec(f.encoder, f.decoder))),
		nil
}

type localBackend struct {
	handler func(rpc.Request) error
}

func newLocalBackend(handler func(rpc.Request) error) backend {
	return &localBackend{handler: handler}
}

func (lb *localBackend) dispatch(req rpc.Request) error {
	req.PID = 0
	return lb.handler(req)
}

type rpcBackend struct {
	sync.Mutex

	addr            string
	logger          *zap.Logger
	successCallback SuccessCallback
	failureCallback FailureCallback
	conn            goetty.IOSession
	reqs            *task.Queue
}

func newRPCBackend(logger *zap.Logger,
	successCallback SuccessCallback,
	failureCallback FailureCallback,
	addr string,
	conn goetty.IOSession) *rpcBackend {
	bc := &rpcBackend{
		logger:          logger,
		successCallback: successCallback,
		failureCallback: failureCallback,
		addr:            addr,
		conn:            conn,
		reqs:            task.New(32),
	}

	bc.writeLoop()
	return bc
}

func (bc *rpcBackend) dispatch(req rpc.Request) error {
	if !bc.checkConnect() {
		return errConnect
	}

	return bc.reqs.Put(req)
}

func (bc *rpcBackend) checkConnect() bool {
	if nil == bc {
		return false
	}

	if bc.conn.Connected() {
		return true
	}

	bc.Lock()
	defer bc.Unlock()

	if bc.conn.Connected() {
		return true
	}

	ok, err := bc.conn.Connect(bc.addr, defaultConnectTimeout)
	if err != nil {
		bc.logger.Error("fail to connect to backend",
			zap.String("backend", bc.addr),
			zap.Error(err))
		return false
	}

	bc.readLoop()
	return ok
}

func (bc *rpcBackend) writeLoop() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				bc.logger.Error("backend write loop failed, restart later",
					zap.String("backend", bc.addr),
					zap.Any("err", err))
				bc.writeLoop()
			}
		}()

		batch := int64(16)
		bc.logger.Info("backend write loop started",
			zap.String("backend", bc.addr))

		items := make([]interface{}, batch)
		for {
			n, err := bc.reqs.Get(batch, items)
			if err != nil {
				bc.logger.Fatal("BUG: fail to read from queue",
					zap.String("backend", bc.addr),
					zap.Error(err))
				return
			}

			for i := int64(0); i < n; i++ {
				if items[i] == closeFlag {
					bc.logger.Info("backend  write loop stopped",
						zap.String("backend", bc.addr))
					return
				}

				bc.conn.Write(items[i])
			}

			err = bc.conn.Flush()
			if err != nil {
				for i := int64(0); i < n; i++ {
					req := items[i].(rpc.Request)
					bc.failureCallback(&req, err)
				}
			}
		}
	}()
}

func (bc *rpcBackend) readLoop() {
	go func() {
		bc.logger.Info("backend read loop started",
			zap.String("backend", bc.addr))

		for {
			data, err := bc.conn.Read()
			if err != nil {
				bc.logger.Info("backend read loop stopped",
					zap.String("backend", bc.addr))
				bc.conn.Close()
				return

			}

			if rsp, ok := data.(rpc.Response); ok {
				if ce := bc.logger.Check(zapcore.DebugLevel, "received response"); ce != nil {
					ce.Write(log.HexField("id", rsp.ID),
						zap.String("backend", bc.addr))
				}
				bc.successCallback(rsp)
			}
		}
	}()
}
