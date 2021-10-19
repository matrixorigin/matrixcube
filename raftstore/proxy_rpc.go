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
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/codec/length"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type proxyRPC interface {
	start() error
	stop()
	onResponse(header rpc.ResponseBatchHeader, rsp rpc.Response)
}

type defaultRPC struct {
	logger  *zap.Logger
	app     goetty.NetApplication
	handler func(rpc.Request) error
}

func newProxyRPC(logger *zap.Logger, addr string, maxBodySize int, handler func(rpc.Request) error) proxyRPC {
	rpc := &defaultRPC{
		logger:  log.Adjust(logger),
		handler: handler,
	}

	encoder, decoder := length.NewWithSize(rc, rc, 0, 0, 0, maxBodySize)
	app, err := goetty.NewTCPApplication(addr, rpc.onMessage,
		goetty.WithAppSessionOptions(goetty.WithCodec(encoder, decoder),
			goetty.WithEnableAsyncWrite(16),
			goetty.WithLogger(logger)))

	if err != nil {
		rpc.logger.Fatal("fail to create rpc",
			zap.Error(err))
	}

	rpc.app = app
	return rpc
}

func (r *defaultRPC) start() error {
	return r.app.Start()
}

func (r *defaultRPC) stop() {
	r.app.Stop()
}

func (r *defaultRPC) onMessage(rs goetty.IOSession, value interface{}, seq uint64) error {
	req := value.(rpc.Request)
	req.PID = int64(rs.ID())
	err := r.handler(req)
	if err != nil {
		rsp := rpc.Response{}
		rsp.ID = req.ID
		rsp.Error.Message = err.Error()
		rs.WriteAndFlush(rsp)
	}
	return nil
}

func (r *defaultRPC) onResponse(header rpc.ResponseBatchHeader, rsp rpc.Response) {
	if rs, _ := r.app.GetSession(uint64(rsp.PID)); rs != nil {
		if !header.IsEmpty() {
			if header.Error.RaftEntryTooLarge == nil {
				rsp.Type = rpc.CmdType_RaftError
			} else {
				rsp.Type = rpc.CmdType_Invalid
			}

			rsp.Error = header.Error
		}

		if ce := r.logger.Check(zapcore.DebugLevel, "receive response"); ce != nil {
			ce.Write(log.HexField("id", rsp.ID))
		}
		rs.WriteAndFlush(rsp)
	} else {
		if ce := r.logger.Check(zapcore.DebugLevel, "skip receive response"); ce != nil {
			ce.Write(log.HexField("id", rsp.ID), log.ReasonField("missing session"))
		}
	}
}
