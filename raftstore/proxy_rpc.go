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
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"go.uber.org/zap"
)

type proxyRPC interface {
	start() error
	stop()
	onResponse(header rpcpb.ResponseBatchHeader, rsp rpcpb.Response)
}

type defaultRPC struct {
	logger  *zap.Logger
	app     goetty.NetApplication
	handler func(rpcpb.Request) error
}

func newProxyRPC(logger *zap.Logger, addr string, maxBodySize int, handler func(rpcpb.Request) error) proxyRPC {
	rpc := &defaultRPC{
		logger:  log.Adjust(logger),
		handler: handler,
	}

	encoder, decoder := length.NewWithSize(rc, rc, 0, 0, 0, maxBodySize)
	app, err := goetty.NewTCPApplication(
		addr,
		rpc.onMessage,
		goetty.WithAppLogger(logger),
		goetty.WithAppSessionOptions(
			goetty.WithCodec(encoder, decoder),
			goetty.WithEnableAsyncWrite(16),
			goetty.WithLogger(logger),
		),
	)

	if err != nil {
		rpc.logger.Fatal("fail to create rpcpb",
			zap.Error(err))
	}

	rpc.app = app
	return rpc
}

func (r *defaultRPC) start() error {
	return r.app.Start()
}

func (r *defaultRPC) stop() {
	if err := r.app.Stop(); err != nil {
		r.logger.Fatal("stop rpc failed",
			zap.Error(err))
	}
}

func (r *defaultRPC) onMessage(rs goetty.IOSession, value interface{}, seq uint64) error {
	req := value.(rpcpb.Request)
	req.PID = int64(rs.ID())
	err := r.handler(req)
	if err != nil {
		rsp := rpcpb.Response{}
		rsp.ID = req.ID
		rsp.Error.Message = err.Error()
		rs.WriteAndFlush(rsp)
	}
	return nil
}

func (r *defaultRPC) onResponse(header rpcpb.ResponseBatchHeader, rsp rpcpb.Response) {
	if rs, _ := r.app.GetSession(uint64(rsp.PID)); rs != nil {
		rsp.Error = header.Error
		if ce := r.logger.Check(zap.DebugLevel, "rpcpb received response"); ce != nil {
			ce.Write(log.HexField("id", rsp.ID),
				log.RaftResponseField("response", &rsp))
		}
		rs.WriteAndFlush(rsp)
	} else {
		if ce := r.logger.Check(zap.DebugLevel, "rpcpb received response skipped"); ce != nil {
			ce.Write(log.HexField("id", rsp.ID),
				log.RaftResponseField("response", &rsp),
				log.ReasonField("missing session"))
		}
	}
}
