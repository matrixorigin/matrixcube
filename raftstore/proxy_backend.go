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

type backend struct {
	sync.Mutex

	addr string
	p    *shardsProxy
	conn goetty.IOSession
	reqs *task.Queue
}

func newBackend(p *shardsProxy, addr string, conn goetty.IOSession) *backend {
	bc := &backend{
		p:    p,
		addr: addr,
		conn: conn,
		reqs: task.New(32),
	}

	bc.writeLoop()
	return bc
}

func (bc *backend) addReq(req rpc.Request) error {
	return bc.reqs.Put(req)
}

func (bc *backend) writeLoop() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				bc.p.logger.Error("backend write loop failed, restart later",
					zap.String("backend", bc.addr),
					zap.Any("err", err))
				bc.writeLoop()
			}
		}()

		batch := int64(16)
		bc.p.logger.Info("backend write loop started",
			zap.String("backend", bc.addr))

		items := make([]interface{}, batch)
		for {
			n, err := bc.reqs.Get(batch, items)
			if err != nil {
				bc.p.logger.Fatal("BUG: fail to read from queue",
					zap.String("backend", bc.addr),
					zap.Error(err))
				return
			}

			for i := int64(0); i < n; i++ {
				if items[i] == closeFlag {
					bc.p.logger.Info("backend  write loop stopped",
						zap.String("backend", bc.addr))
					return
				}

				bc.conn.Write(items[i])
			}

			err = bc.conn.Flush()
			if err != nil {
				for i := int64(0); i < n; i++ {
					bc.p.errorDone(items[i].(*rpc.Request), err)
				}
			}
		}
	}()
}

func (bc *backend) readLoop() {
	go func() {
		bc.p.logger.Info("backend read loop started",
			zap.String("backend", bc.addr))

		for {
			data, err := bc.conn.Read()
			if err != nil {
				bc.p.logger.Info("backend read loop stopped",
					zap.String("backend", bc.addr))
				bc.conn.Close()
				return

			}

			if rsp, ok := data.(rpc.Response); ok {
				if ce := bc.p.logger.Check(zapcore.DebugLevel, "received response"); ce != nil {
					ce.Write(log.HexField("id", rsp.ID),
						zap.String("backend", bc.addr))
				}
				bc.p.done(rsp)
			}
		}
	}()
}
