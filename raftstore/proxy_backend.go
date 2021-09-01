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
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/task"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
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

func (bc *backend) addReq(req *raftcmdpb.Request) error {
	return bc.reqs.Put(req)
}

func (bc *backend) writeLoop() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				logger.Errorf("backend %s write loop failed with %+v, restart later",
					bc.addr,
					err)
				bc.writeLoop()
			}
		}()

		batch := int64(16)
		logger.Infof("backend %s write loop started",
			bc.addr)

		items := make([]interface{}, batch)
		for {
			n, err := bc.reqs.Get(batch, items)
			if err != nil {
				logger.Fatalf("BUG: read from queue failed with %+v", err)
				return
			}

			for i := int64(0); i < n; i++ {
				if items[i] == closeFlag {
					logger.Infof("backend %s write loop stopped",
						bc.addr)
					return
				}

				bc.conn.Write(items[i])
			}

			err = bc.conn.Flush()
			if err != nil {
				for i := int64(0); i < n; i++ {
					bc.p.errorDone(items[i].(*raftcmdpb.Request), err)
				}
			}

			for i := int64(0); i < n; i++ {
				pb.ReleaseRequest(items[i].(*raftcmdpb.Request))
			}
		}
	}()
}

func (bc *backend) readLoop() {
	go func() {
		logger.Infof("backend %s read loop started", bc.addr)

		for {
			data, err := bc.conn.Read()
			if err != nil {
				logger.Infof("backend %s read loop stopped", bc.addr)
				bc.conn.Close()
				return

			}

			if rsp, ok := data.(*raftcmdpb.Response); ok {
				if logger.DebugEnabled() {
					logger.Debugf("%s proxy received response from %s",
						hex.EncodeToString(rsp.ID),
						bc.addr)
				}
				bc.p.done(rsp)
			}
		}
	}()
}
