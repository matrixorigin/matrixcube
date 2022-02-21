// Copyright 2021 MatrixOrigin.
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
	"fmt"
	"testing"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/codec/length"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/util/testutil"
	"github.com/stretchr/testify/assert"
)

func TestRPCProxy(t *testing.T) {
	defer leaktest.AfterTest(t)()

	addr := fmt.Sprintf("127.0.0.1:%d", testutil.GenTestPorts(1)[0])
	c := make(chan rpcpb.Request, 10)
	ec := make(chan error, 10)
	p := newProxyRPC(nil, addr, 1024*1024, func(r rpcpb.Request) error {
		c <- r
		return <-ec
	})
	assert.NoError(t, p.start())
	defer p.stop()

	pid := int64(0)
	v := &rpcCodec{clientSide: true}
	encoder, decoder := length.NewWithSize(v, v, 0, 0, 0, 1024*1024)
	conn := goetty.NewIOSession(goetty.WithCodec(encoder, decoder), goetty.WithTimeout(time.Second, time.Second))
	ok, err := conn.Connect(addr, time.Second)
	assert.NoError(t, err)
	assert.True(t, ok)
	defer conn.Close()

	req := newTestRPCRequests(1)[0]
	req.Cmd = []byte("c1")
	assert.NoError(t, conn.WriteAndFlush(req))

	select {
	case r := <-c:
		assert.True(t, r.PID > 0)
		pid = r.PID
		r.PID = 0
		assert.Equal(t, req, r)
		ec <- nil
	case <-time.After(time.Second):
		assert.FailNow(t, "timeout")
	}

	req = newTestRPCRequests(1)[0]
	req.Cmd = []byte("error")
	assert.NoError(t, conn.WriteAndFlush(req))
	select {
	case r := <-c:
		assert.True(t, r.PID > 0)
		r.PID = 0
		assert.Equal(t, req, r)
		ec <- errors.New(string(req.Cmd))
	case <-time.After(time.Second):
		assert.FailNow(t, "timeout")
	}
	data, err := conn.Read()
	assert.NoError(t, err)
	assert.Equal(t, string(req.Cmd), data.(rpcpb.Response).Error.Message)

	rsp := rpcpb.Response{PID: pid, Value: []byte("v1")}
	p.onResponse(rpcpb.ResponseBatchHeader{}, rsp)
	data, err = conn.Read()
	assert.NoError(t, err)
	assert.Equal(t, data, rsp)
}
