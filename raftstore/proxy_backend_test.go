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
	"fmt"
	"testing"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/codec/length"
	"github.com/matrixorigin/matrixcube/pb/errorpb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/util/testutil"
	"github.com/stretchr/testify/assert"
)

func TestLocalBackend(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := make(chan rpcpb.Request, 10)
	bc := newLocalBackend(func(r rpcpb.Request) error {
		c <- r
		return nil
	})

	req := newTestRPCRequests(1)[0]
	req.Cmd = []byte("c1")
	assert.NoError(t, bc.dispatch(req))
	assert.Equal(t, req, <-c)
}

func TestRemoteBackend(t *testing.T) {
	defer leaktest.AfterTest(t)()

	addr := fmt.Sprintf("127.0.0.1:%d", testutil.GenTestPorts(1)[0])

	c1 := make(chan rpcpb.Request, 1)
	p := newProxyRPC(nil, addr, 1024*1024, func(r rpcpb.Request) error {
		c1 <- r
		return nil
	})
	assert.NoError(t, p.start())
	defer p.stop()

	v := &rpcCodec{clientSide: true}
	encoder, decoder := length.NewWithSize(v, v, 0, 0, 0, 1024*1024)
	conn := goetty.NewIOSession(goetty.WithCodec(encoder, decoder), goetty.WithTimeout(time.Second, time.Second))
	defer conn.Close()

	c2 := make(chan rpcpb.Response, 1)
	ec2 := make(chan error, 10)
	bc := newRemoteBackend(nil, func(r rpcpb.Response) { c2 <- r }, func(id []byte, e error) { ec2 <- e }, addr, conn)
	defer bc.close()

	req := newTestRPCRequests(1)[0]
	req.Cmd = []byte("c1")
	assert.NoError(t, bc.dispatch(req))

	r := <-c1
	assert.True(t, r.PID > 0)

	r1 := rpcpb.Response{PID: r.PID, Value: []byte("v1")}
	p.onResponse(rpcpb.ResponseBatchHeader{}, r1)
	assert.Equal(t, r1, <-c2)

	p.onResponse(rpcpb.ResponseBatchHeader{Error: errorpb.Error{Message: "error"}}, r1)
	rsp := <-c2
	assert.NotEmpty(t, rsp.Error)
}
