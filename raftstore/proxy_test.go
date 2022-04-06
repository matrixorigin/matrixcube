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
	"sync"
	"testing"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/codec/length"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/matrixorigin/matrixcube/util/testutil"
)

// FIXME: add leaktest checks

type testBackendFactory struct {
	sync.RWMutex
	backends  map[string]backend
	successes map[string]SuccessCallback
	failures  map[string]FailureCallback
}

func newTestBackendFactory() *testBackendFactory {
	return &testBackendFactory{
		backends:  make(map[string]backend),
		successes: make(map[string]SuccessCallback),
		failures:  make(map[string]FailureCallback),
	}
}

func (f *testBackendFactory) create(addr string, success SuccessCallback, failure FailureCallback) (backend, error) {
	f.Lock()
	defer f.Unlock()

	bc, ok := f.backends[addr]
	if !ok {
		return nil, fmt.Errorf("missing backend %s", addr)
	}

	f.successes[addr] = success
	f.failures[addr] = failure
	return bc, nil
}

func TestLocalDispatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := make(chan rpcpb.Response, 1)
	fc := make(chan []byte, 1)
	success := func(r rpcpb.Response) { sc <- r }
	failure := func(id []byte, e error) {
		select {
		case fc <- id:
		default:
		}
	}
	factory := newTestBackendFactory()
	rr, err := newRouterBuilder().build(make(chan rpcpb.EventNotify))
	assert.NoError(t, err)
	sp, err := newShardsProxyBuilder().
		withRetryInterval(time.Millisecond*10).
		withBackendFactory(factory).
		withRequestCallback(success, failure).
		build(rr)
	assert.NoError(t, err)

	rc := newMockRetryController()
	sp.SetRetryController(rc)

	// no shard
	req := rpcpb.Request{}
	req.ID = []byte("k1")
	req.Key = []byte("k1")
	rc.setRequest(req, time.Millisecond*50)

	err = sp.Dispatch(req)
	assert.NoError(t, err)
	select {
	case <-sc:
		assert.Fail(t, "need failure callback")
	case v := <-fc:
		assert.Equal(t, req.ID, v)
	case <-time.After(time.Millisecond * 150):
		assert.Fail(t, "need failure callback")
	}

	// no backend
	assert.Error(t, sp.DispatchTo(req, Shard{}, "1"))

	// no resp
	factory.backends["b1"] = newLocalBackend(func(r rpcpb.Request) error { return nil })
	assert.NoError(t, sp.DispatchTo(req, Shard{}, "b1"))
	select {
	case <-sc:
		assert.Fail(t, "need timeout")
	case <-fc:
		assert.Fail(t, "need timeout")
	case <-time.After(time.Millisecond * 150):
	}

	// success
	factory.backends["b2"] = newLocalBackend(func(r rpcpb.Request) error {
		sp.OnResponse(rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: req.ID}}})
		return nil
	})
	assert.NoError(t, sp.DispatchTo(req, Shard{}, "b2"))
	select {
	case rsp := <-sc:
		assert.Equal(t, rpcpb.Response{ID: req.ID}, rsp)
	case <-fc:
		assert.Fail(t, "need succ")
	case <-time.After(time.Millisecond * 50):
		assert.Fail(t, "need succ")
	}
}

func TestRPCDispatch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rr, err := newRouterBuilder().build(make(chan rpcpb.EventNotify))
	assert.NoError(t, err)

	v := &rpcCodec{clientSide: true}
	encoder, decoder := length.NewWithSize(v, v, 0, 0, 0, 1024*1024)

	var sp1, sp2 ShardsProxy
	addr1 := fmt.Sprintf("127.0.0.1:%d", testutil.GenTestPorts(1)[0])
	rpc1 := newProxyRPC(log.GetDefaultZapLoggerWithLevel(zap.DebugLevel).With(zap.String("sp", "sp1")), addr1, 1024*1024, func(r rpcpb.Request) error {
		sp1.OnResponse(rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, PID: r.PID}}})
		return nil
	})
	factory1 := newTestBackendFactory()
	sc1 := make(chan rpcpb.Response, 1)
	fc1 := make(chan []byte, 1)
	success1 := func(r rpcpb.Response) { sc1 <- r }
	failure1 := func(id []byte, e error) {
		select {
		case fc1 <- id:
		default:
		}
	}
	sp1, err = newShardsProxyBuilder().
		withRetryInterval(time.Millisecond*10).
		withRPC(rpc1).
		withBackendFactory(factory1).
		withRequestCallback(success1, failure1).
		build(rr)
	assert.NoError(t, err)
	assert.NoError(t, sp1.Start())
	defer func() {
		assert.NoError(t, sp1.Stop())
	}()

	addr2 := fmt.Sprintf("127.0.0.1:%d", testutil.GenTestPorts(1)[0])
	rpc2 := newProxyRPC(log.GetDefaultZapLoggerWithLevel(zap.DebugLevel).With(zap.String("sp", "sp2")), addr2, 1024*1024, func(r rpcpb.Request) error {
		t.Logf("sp2 received")
		sp2.OnResponse(rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, PID: r.PID}}})
		return nil
	})
	factory2 := newTestBackendFactory()
	sc2 := make(chan rpcpb.Response, 1)
	fc2 := make(chan []byte, 1)
	success2 := func(r rpcpb.Response) { sc2 <- r }
	failure2 := func(id []byte, e error) {
		select {
		case fc2 <- id:
		default:
		}
	}
	sp2, err = newShardsProxyBuilder().
		withRetryInterval(time.Millisecond*10).
		withRPC(rpc2).
		withBackendFactory(factory2).
		withRequestCallback(success2, failure2).
		build(rr)
	assert.NoError(t, err)
	assert.NoError(t, sp2.Start())
	defer func() {
		assert.NoError(t, sp2.Stop())
	}()

	rc := newMockRetryController()
	sp2.SetRetryController(rc)

	factory1.backends[addr2] = newRemoteBackend(log.GetDefaultZapLoggerWithLevel(zap.DebugLevel).With(zap.String("sp", "sp1")),
		success1, failure1, addr2, goetty.NewIOSession(goetty.WithCodec(encoder, decoder)))

	req := rpcpb.Request{}
	req.ID = []byte("k1")
	req.Key = []byte("k1")
	rc.setRequest(req, time.Millisecond*100)
	assert.NoError(t, sp1.DispatchTo(req, Shard{}, addr2))

	select {
	case <-sc1:
	case <-fc1:
		assert.Fail(t, "need succ")
	case <-time.After(time.Millisecond * 100):
		assert.Fail(t, "need succ")
	}
}
