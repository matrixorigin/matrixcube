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
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/util"
)

var (
	// ErrTimeout timeout error
	ErrTimeout = errors.New("exec timeout")
)

var (
	// RetryInterval retry interval
	RetryInterval = time.Second
)

type doneFunc func(*raftcmdpb.Response)
type errorDoneFunc func(*raftcmdpb.Request, error)

// ShardsProxy Shards proxy, distribute the appropriate request to the corresponding backend,
// retry the request for the error
type ShardsProxy interface {
	Dispatch(req *raftcmdpb.Request) error
	DispatchTo(req *raftcmdpb.Request, shard uint64, store string) error
	Router() Router
}

// NewShardsProxy returns a shard proxy
func NewShardsProxy(router Router,
	doneCB doneFunc,
	errorDoneCB errorDoneFunc) ShardsProxy {
	return &shardsProxy{
		router:      router,
		doneCB:      doneCB,
		errorDoneCB: errorDoneCB,
	}
}

// NewShardsProxyWithStore returns a shard proxy with a raftstore
func NewShardsProxyWithStore(store Store,
	doneCB doneFunc,
	errorDoneCB errorDoneFunc,
) (ShardsProxy, error) {
	sp := &shardsProxy{
		store:       store,
		local:       store.Meta(),
		router:      store.GetRouter(),
		doneCB:      doneCB,
		errorDoneCB: errorDoneCB,
	}

	sp.store.RegisterLocalRequestCB(sp.onLocalResp)
	return sp, nil
}

type shardsProxy struct {
	local       bhmetapb.Store
	store       Store
	router      Router
	doneCB      doneFunc
	errorDoneCB errorDoneFunc
	backends    sync.Map // store addr -> *backend
}

func (p *shardsProxy) Dispatch(req *raftcmdpb.Request) error {
	shard, to := p.router.SelectShard(req.Group, req.Key)
	return p.DispatchTo(req, shard, to)
}

func (p *shardsProxy) DispatchTo(req *raftcmdpb.Request, shard uint64, to string) error {
	// No leader, retry after a leader tick
	if to == "" {
		if logger.ErrorEnabled() {
			logger.Errorf("%s retry with no leader, shard %d, group %d",
				hex.EncodeToString(req.ID),
				shard,
				req.Group)
		}

		p.retryWithRaftError(req, "dispath to nil store", RetryInterval)
		return nil
	}

	return p.forwardToBackend(req, to)
}

func (p *shardsProxy) Router() Router {
	return p.router
}

func (p *shardsProxy) forwardToBackend(req *raftcmdpb.Request, leader string) error {
	if p.store != nil && p.local.ClientAddr == leader {
		req.PID = 0
		return p.store.OnRequest(req)
	}

	bc, err := p.getConn(leader)
	if err != nil {
		return err
	}

	return bc.addReq(req)
}

func (p *shardsProxy) onLocalResp(header *raftcmdpb.RaftResponseHeader, rsp *raftcmdpb.Response) {
	if header != nil {
		if header.Error.RaftEntryTooLarge == nil {
			rsp.Type = raftcmdpb.CMDType_RaftError
		} else {
			rsp.Type = raftcmdpb.CMDType_Invalid
		}

		rsp.Error = header.Error
	}

	p.done(rsp)
	pb.ReleaseResponse(rsp)
}

func (p *shardsProxy) done(rsp *raftcmdpb.Response) {
	if rsp.Type == raftcmdpb.CMDType_Invalid && rsp.Error.Message != "" {
		p.errorDoneCB(rsp.OriginRequest, errors.New(rsp.Error.String()))
		return
	}

	if rsp.Type != raftcmdpb.CMDType_RaftError && !rsp.Stale {
		p.doneCB(rsp)
		return
	}

	p.retryWithRaftError(rsp.OriginRequest, rsp.Error.String(), RetryInterval)
	pb.ReleaseResponse(rsp)
}

func (p *shardsProxy) errorDone(req *raftcmdpb.Request, err error) {
	p.errorDoneCB(req, err)
}

func (p *shardsProxy) retryWithRaftError(req *raftcmdpb.Request, err string, later time.Duration) {
	if req != nil {
		if time.Now().Unix() >= req.StopAt {
			p.errorDoneCB(req, errors.New(err))
			return
		}

		util.DefaultTimeoutWheel().Schedule(later, p.doRetry, *req)
	}
}

func (p *shardsProxy) doRetry(arg interface{}) {
	req := arg.(raftcmdpb.Request)
	if req.ToShard == 0 {
		p.Dispatch(&req)
		return
	}

	to := ""
	if req.AllowFollower {
		to = p.router.RandomPeerStore(req.ToShard).ClientAddr
	} else {
		to = p.router.LeaderPeerStore(req.ToShard).ClientAddr
	}

	p.DispatchTo(&req, req.ToShard, to)
}

func (p *shardsProxy) getConn(addr string) (*backend, error) {
	bc := p.getConnLocked(addr)
	if p.checkConnect(bc) {
		return bc, nil
	}

	return bc, errConnect
}

func (p *shardsProxy) getConnLocked(addr string) *backend {
	if value, ok := p.backends.Load(addr); ok {
		return value.(*backend)
	}

	return p.createConn(addr)
}

func (p *shardsProxy) createConn(addr string) *backend {
	encoder, decoder := p.store.CreateRPCCliendSideCodec()
	bc := newBackend(p, addr,
		goetty.NewIOSession(goetty.WithCodec(encoder, decoder)))

	old, loaded := p.backends.LoadOrStore(addr, bc)
	if loaded {
		return old.(*backend)
	}

	return bc
}

func (p *shardsProxy) checkConnect(bc *backend) bool {
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
		logger.Errorf("connect to backend %s failed with %+v",
			bc.addr,
			err)
		return false
	}

	bc.readLoop()
	return ok
}
