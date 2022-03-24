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
	"bytes"
	"errors"
	"sync"
	"time"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/errorpb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/util"
	"go.uber.org/zap"
)

var (
	// ErrTimeout timeout error
	ErrTimeout = errors.New("exec timeout")
	// ErrKeysNotInShard keys not in shard, request data needs to be split
	ErrKeysNotInShard = errors.New("keys not in shard, request data needs to be split")

	errStopped = errors.New("stopped")
)

var (
	defaultRetryInterval = time.Second
)

// SuccessCallback request success callback
type SuccessCallback func(resp rpcpb.Response)

// FailureCallback request failure callback
type FailureCallback func(requestID []byte, err error)

// RetryController retry controller
type RetryController interface {
	// Retry used to control retry if retryable error encountered. returns false means stop retry.
	Retry(requestID []byte) (rpcpb.Request, bool)
}

// ShardsProxy Shards proxy, distribute the appropriate request to the corresponding backend,
// retry the request for the error
type ShardsProxy interface {
	Start() error
	Stop() error
	Dispatch(req rpcpb.Request) error
	DispatchTo(req rpcpb.Request, shard Shard, store string) error
	SetCallback(SuccessCallback, FailureCallback)
	SetRetryController(retryController RetryController)
	OnResponse(rpcpb.ResponseBatch)
	Router() Router
}

type backendFactory interface {
	create(string, SuccessCallback, FailureCallback) (backend, error)
}

type backend interface {
	dispatch(rpcpb.Request) error
	close()
}

type shardsProxyConfig struct {
	backendFactory  backendFactory
	successCallback SuccessCallback
	failureCallback FailureCallback
	retryController RetryController
	logger          *zap.Logger
	router          Router
	rpcpb           proxyRPC
	maxBodySize     int
	retryInterval   time.Duration
}

type shardsProxyBuilder struct {
	cfg shardsProxyConfig
}

func newShardsProxyBuilder() *shardsProxyBuilder {
	return &shardsProxyBuilder{}
}

func (sb *shardsProxyBuilder) withRetryInterval(value time.Duration) *shardsProxyBuilder {
	sb.cfg.retryInterval = value
	return sb
}

func (sb *shardsProxyBuilder) withMaxBodySize(size int) *shardsProxyBuilder {
	sb.cfg.maxBodySize = size
	return sb
}

func (sb *shardsProxyBuilder) withBackendFactory(factory backendFactory) *shardsProxyBuilder {
	sb.cfg.backendFactory = factory
	return sb
}

func (sb *shardsProxyBuilder) withRPC(rpcpb proxyRPC) *shardsProxyBuilder {
	sb.cfg.rpcpb = rpcpb
	return sb
}

func (sb *shardsProxyBuilder) withRequestCallback(successCallback SuccessCallback, failureCallback FailureCallback) *shardsProxyBuilder {
	sb.cfg.failureCallback = failureCallback
	sb.cfg.successCallback = successCallback
	return sb
}

func (sb *shardsProxyBuilder) withLogger(logger *zap.Logger) *shardsProxyBuilder {
	sb.cfg.logger = logger
	return sb
}

func (sb *shardsProxyBuilder) build(router Router) (ShardsProxy, error) {
	sb.cfg.logger = log.Adjust(sb.cfg.logger)

	if sb.cfg.successCallback == nil {
		sb.cfg.successCallback = func(r rpcpb.Response) {}
	}

	if sb.cfg.failureCallback == nil {
		sb.cfg.failureCallback = func(id []byte, e error) {}
	}

	if sb.cfg.retryInterval == 0 {
		sb.cfg.retryInterval = defaultRetryInterval
	}

	sb.cfg.router = router
	return newShardsProxy(sb.cfg)
}

type shardsProxy struct {
	sync.RWMutex

	cfg      shardsProxyConfig
	logger   *zap.Logger
	backends map[string]backend
	stopped  bool
}

func newShardsProxy(cfg shardsProxyConfig) (ShardsProxy, error) {
	return &shardsProxy{
		cfg:      cfg,
		logger:   cfg.logger,
		backends: make(map[string]backend),
	}, nil
}

func (p *shardsProxy) Start() error {
	p.Lock()
	defer p.Unlock()

	if p.stopped {
		return errStopped
	}

	if p.cfg.rpcpb != nil {
		return p.cfg.rpcpb.start()
	}
	return nil
}

func (p *shardsProxy) Stop() error {
	p.Lock()
	defer p.Unlock()

	if p.stopped {
		return nil
	}

	if p.cfg.rpcpb != nil {
		p.cfg.rpcpb.stop()
	}

	for k, b := range p.backends {
		b.close()
		delete(p.backends, k)
	}
	p.stopped = true
	return nil
}

func (p *shardsProxy) SetCallback(success SuccessCallback, failure FailureCallback) {
	p.cfg.successCallback = success
	p.cfg.failureCallback = failure
}

func (p *shardsProxy) SetRetryController(retryController RetryController) {
	p.cfg.retryController = retryController
}

func (p *shardsProxy) Dispatch(req rpcpb.Request) error {
	if req.ToShard == 0 {
		shard, to := p.cfg.router.SelectShard(req.Group, req.Key)
		return p.DispatchTo(req, shard, to)
	}

	return p.DispatchTo(req,
		p.cfg.router.GetShard(req.ToShard),
		p.cfg.router.LeaderReplicaStore(req.ToShard).ClientAddress)
}

func (p *shardsProxy) DispatchTo(req rpcpb.Request, shard Shard, to string) error {
	if ce := p.logger.Check(zap.DebugLevel, "dispatch request"); ce != nil {
		ce.Write(log.HexField("id", req.ID),
			zap.Uint64("to-shard", shard.ID),
			zap.String("to-store", to),
			log.RaftRequestField("request", &req))
	}

	// No leader, retry after a leader tick
	if to == "" {
		p.retryDispatch(req.ID, "dispath to nil store")
		return nil
	}

	// the current request is designed to operate on multiple Keys
	if req.KeysRange != nil && !keysRangeInShard(req.KeysRange, shard) {
		if ce := p.logger.Check(zap.DebugLevel, "keys not in shard"); ce != nil {
			ce.Write(log.HexField("id", req.ID),
				log.ShardField("shard", shard))
		}
		return ErrKeysNotInShard
	}

	req.Epoch = shard.Epoch
	return p.forwardToBackend(req, to)
}

func (p *shardsProxy) Router() Router {
	return p.cfg.router
}

func (p *shardsProxy) forwardToBackend(req rpcpb.Request, leader string) error {
	var err error
	bc := p.getBackend(leader)
	if bc == nil {
		p.Lock()
		defer p.Unlock()

		if p.stopped {
			return errStopped
		}

		bc, err = p.createBackendLocked(leader)
		if err != nil {
			return err
		}
	}

	return bc.dispatch(req)
}

func (p *shardsProxy) OnResponse(resp rpcpb.ResponseBatch) {
	for _, rsp := range resp.Responses {
		if rsp.PID != 0 && p.cfg.rpcpb != nil {
			p.cfg.rpcpb.onResponse(resp.Header, rsp)
		} else {
			p.onLocalResp(resp.Header, rsp)
		}
	}
}

func (p *shardsProxy) getBackend(addr string) backend {
	p.RLock()
	defer p.RUnlock()

	return p.backends[addr]
}

func (p *shardsProxy) createBackendLocked(addr string) (backend, error) {
	bc, err := p.cfg.backendFactory.create(addr, p.done, p.doneWithError)
	if err != nil {
		return nil, err
	}

	p.addBackendLocked(addr, bc)
	return bc, nil
}

func (p *shardsProxy) addBackendLocked(addr string, bc backend) {
	p.backends[addr] = bc
}

func (p *shardsProxy) onLocalResp(header rpcpb.ResponseBatchHeader, rsp rpcpb.Response) {
	rsp.Error = header.Error
	p.done(rsp)
}

func (p *shardsProxy) doneWithError(requestID []byte, err error) {
	p.retryDispatch(requestID, err.Error())
}

func (p *shardsProxy) done(rsp rpcpb.Response) {
	if ce := p.logger.Check(zap.DebugLevel, "requests done"); ce != nil {
		ce.Write(log.RaftResponseField("resp", &rsp))
	}

	if !errorpb.HasError(rsp.Error) {
		p.cfg.successCallback(rsp)
		return
	}

	if !errorpb.Retryable(rsp.Error) {
		if rsp.Error.ShardUnavailable != nil {
			p.cfg.failureCallback(rsp.ID, NewShardUnavailableErr(rsp.Error.ShardUnavailable.ShardID))
			return
		}
		p.cfg.failureCallback(rsp.ID, errors.New(rsp.Error.String()))
		return
	}

	p.adjustRoute(rsp.Error)
	p.retryDispatch(rsp.ID, rsp.Error.String())
}

func (p *shardsProxy) adjustRoute(err errorpb.Error) {
	if err.NotLeader != nil {
		p.cfg.router.UpdateLeader(err.NotLeader.ShardID, err.NotLeader.Leader.ID)
	}
}

func (p *shardsProxy) retryDispatch(requestID []byte, err string) {
	if p.cfg.retryController == nil {
		if ce := p.logger.Check(zap.DebugLevel, "dispatch request failed with no retry"); ce != nil {
			ce.Write(log.HexField("id", requestID),
				log.ReasonField("retry controller not set"),
				zap.String("cause", err))
		}
		p.cfg.failureCallback(requestID, errors.New(err))
		return
	}

	req, ok := p.cfg.retryController.Retry(requestID)
	if !ok {
		if ce := p.logger.Check(zap.DebugLevel, "dispatch request failed with no retry"); ce != nil {
			ce.Write(log.HexField("id", requestID),
				log.ReasonField("retry controller return false"),
				zap.String("cause", err))
		}
		p.cfg.failureCallback(requestID, errors.New(err))
		return
	}

	// FIXME: more efficient retry mechanism
	if ce := p.logger.Check(zap.DebugLevel, "dispatch request failed, retry later"); ce != nil {
		ce.Write(log.HexField("id", req.ID),
			zap.String("cause", err))
	}
	util.DefaultTimeoutWheel().Schedule(p.cfg.retryInterval, p.doRetry, req)
}

func (p *shardsProxy) doRetry(arg interface{}) {
	req := arg.(rpcpb.Request)
	if req.ToShard == 0 {
		err := p.Dispatch(req)
		if err != nil {
			p.cfg.failureCallback(req.ID, err)
		}
		return
	}

	err := p.DispatchTo(req, p.cfg.router.GetShard(req.ToShard), p.cfg.router.LeaderReplicaStore(req.ToShard).ClientAddress)
	if err != nil {
		p.cfg.failureCallback(req.ID, err)
	}
}

func keysRangeInShard(keys *rpcpb.Range, shard Shard) bool {
	return (len(shard.Start) == 0 || bytes.Compare(shard.Start, keys.From) <= 0) &&
		(len(shard.End) == 0 || bytes.Compare(shard.End, keys.To) >= 0)
}
