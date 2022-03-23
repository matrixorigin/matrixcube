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

package client

import (
	"context"
	"sync"

	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/util/uuid"
	"go.uber.org/zap"
)

// Option client option
type Option func(*Future)

// WithShardGroup set shard group to execute the request
func WithShardGroup(group uint64) Option {
	return func(c *Future) {
		c.req.Group = group
	}
}

// WithRouteKey use the specified key to route request
func WithRouteKey(key []byte) Option {
	return func(c *Future) {
		c.req.Key = key
	}
}

// WithKeysRange If the current request operates on multiple Keys, set the range [from, to) of Keys
// operated by the current request. The client needs to split the request again if it wants
// to re-route according to KeysRange after the data management scope of the Shard has
// changed, or if it returns the specified error.
func WithKeysRange(from, to []byte) Option {
	return func(c *Future) {
		c.req.KeysRange = &rpcpb.Range{From: from, To: to}
	}
}

// WithShard use the specified shard to route request
func WithShard(shard uint64) Option {
	return func(c *Future) {
		c.req.ToShard = shard
	}
}

// Future is used to obtain response data synchronously.
type Future struct {
	value []byte
	err   error
	req   rpcpb.Request
	ctx   context.Context
	c     chan struct{}

	mu struct {
		sync.Mutex
		closed bool
	}
}

func newFuture(ctx context.Context, req rpcpb.Request) *Future {
	return &Future{
		ctx: ctx,
		req: req,
		c:   make(chan struct{}, 1),
	}
}

// Get get the response data synchronously, blocking until `context.Done` or the response is received.
// This method cannot be called more than once. After calling `Get`, `Close` must be called to close
// `Future`.
func (f *Future) Get() ([]byte, error) {
	select {
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	case <-f.c:
		return f.value, f.err
	}
}

// Close close the future.
func (f *Future) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()

	close(f.c)
	f.mu.closed = true
}

func (f *Future) canRetry() bool {
	select {
	case <-f.ctx.Done():
		return false
	default:
		return true
	}
}

func (f *Future) done(value []byte, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.mu.closed {
		f.value = value
		f.err = err
		select {
		case f.c <- struct{}{}:
		default:
			panic("BUG")
		}
	}
}

// Client is a cube client, providing read and write access to the external.
type Client interface {
	// Start start the cube client
	Start() error
	// Stop stop the cube client
	Stop() error

	// Admin exec the admin request, and use the `Future` to get the response.
	Admin(ctx context.Context, requestType uint64, payload []byte, opts ...Option) *Future
	// Write exec the write request, and use the `Future` to get the response.
	Write(ctx context.Context, requestType uint64, payload []byte, opts ...Option) *Future
	// Read exec the read request, and use the `Future` to get the response
	Read(ctx context.Context, requestType uint64, payload []byte, opts ...Option) *Future

	// AddLabelToShard add lable to shard, and use the `Future` to get the response
	AddLabelToShard(ctx context.Context, name, value string, shard uint64) *Future
}

var _ Client = (*client)(nil)

// client a tcp application server
type client struct {
	cfg         Cfg
	inflights   sync.Map // request id -> *Future
	shardsProxy raftstore.ShardsProxy
	dispatcher  func(req rpcpb.Request, proxy raftstore.ShardsProxy) error
	logger      *zap.Logger
}

// NewClient creates and return a cube client
func NewClient(cfg Cfg) Client {
	return NewClientWithDispatcher(cfg, nil)
}

// NewClientWithDispatcher similar to NewClient, but with a special dispatcher
func NewClientWithDispatcher(cfg Cfg, dispatcher func(req rpcpb.Request, proxy raftstore.ShardsProxy) error) Client {
	return &client{
		cfg:        cfg,
		dispatcher: dispatcher,
	}
}

func (s *client) Start() error {
	s.logger = s.cfg.Store.GetConfig().Logger.Named("client")

	s.logger.Info("begin to start cube client")
	if !s.cfg.storeStarted {
		s.cfg.Store.Start()
	}
	s.shardsProxy = s.cfg.Store.GetShardsProxy()
	s.shardsProxy.SetCallback(s.done, s.doneError)
	s.shardsProxy.SetRetryController(s)
	s.logger.Info("cube client started")
	return nil
}

func (s *client) Stop() error {
	if !s.cfg.storeStarted {
		s.cfg.Store.Stop()
	}
	s.logger.Info("cube client stopped")
	return nil
}

func (s *client) Write(ctx context.Context, requestType uint64, payload []byte, opts ...Option) *Future {
	return s.exec(ctx, requestType, payload, rpcpb.Write, opts...)
}

func (s *client) Read(ctx context.Context, requestType uint64, payload []byte, opts ...Option) *Future {
	return s.exec(ctx, requestType, payload, rpcpb.Read, opts...)
}

func (s *client) Admin(ctx context.Context, requestType uint64, payload []byte, opts ...Option) *Future {
	return s.exec(ctx, requestType, payload, rpcpb.Admin, opts...)
}

func (s *client) AddLabelToShard(ctx context.Context, name, value string, shard uint64) *Future {
	payload := protoc.MustMarshal(&rpcpb.UpdateLabelsRequest{
		Labels: []metapb.Label{{Key: name, Value: value}},
		Policy: rpcpb.Add,
	})
	return s.exec(ctx, uint64(rpcpb.AdminUpdateLabels), payload, rpcpb.Admin, WithShard(shard))
}

func (s *client) exec(ctx context.Context, requestType uint64, payload []byte, cmdType rpcpb.CmdType, opts ...Option) *Future {
	req := rpcpb.Request{}
	req.ID = uuid.NewV4().Bytes()
	req.Type = cmdType
	req.CustomType = requestType
	req.Cmd = payload

	f := newFuture(ctx, req)
	for _, opt := range opts {
		opt(f)
	}
	s.inflights.Store(hack.SliceToString(f.req.ID), f)

	if ce := s.logger.Check(zap.DebugLevel, "begin to send request"); ce != nil {
		ce.Write(log.RequestIDField(req.ID))
	}

	var err error
	if s.dispatcher != nil {
		err = s.dispatcher(f.req, s.shardsProxy)
	} else {
		err = s.shardsProxy.Dispatch(f.req)
	}
	if err != nil {
		f.done(nil, err)
	}
	return f
}

func (s *client) Retry(requestID []byte) (rpcpb.Request, bool) {
	id := hack.SliceToString(requestID)
	if c, ok := s.inflights.Load(id); ok {
		f := c.(*Future)
		if f.canRetry() {
			return f.req, true
		}
	}

	return rpcpb.Request{}, false
}

func (s *client) done(resp rpcpb.Response) {
	if ce := s.logger.Check(zap.DebugLevel, "response received"); ce != nil {
		ce.Write(log.RequestIDField(resp.ID))
	}

	id := hack.SliceToString(resp.ID)
	if c, ok := s.inflights.Load(hack.SliceToString(resp.ID)); ok {
		s.inflights.Delete(id)
		c.(*Future).done(resp.Value, nil)
	} else {
		if ce := s.logger.Check(zap.DebugLevel, "response skipped"); ce != nil {
			ce.Write(log.RequestIDField(resp.ID), log.ReasonField("missing ctx"))
		}
	}
}

func (s *client) doneError(requestID []byte, err error) {
	if ce := s.logger.Check(zap.DebugLevel, "error response received"); ce != nil {
		ce.Write(log.RequestIDField(requestID), zap.Error(err))
	}

	id := hack.SliceToString(requestID)
	if c, ok := s.inflights.Load(id); ok {
		s.inflights.Delete(id)
		c.(*Future).done(nil, err)
	}
}
