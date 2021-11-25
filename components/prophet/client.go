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

package prophet

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/fagongzi/goetty"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"go.uber.org/zap"
)

var (
	// ErrClosed error of client closed
	ErrClosed = errors.New("client is closed")
	// ErrTimeout timeout
	ErrTimeout = errors.New("rpc timeout")
)

var (
	stateRunning = int32(0)
	stateStopped = int32(1)
)

// Client prophet client
type Client interface {
	Close() error
	AllocID() (uint64, error)
	CreateDestroying(id uint64, index uint64, removeData bool, replicas []uint64) (metapb.ResourceState, error)
	ReportDestroyed(id uint64, replicaID uint64) (metapb.ResourceState, error)
	GetDestroying(id uint64) (*metapb.DestroyingStatus, error)
	PutContainer(container metadata.Container) error
	GetContainer(containerID uint64) (metadata.Container, error)
	ResourceHeartbeat(meta metadata.Resource, hb rpcpb.ResourceHeartbeatReq) error
	ContainerHeartbeat(hb rpcpb.ContainerHeartbeatReq) (rpcpb.ContainerHeartbeatRsp, error)
	AskBatchSplit(res metadata.Resource, count uint32) ([]rpcpb.SplitID, error)
	NewWatcher(flag uint32) (Watcher, error)
	GetResourceHeartbeatRspNotifier() (chan rpcpb.ResourceHeartbeatRsp, error)
	// AsyncAddResources add resources asynchronously. The operation add new resources meta on the
	// prophet leader cache and embed etcd. And porphet leader has a background goroutine to notify
	// all related containers to create resource replica peer at local.
	AsyncAddResources(resources ...metadata.Resource) error
	// AsyncAddResourcesWithLeastPeers same of `AsyncAddResources`, but if the number of peers successfully
	// allocated exceed the `leastPeers`, no error will be returned.
	AsyncAddResourcesWithLeastPeers(resources []metadata.Resource, leastPeers []int) error
	// AsyncRemoveResources remove resource asynchronously. The operation only update the resource state
	// on the prophet leader cache and embed etcd. The resource actual destroy triggered in three ways as below:
	// a) Each cube node starts a backgroud goroutine to check all the resources state, and resource will
	//    destroyed if encounter removed state.
	// b) Resource heartbeat received a DestroyDirectly schedule command.
	// c) If received a resource removed event.
	AsyncRemoveResources(ids ...uint64) error
	// CheckResourceState returns resources state
	CheckResourceState(resources *roaring64.Bitmap) (rpcpb.CheckResourceStateRsp, error)

	// PutPlacementRule put placement rule
	PutPlacementRule(rule rpcpb.PlacementRule) error
	// GetAppliedRules returns applied rules of the resource
	GetAppliedRules(id uint64) ([]rpcpb.PlacementRule, error)

	// CreateJob create job
	CreateJob(metapb.Job) error
	// RemoveJob remove job
	RemoveJob(metapb.Job) error
	// ExecuteJob execute on job and returns the execute result
	ExecuteJob(metapb.Job, []byte) ([]byte, error)
}

type asyncClient struct {
	opts *options

	containerID uint64
	adapter     metadata.Adapter
	id          uint64
	contexts    sync.Map // id -> request
	leaderConn  goetty.IOSession

	ctx                   context.Context
	cancel                context.CancelFunc
	resetReadC            chan string
	resetLeaderConnC      chan struct{}
	writeC                chan *ctx
	resourceHeartbeatRspC chan rpcpb.ResourceHeartbeatRsp

	mu struct {
		sync.RWMutex
		state int32
	}
}

// NewClient create a prophet client
func NewClient(adapter metadata.Adapter, opts ...Option) Client {
	c := &asyncClient{
		opts:                  &options{},
		adapter:               adapter,
		leaderConn:            createConn(),
		resetReadC:            make(chan string),
		resetLeaderConnC:      make(chan struct{}),
		writeC:                make(chan *ctx, 128),
		resourceHeartbeatRspC: make(chan rpcpb.ResourceHeartbeatRsp, 128),
	}

	for _, opt := range opts {
		opt(c.opts)
	}
	c.opts.adjust()

	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.start()
	return c
}

func (c *asyncClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mu.state == stateStopped {
		return nil
	}

	// 1. stop write loop
	// 2. stop read loop
	// 3. no read and write, close all channels
	c.mu.state = stateStopped
	c.writeC <- newStopCtx()
	return nil
}

func (c *asyncClient) doClose() {
	close(c.resourceHeartbeatRspC)
	close(c.resetLeaderConnC)
	close(c.resetReadC)
	close(c.writeC)
}

func (c *asyncClient) AllocID() (uint64, error) {
	if !c.running() {
		return 0, ErrClosed
	}

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypeAllocIDReq

	resp, err := c.syncDo(req)
	if err != nil {
		return 0, err
	}

	return resp.AllocID.ID, nil
}

func (c *asyncClient) ResourceHeartbeat(meta metadata.Resource, hb rpcpb.ResourceHeartbeatReq) error {
	if !c.running() {
		return ErrClosed
	}

	data, err := meta.Marshal()
	if err != nil {
		return err
	}

	hb.Resource = data
	req := &rpcpb.Request{}
	req.Type = rpcpb.TypeResourceHeartbeatReq
	req.ResourceHeartbeat = hb

	c.asyncDo(req, nil)
	return nil
}

func (c *asyncClient) ContainerHeartbeat(hb rpcpb.ContainerHeartbeatReq) (rpcpb.ContainerHeartbeatRsp, error) {
	if !c.running() {
		return rpcpb.ContainerHeartbeatRsp{}, ErrClosed
	}

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypeContainerHeartbeatReq
	req.ContainerHeartbeat = hb

	resp, err := c.syncDo(req)
	if err != nil {
		c.opts.logger.Error("fail to send container heartbeat",
			zap.Uint64("container", hb.Stats.ContainerID),
			zap.Error(err))
		return rpcpb.ContainerHeartbeatRsp{}, err
	}

	return resp.ContainerHeartbeat, nil
}

func (c *asyncClient) CreateDestroying(id uint64, index uint64, removeData bool, replicas []uint64) (metapb.ResourceState, error) {
	if !c.running() {
		return metapb.ResourceState_Destroying, ErrClosed
	}

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypeCreateDestoryingReq
	req.CreateDestorying.ID = id
	req.CreateDestorying.Index = index
	req.CreateDestorying.Replicas = replicas
	req.CreateDestorying.RemoveData = removeData

	rsp, err := c.syncDo(req)
	if err != nil {
		return metapb.ResourceState_Destroying, err
	}
	return rsp.CreateDestorying.State, nil
}

func (c *asyncClient) GetDestroying(id uint64) (*metapb.DestroyingStatus, error) {
	if !c.running() {
		return nil, ErrClosed
	}

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypeGetDestoryingReq
	req.GetDestorying.ID = id

	rsp, err := c.syncDo(req)
	if err != nil {
		return nil, err
	}

	return rsp.GetDestorying.Status, nil
}

func (c *asyncClient) ReportDestroyed(id uint64, replicaID uint64) (metapb.ResourceState, error) {
	if !c.running() {
		return metapb.ResourceState_Destroying, ErrClosed
	}

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypeReportDestoryedReq
	req.ReportDestoryed.ID = id
	req.ReportDestoryed.ReplicaID = replicaID

	rsp, err := c.syncDo(req)
	if err != nil {
		return metapb.ResourceState_Destroying, err
	}

	return rsp.ReportDestoryed.State, nil
}

func (c *asyncClient) PutContainer(container metadata.Container) error {
	if !c.running() {
		return ErrClosed
	}

	data, err := container.Marshal()
	if err != nil {
		return err
	}

	c.containerID = container.ID()
	defer c.maybeRegisterContainer()

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypePutContainerReq
	req.PutContainer.Container = data
	_, err = c.syncDo(req)
	if err != nil {
		return err
	}

	return nil
}

func (c *asyncClient) GetContainer(containerID uint64) (metadata.Container, error) {
	if !c.running() {
		return nil, ErrClosed
	}

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypeGetContainerReq
	req.GetContainer.ID = containerID

	resp, err := c.syncDo(req)
	if err != nil {
		return nil, err
	}

	meta := c.adapter.NewContainer()
	err = meta.Unmarshal(resp.GetContainer.Data)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

func (c *asyncClient) AskBatchSplit(res metadata.Resource, count uint32) ([]rpcpb.SplitID, error) {
	if !c.running() {
		return nil, ErrClosed
	}

	data, err := res.Marshal()
	if err != nil {
		return nil, err
	}

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypeAskBatchSplitReq
	req.AskBatchSplit.Data = data
	req.AskBatchSplit.Count = count

	resp, err := c.syncDo(req)
	if err != nil {
		return nil, err
	}

	return resp.AskBatchSplit.SplitIDs, nil
}

func (c *asyncClient) NewWatcher(flag uint32) (Watcher, error) {
	if !c.running() {
		return nil, ErrClosed
	}

	return newWatcher(flag, c, c.opts.logger), nil
}

func (c *asyncClient) GetResourceHeartbeatRspNotifier() (chan rpcpb.ResourceHeartbeatRsp, error) {
	if !c.running() {
		return nil, ErrClosed
	}

	return c.resourceHeartbeatRspC, nil
}

func (c *asyncClient) AsyncAddResources(resources ...metadata.Resource) error {
	return c.AsyncAddResourcesWithLeastPeers(resources, make([]int, len(resources)))
}

func (c *asyncClient) AsyncAddResourcesWithLeastPeers(resources []metadata.Resource, leastPeers []int) error {
	if !c.running() {
		return ErrClosed
	}

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypeCreateResourcesReq
	for idx, res := range resources {
		data, err := res.Marshal()
		if err != nil {
			return err
		}
		req.CreateResources.Resources = append(req.CreateResources.Resources, data)
		req.CreateResources.LeastReplicas = append(req.CreateResources.LeastReplicas, uint64(leastPeers[idx]))
	}

	_, err := c.syncDo(req)
	if err != nil {
		return err
	}

	return nil
}

func (c *asyncClient) AsyncRemoveResources(ids ...uint64) error {
	if !c.running() {
		return ErrClosed
	}

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypeRemoveResourcesReq
	req.RemoveResources.IDs = append(req.RemoveResources.IDs, ids...)

	_, err := c.syncDo(req)
	if err != nil {
		return err
	}

	return nil
}

func (c *asyncClient) CheckResourceState(resources *roaring64.Bitmap) (rpcpb.CheckResourceStateRsp, error) {
	if !c.running() {
		return rpcpb.CheckResourceStateRsp{}, ErrClosed
	}

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypeCheckResourceStateReq
	req.CheckResourceState.IDs = util.MustMarshalBM64(resources)

	rsp, err := c.syncDo(req)
	if err != nil {
		return rpcpb.CheckResourceStateRsp{}, err
	}

	return rsp.CheckResourceState, nil
}

func (c *asyncClient) PutPlacementRule(rule rpcpb.PlacementRule) error {
	if !c.running() {
		return ErrClosed
	}

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypePutPlacementRuleReq
	req.PutPlacementRule.Rule = rule

	_, err := c.syncDo(req)
	if err != nil {
		return err
	}

	return nil
}

func (c *asyncClient) GetAppliedRules(id uint64) ([]rpcpb.PlacementRule, error) {
	if !c.running() {
		return nil, ErrClosed
	}

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypeGetAppliedRulesReq
	req.GetAppliedRules.ResourceID = id

	rsp, err := c.syncDo(req)
	if err != nil {
		return nil, err
	}

	return rsp.GetAppliedRules.Rules, nil
}

func (c *asyncClient) CreateJob(job metapb.Job) error {
	if !c.running() {
		return ErrClosed
	}

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypeCreateJobReq
	req.CreateJob.Job = job

	_, err := c.syncDo(req)
	if err != nil {
		return err
	}

	return nil
}

func (c *asyncClient) RemoveJob(job metapb.Job) error {
	if !c.running() {
		return ErrClosed
	}

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypeRemoveJobReq
	req.RemoveJob.Job = job

	_, err := c.syncDo(req)
	if err != nil {
		return err
	}

	return nil
}

func (c *asyncClient) ExecuteJob(job metapb.Job, data []byte) ([]byte, error) {
	if !c.running() {
		return nil, ErrClosed
	}

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypeExecuteJobReq
	req.ExecuteJob.Job = job
	req.ExecuteJob.Data = data

	rsp, err := c.syncDo(req)
	if err != nil {
		return nil, err
	}

	return rsp.ExecuteJob.Data, nil
}

func (c *asyncClient) start() {
	go c.readLoop()
	go c.writeLoop()
	c.scheduleResetLeaderConn()
	c.opts.logger.Info("started")
}

func (c *asyncClient) running() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.mu.state == stateRunning
}

func (c *asyncClient) syncDo(req *rpcpb.Request) (*rpcpb.Response, error) {
	for {
		ctx := newSyncCtx(req)
		if err := c.do(ctx); err != nil {
			return nil, err
		}

		ctx.wait()
		if ctx.err != nil {
			if ctx.err == util.ErrNotLeader {
				time.Sleep(time.Millisecond * 100)
				continue
			}

			return nil, ctx.err
		}

		return ctx.resp, nil
	}
}

func (c *asyncClient) asyncDo(req *rpcpb.Request, cb func(*rpcpb.Response, error)) {
	c.do(newAsyncCtx(req, cb))
}

func (c *asyncClient) do(ctx *ctx) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.mu.state == stateStopped {
		return ErrClosed
	}

	ctx.req.ID = c.nextID()
	if ctx.sync || ctx.cb != nil {
		c.contexts.Store(ctx.req.ID, ctx)
		util.DefaultTimeoutWheel().Schedule(c.opts.rpcTimeout, c.timeout, ctx.req.ID)
	}
	c.writeC <- ctx
	return nil
}

func (c *asyncClient) timeout(arg interface{}) {
	if v, ok := c.contexts.Load(arg); ok {
		c.contexts.Delete(arg)
		v.(*ctx).done(nil, ErrTimeout)
	}
}

func (c *asyncClient) scheduleResetLeaderConn() bool {
	if !c.running() {
		return false
	}

	select {
	case c.resetLeaderConnC <- struct{}{}:
		c.opts.logger.Debug(" schedule reset leader connection")
	case <-time.After(time.Second * 10):
		c.opts.logger.Fatal("BUG: schedule reset leader connection timeout")
	}
	return true
}

func (c *asyncClient) nextID() uint64 {
	return atomic.AddUint64(&c.id, 1)
}

func (c *asyncClient) writeLoop() {
	for {
		select {
		case ctx, ok := <-c.writeC:
			if ok {
				if ctx.stop {
					c.cancel()
					c.leaderConn.Close()
					return
				}

				c.doWrite(ctx)
			}
		case _, ok := <-c.resetLeaderConnC:
			if ok {
				if err := c.resetLeaderConn(); err != nil {
					c.opts.logger.Error("fail to reset leader connection",
						zap.Error(err))
				}
			}
		}
	}
}

func (c *asyncClient) readLoop() {
	c.opts.logger.Info("read loop started")
	defer func() {
		c.doClose()
		c.opts.logger.Error("read loop stopped")
	}()

OUTER:
	for {
		select {
		case <-c.ctx.Done():
			c.contexts.Range(func(key, value interface{}) bool {
				if value != nil {
					value.(*ctx).done(nil, ErrClosed)
				}
				return true
			})
			return
		case leader, ok := <-c.resetReadC:
			if ok {
				c.opts.logger.Info("read loop actived, ready to read from leader")
				for {
					msg, err := c.leaderConn.Read()
					if err != nil {
						c.opts.logger.Error("fail to read resp from leader",
							zap.String("leader", leader),
							zap.Error(err))
						if !c.scheduleResetLeaderConn() {
							return
						}
						continue OUTER
					}

					resp := msg.(*rpcpb.Response)
					if resp.Error != "" && util.IsNotLeaderError(resp.Error) {
						if !c.scheduleResetLeaderConn() {
							return
						}

						// retry
						c.requestDoneWithRetry(resp)
						continue OUTER
					}

					if c.maybeAddResourceHeartbeatResp(resp) {
						continue
					}

					c.requestDone(resp)
				}
			}
		}
	}
}

func (c *asyncClient) requestDoneWithRetry(resp *rpcpb.Response) {
	v, ok := c.contexts.Load(resp.ID)
	if ok && v != nil {
		v.(*ctx).done(nil, util.ErrNotLeader)
	}
}

func (c *asyncClient) requestDone(resp *rpcpb.Response) {
	v, ok := c.contexts.Load(resp.ID)
	if ok && v != nil {
		c.contexts.Delete(resp.ID)
		if resp.Error != "" {
			v.(*ctx).done(nil, errors.New(resp.Error))
		} else {
			v.(*ctx).done(resp, nil)
		}
	}
}

func (c *asyncClient) maybeAddResourceHeartbeatResp(resp *rpcpb.Response) bool {
	if resp.Type == rpcpb.TypeResourceHeartbeatRsp &&
		resp.Error == "" &&
		resp.ResourceHeartbeat.ResourceID > 0 {
		c.opts.logger.Debug("resource heartbeat response added",
			zap.Uint64("resource", resp.ResourceHeartbeat.ResourceID))
		c.resourceHeartbeatRspC <- resp.ResourceHeartbeat
		return true
	}
	return false
}

func (c *asyncClient) doWrite(ctx *ctx) {
	err := c.leaderConn.Write(ctx.req)
	if err != nil {
		c.opts.logger.Error("fail to send request",
			zap.Uint64("id", ctx.req.ID),
			zap.Error(err))
	}
}

func (c *asyncClient) resetLeaderConn() error {
	c.leaderConn.Close()
	_, err := c.initLeaderConn(c.leaderConn, c.opts.rpcTimeout, true)
	return err
}

func (c *asyncClient) initLeaderConn(conn goetty.IOSession, timeout time.Duration, registerContainer bool) (string, error) {
	addr := ""
	for {
		if !c.running() {
			return "", ErrClosed
		}

		select {
		case <-time.After(timeout):
			return "", ErrTimeout
		default:
			leader := c.opts.leaderGetter()
			if leader != nil {
				addr = leader.Addr
			}

			if addr != "" {
				c.opts.logger.Info("start connect to leader",
					zap.String("leader", addr))
				conn.Close()
				ok, err := conn.Connect(addr, c.opts.rpcTimeout)
				if err == nil && ok {
					if registerContainer {
						c.maybeRegisterContainer()
					}

					c.opts.logger.Info("connect to leader succeed",
						zap.String("leader", addr))
					if registerContainer {
						select {
						case c.resetReadC <- addr:
						case <-time.After(time.Second * 10):
							c.opts.logger.Fatal("BUG: active read loop timeout")
						}
					}

					return addr, nil
				}

				c.opts.logger.Error("fail to init leader connection, retry later",
					zap.Error(err))
			}
		}

		time.Sleep(time.Millisecond * 200)
	}
}

func (c *asyncClient) maybeRegisterContainer() {
	if c.containerID > 0 {
		req := &rpcpb.Request{}
		req.Type = rpcpb.TypeRegisterContainer
		req.ContainerID = c.containerID
		c.doWrite(newAsyncCtx(req, nil))
	}
}

type ctx struct {
	state uint64
	req   *rpcpb.Request
	resp  *rpcpb.Response
	err   error
	c     chan struct{}
	cb    func(resp *rpcpb.Response, err error)
	sync  bool
	stop  bool
}

func newStopCtx() *ctx {
	return &ctx{
		stop: true,
	}
}

func newSyncCtx(req *rpcpb.Request) *ctx {
	return &ctx{
		req:  req,
		c:    make(chan struct{}),
		sync: true,
	}
}

func newAsyncCtx(req *rpcpb.Request, cb func(resp *rpcpb.Response, err error)) *ctx {
	return &ctx{
		req:  req,
		cb:   cb,
		sync: false,
	}
}

func (c *ctx) done(resp *rpcpb.Response, err error) {
	if atomic.CompareAndSwapUint64(&c.state, 0, 1) {
		if c.sync {
			c.resp = resp
			c.err = err
			close(c.c)
		} else {
			if c.cb != nil {
				c.cb(resp, err)
			}
		}
	}
}

func (c *ctx) wait() {
	if c.sync {
		<-c.c
	}
}
