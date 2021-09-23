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
	PutContainer(container metadata.Container) error
	GetContainer(containerID uint64) (metadata.Container, error)
	ResourceHeartbeat(meta metadata.Resource, hb rpcpb.ResourceHeartbeatReq) error
	ContainerHeartbeat(hb rpcpb.ContainerHeartbeatReq) (rpcpb.ContainerHeartbeatRsp, error)
	AskSplit(res metadata.Resource) (rpcpb.SplitID, error)
	ReportSplit(left, right metadata.Resource) error
	AskBatchSplit(res metadata.Resource, count uint32) ([]rpcpb.SplitID, error)
	ReportBatchSplit(results ...metadata.Resource) error
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
	// on the prophet leader cache and embed etcd. The resource actual destory triggered in three ways as below:
	// a) Each cube node starts a backgroud goroutine to check all the resources state, and resource will
	//    destoryed if encounter removed state.
	// b) Resource heartbeat received a DestoryDirectly schedule command.
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

	containerID   uint64
	adapter       metadata.Adapter
	id            uint64
	state         int32
	contexts      sync.Map // id -> request
	leaderConn    goetty.IOSession
	currentLeader string

	ctx                   context.Context
	cancel                context.CancelFunc
	resetReadC            chan struct{}
	resetLeaderConnC      chan struct{}
	writeC                chan *ctx
	resourceHeartbeatRspC chan rpcpb.ResourceHeartbeatRsp
}

// NewClient create a prophet client
func NewClient(adapter metadata.Adapter, opts ...Option) Client {
	c := &asyncClient{
		opts:                  &options{},
		adapter:               adapter,
		leaderConn:            createConn(),
		resetReadC:            make(chan struct{}),
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
	// 1. stop write loop
	// 2. stop read loop
	// 3. no read and write, close all channels
	if atomic.CompareAndSwapInt32(&c.state, stateRunning, stateStopped) {
		c.writeC <- newStopCtx()
	}
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
		util.GetLogger().Errorf("container %d heartbeat failed with %+v",
			hb.Stats.ContainerID,
			err)
		return rpcpb.ContainerHeartbeatRsp{}, err
	}

	return resp.ContainerHeartbeat, nil
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

func (c *asyncClient) AskSplit(res metadata.Resource) (rpcpb.SplitID, error) {
	if !c.running() {
		return rpcpb.SplitID{}, ErrClosed
	}

	data, err := res.Marshal()
	if err != nil {
		return rpcpb.SplitID{}, err
	}

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypeAskSplitReq
	req.AskSplit.Data = data

	resp, err := c.syncDo(req)
	if err != nil {
		return rpcpb.SplitID{}, err
	}

	return resp.AskSplit.SplitID, nil
}

func (c *asyncClient) ReportSplit(left, right metadata.Resource) error {
	if !c.running() {
		return ErrClosed
	}

	leftData, err := left.Marshal()
	if err != nil {
		return err
	}

	rightData, err := right.Marshal()
	if err != nil {
		return err
	}

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypeReportSplitReq
	req.ReportSplit.Left = leftData
	req.ReportSplit.Right = rightData

	_, err = c.syncDo(req)
	if err != nil {
		return err
	}

	return nil
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

func (c *asyncClient) ReportBatchSplit(results ...metadata.Resource) error {
	if !c.running() {
		return ErrClosed
	}

	var resources [][]byte
	for _, res := range results {
		v, err := res.Marshal()
		if err != nil {
			return err
		}

		resources = append(resources, v)
	}

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypeBatchReportSplitReq
	req.BatchReportSplit.Resources = resources

	_, err := c.syncDo(req)
	if err != nil {
		return err
	}

	return nil
}

func (c *asyncClient) NewWatcher(flag uint32) (Watcher, error) {
	if !c.running() {
		return nil, ErrClosed
	}

	return newWatcher(flag, c), nil
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
	util.GetLogger().Infof("client started")
}

func (c *asyncClient) running() bool {
	return atomic.LoadInt32(&c.state) == stateRunning
}

func (c *asyncClient) syncDo(req *rpcpb.Request) (*rpcpb.Response, error) {
	for {
		if !c.running() {
			return nil, ErrClosed
		}

		ctx := newSyncCtx(req)
		c.do(ctx)
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

func (c *asyncClient) do(ctx *ctx) {
	ctx.req.ID = c.nextID()
	if ctx.sync || ctx.cb != nil {
		c.contexts.Store(ctx.req.ID, ctx)
		util.DefaultTimeoutWheel().Schedule(c.opts.rpcTimeout, c.timeout, ctx.req.ID)
	}
	c.writeC <- ctx
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
		util.GetLogger().Debugf("client schedule reset leader conn")
	case <-time.After(time.Second * 10):
		util.GetLogger().Fatalf("BUG: client schedule reset leader conn")
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
					util.GetLogger().Errorf("reset leader connection failed with %+v",
						err)
				}
			}
		}
	}
}

func (c *asyncClient) readLoop() {
	util.GetLogger().Info("client read loop started")
	defer func() {
		c.doClose()
		util.GetLogger().Errorf("client read loop stopped")
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
		case _, ok := <-c.resetReadC:
			if ok {
				util.GetLogger().Infof("client read loop actived, ready to read from leader")
				for {
					msg, err := c.leaderConn.Read()
					if err != nil {
						util.GetLogger().Errorf("client read resp from leader %s failed with %+v",
							c.currentLeader,
							err)
						if !c.scheduleResetLeaderConn() {
							return
						}
						continue OUTER
					}

					resp := msg.(*rpcpb.Response)
					v, ok := c.contexts.Load(resp.ID)
					if resp.Error != "" && util.IsNotLeaderError(resp.Error) {
						if !c.scheduleResetLeaderConn() {
							return
						}

						// retry
						if ok && v != nil {
							v.(*ctx).done(nil, util.ErrNotLeader)
						}
						continue OUTER
					}

					if resp.Type == rpcpb.TypeResourceHeartbeatRsp && resp.Error == "" && resp.ResourceHeartbeat.ResourceID > 0 {
						util.GetLogger().Debugf("resource hb resp %+v added",
							resp.ResourceHeartbeat)
						c.resourceHeartbeatRspC <- resp.ResourceHeartbeat
						continue
					}

					if ok && v != nil {
						c.contexts.Delete(resp.ID)
						if resp.Error != "" {
							v.(*ctx).done(nil, errors.New(resp.Error))
						} else {
							v.(*ctx).done(resp, nil)
						}
					}
				}
			}
		}
	}
}

func (c *asyncClient) doWrite(ctx *ctx) {
	err := c.leaderConn.Write(ctx.req)
	if err != nil {
		util.GetLogger().Errorf("client write %+v failed with %+v",
			ctx.req, err)
	}
}

func (c *asyncClient) resetLeaderConn() error {
	c.leaderConn.Close()
	return c.initLeaderConn(c.leaderConn, c.opts.rpcTimeout, true)
}

func (c *asyncClient) initLeaderConn(conn goetty.IOSession, timeout time.Duration, registerContainer bool) error {
	addr := ""
	for {
		if !c.running() {
			return ErrClosed
		}

		select {
		case <-time.After(timeout):
			return ErrTimeout
		default:
			leader := c.opts.leaderGetter()
			if leader != nil {
				addr = leader.Addr
			}

			if addr != "" {
				util.GetLogger().Infof("client start init connection to leader %+s", addr)
				conn.Close()
				ok, err := conn.Connect(addr, c.opts.rpcTimeout)
				if err == nil && ok {
					if registerContainer {
						c.maybeRegisterContainer()
					}

					c.currentLeader = addr
					util.GetLogger().Infof("client start connect to leader %s", addr)
					if registerContainer {
						select {
						case c.resetReadC <- struct{}{}:
						case <-time.After(time.Second * 10):
							util.GetLogger().Fatalf("BUG: active read loop timeout")
						}
					}

					return nil
				}

				util.GetLogger().Errorf("init leader conn failed with %+v, retry later", err)
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
		c.asyncDo(req, nil)
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
