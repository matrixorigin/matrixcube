package prophet

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
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
	resetLeaderConnC      chan string
	writeC                chan *ctx
	resourceHeartbeatRspC chan rpcpb.ResourceHeartbeatRsp
	allocIDC              chan uint64
}

// NewClient create a prophet client
func NewClient(adapter metadata.Adapter, opts ...Option) Client {
	c := &asyncClient{
		opts:                  &options{},
		adapter:               adapter,
		leaderConn:            createConn(),
		resetReadC:            make(chan struct{}, 1),
		resetLeaderConnC:      make(chan string, 1),
		writeC:                make(chan *ctx, 128),
		resourceHeartbeatRspC: make(chan rpcpb.ResourceHeartbeatRsp, 128),
		allocIDC:              make(chan uint64, 128),
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
	if atomic.CompareAndSwapInt32(&c.state, stateRunning, stateStopped) {
		c.doClose()

	}
	return nil
}

func (c *asyncClient) AllocID() (uint64, error) {
	if !c.running() {
		return 0, ErrClosed
	}

	req := &rpcpb.Request{}
	req.Type = rpcpb.TypeAllocIDReq

	c.asyncDo(req, func(resp *rpcpb.Response, err error) {
		if err != nil {
			util.GetLogger().Errorf("alloc id failed with %+v", err)
		} else {
			c.allocIDC <- resp.AllocID.ID
		}
	})

	select {
	case id, ok := <-c.allocIDC:
		if !ok {
			return 0, ErrClosed
		}

		return id, nil
	case <-time.After(c.opts.rpcTimeout):
		return 0, ErrTimeout
	}
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

func (c *asyncClient) doClose() {
	c.cancel()
	close(c.resourceHeartbeatRspC)
	close(c.resetLeaderConnC)
	close(c.writeC)
	close(c.allocIDC)
	c.leaderConn.Close()
}

func (c *asyncClient) start() {
	err := c.initLeaderConn(c.leaderConn, "", time.Minute)
	if err != nil {
		util.GetLogger().Fatalf("connect to leader failed after 1 minute, error %+v", err)
	}

	go c.writeLoop()
	go c.readLoop()
	util.GetLogger().Infof("client started")
}

func (c *asyncClient) running() bool {
	return atomic.LoadInt32(&c.state) == stateRunning
}

func (c *asyncClient) syncDo(req *rpcpb.Request) (*rpcpb.Response, error) {
	ctx := newSyncCtx(req)
	c.do(ctx)
	ctx.wait()

	if ctx.err != nil {
		return nil, ctx.err
	}

	if ctx.resp.Error != "" {
		return nil, errors.New(ctx.resp.Error)
	}

	return ctx.resp, nil
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
		c.scheduleResetLeaderConn("")
	}
}

func (c *asyncClient) scheduleResetLeaderConn(newLeader string) {
	if atomic.LoadInt32(&c.state) != stateRunning {
		return
	}

	select {
	case c.resetLeaderConnC <- newLeader:
	default:
	}
}

func (c *asyncClient) nextID() uint64 {
	return atomic.AddUint64(&c.id, 1)
}

func (c *asyncClient) writeLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case ctx, ok := <-c.writeC:
			if ok {
				c.doWrite(ctx)
			}
		case newLeader, ok := <-c.resetLeaderConnC:
			if ok {
				if err := c.resetLeaderConn(newLeader); err != nil {
					util.GetLogger().Errorf("reset leader connection failed with %+v",
						newLeader,
						err)
				}
			}
		}
	}
}

func (c *asyncClient) readLoop() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case _, ok := <-c.resetReadC:
			if ok {
				util.GetLogger().Infof("client read loop started")
				for {
					msg, err := c.leaderConn.Read()
					if err != nil {
						util.GetLogger().Errorf("read resp from leader %s failed with %+v",
							c.currentLeader,
							err)
						c.scheduleResetLeaderConn("")
						break
					}

					util.GetLogger().Debugf("read msg %+v",
						msg)
					resp := msg.(*rpcpb.Response)
					if resp.Type == rpcpb.TypeResourceHeartbeatRsp && resp.Error == "" && resp.ResourceHeartbeat.ResourceID > 0 {
						util.GetLogger().Infof("resource hb resp %+v added",
							resp.ResourceHeartbeat)
						c.resourceHeartbeatRspC <- resp.ResourceHeartbeat
						continue
					}

					if v, ok := c.contexts.Load(resp.ID); ok {
						if resp.Error != "" && util.IsNotLeaderError(resp.Error) {
							c.scheduleResetLeaderConn(resp.Leader)
							c.writeC <- v.(*ctx)
							break
						}

						c.contexts.Delete(resp.ID)
						v.(*ctx).done(resp, nil)
					}
				}
			}
		}
	}
}

func (c *asyncClient) doWrite(ctx *ctx) {
	err := c.leaderConn.Write(ctx.req)
	if err != nil {
		util.GetLogger().Errorf("write %+v failed with %+v",
			ctx.req, err)
	}
}

func (c *asyncClient) resetLeaderConn(newLeader string) error {
	if c.currentLeader == newLeader {
		util.GetLogger().Infof("skip reset leader, current leader is %s", newLeader)
		return nil
	}

	c.leaderConn.Close()
	return c.initLeaderConn(c.leaderConn, newLeader, c.opts.rpcTimeout)
}

func (c *asyncClient) initLeaderConn(conn goetty.IOSession, newLeader string, timeout time.Duration) error {
	addr := newLeader
	for {
		select {
		case <-time.After(timeout):
			return ErrTimeout
		default:
			if addr == "" {
				leader := c.opts.leaderGetter()
				if leader != nil {
					addr = leader.Addr
				}
			}

			if addr != "" {
				util.GetLogger().Infof("start init connection to leader %+v", addr)
				conn.Close()
				ok, err := conn.Connect(addr, c.opts.rpcTimeout)
				if err == nil && ok {
					c.maybeRegisterContainer()
					c.currentLeader = addr
					c.resetReadC <- struct{}{}
					util.GetLogger().Infof("connected to leader %s", addr)
					return nil
				}

				util.GetLogger().Errorf("init leader conn failed with %+v, retry later", err)
			}
		}

		time.Sleep(time.Millisecond * 200)
		// to prevent the given leader from being unreachable,
		// use LeaderGetter for the next retry
		addr = ""
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
			c.cb(resp, err)
		}
	}
}

func (c *ctx) wait() {
	if c.sync {
		<-c.c
	}
}
