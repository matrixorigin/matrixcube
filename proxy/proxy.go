package proxy

import (
	"sync"
	"time"

	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/beehive/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
)

var (
	logger           = log.NewLoggerWithPrefix("[beehive-proxy]")
	decoder, encoder = raftstore.CreateRPCCliendSideCodec()
)

type doneFunc func(*raftcmdpb.Response)
type errorDoneFunc func(*raftcmdpb.Request, error)

// ShardsProxy Shards proxy, distribute the appropriate request to the corresponding backend,
// retry the request for the error
type ShardsProxy interface {
	Dispatch(req *raftcmdpb.Request) error
}

// NewShardsProxy returns a shard proxy
func NewShardsProxy(router raftstore.Router,
	doneCB doneFunc,
	errorDoneCB errorDoneFunc) ShardsProxy {
	return &shardsProxy{
		router:      router,
		doneCB:      doneCB,
		errorDoneCB: errorDoneCB,
	}
}

// NewShardsProxyWithStore returns a shard proxy with a raftstore
func NewShardsProxyWithStore(store raftstore.Store,
	doneCB doneFunc,
	errorDoneCB errorDoneFunc,
) (ShardsProxy, error) {
	router := store.NewRouter()
	err := router.Start()
	if err != nil {
		return nil, err
	}

	sp := &shardsProxy{
		store:       store,
		local:       store.Meta(),
		router:      router,
		doneCB:      doneCB,
		errorDoneCB: errorDoneCB,
	}

	sp.store.RegisterLocalRequestCB(sp.onLocalResp)
	return sp, nil
}

type shardsProxy struct {
	local       metapb.Store
	store       raftstore.Store
	router      raftstore.Router
	doneCB      doneFunc
	errorDoneCB errorDoneFunc
	backends    sync.Map // store addr -> *backend
}

func (p *shardsProxy) Dispatch(req *raftcmdpb.Request) error {
	shard, leader := p.router.SelectShard(req.Group, req.Key)
	// No leader, retry after a leader tick
	if leader == "" {
		p.retryWithRaftError(req)
		return nil
	}

	return p.forwardToBackend(req, shard, leader)
}

func (p *shardsProxy) forwardToBackend(req *raftcmdpb.Request, shard uint64, leader string) error {
	if p.store != nil && p.local.RPCAddr == leader {
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
			rsp.Type = raftcmdpb.RaftError
		} else {
			rsp.Type = raftcmdpb.Invalid
		}

		rsp.Error = header.Error
	}

	p.done(rsp)
	pb.ReleaseResponse(rsp)
}

func (p *shardsProxy) done(rsp *raftcmdpb.Response) {
	if rsp.Type != raftcmdpb.RaftError {
		p.doneCB(rsp)
		return
	}

	p.retryWithRaftError(rsp.OriginRequest)
}

func (p *shardsProxy) errorDone(req *raftcmdpb.Request, err error) {
	p.errorDoneCB(req, err)
}

func (p *shardsProxy) retryWithRaftError(req *raftcmdpb.Request) {
	if req != nil {
		util.DefaultTimeoutWheel().Schedule(time.Millisecond*50, p.doRetry, *req)
	}
}

func (p *shardsProxy) doRetry(arg interface{}) {
	req := arg.(raftcmdpb.Request)
	p.Dispatch(&req)
}

func (p *shardsProxy) getConn(addr string) (*backend, error) {
	bc := p.getConnLocked(addr)
	if p.checkConnect(addr, bc) {
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
	bc := newBackend(p, addr, goetty.NewConnector(addr,
		goetty.WithClientConnectTimeout(defaultConnectTimeout),
		goetty.WithClientDecoder(decoder),
		goetty.WithClientEncoder(encoder)))

	old, loaded := p.backends.LoadOrStore(addr, bc)
	if loaded {
		return old.(*backend)
	}

	return bc
}

func (p *shardsProxy) checkConnect(addr string, bc *backend) bool {
	if nil == bc {
		return false
	}

	if bc.conn.IsConnected() {
		return true
	}

	bc.Lock()
	defer bc.Unlock()

	if bc.conn.IsConnected() {
		return true
	}

	ok, err := bc.conn.Connect()
	if err != nil {
		logger.Errorf("connect to backend %s failed with %+v",
			addr,
			err)
		return false
	}

	bc.readLoop()
	return ok
}
