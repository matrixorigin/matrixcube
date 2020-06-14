package proxy

import (
	"encoding/hex"
	"errors"
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

var (
	// ErrTimeout timeout error
	ErrTimeout = errors.New("Exec timeout")
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
	Router() raftstore.Router
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
	shard, to := p.router.SelectShard(req.Group, req.Key)
	return p.DispatchTo(req, shard, to)
}

func (p *shardsProxy) DispatchTo(req *raftcmdpb.Request, shard uint64, to string) error {
	// No leader, retry after a leader tick
	if to == "" {
		if logger.DebugEnabled() {
			logger.Debugf("%s retry with no leader, shard %d, group %d",
				hex.EncodeToString(req.ID),
				shard,
				req.Group)
		}

		p.retryWithRaftError(req, "dispath to nil store", RetryInterval)
		return nil
	}

	return p.forwardToBackend(req, to)
}

func (p *shardsProxy) Router() raftstore.Router {
	return p.router
}

func (p *shardsProxy) forwardToBackend(req *raftcmdpb.Request, leader string) error {
	if p.store != nil && p.local.RPCAddr == leader {
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
	if rsp.Type != raftcmdpb.RaftError && !rsp.Stale {
		p.doneCB(rsp)
		return
	}

	p.retryWithRaftError(rsp.OriginRequest, rsp.Error.String(), RetryInterval)
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
		to = p.router.RandomPeerAddress(req.ToShard)
	} else {
		to = p.router.LeaderAddress(req.ToShard)
	}

	p.DispatchTo(&req, req.ToShard, to)
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
