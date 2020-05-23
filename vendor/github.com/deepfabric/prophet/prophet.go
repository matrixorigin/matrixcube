package prophet

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/fagongzi/goetty"
)

// RoleChangeHandler prophet role change handler
type RoleChangeHandler interface {
	ProphetBecomeLeader()
	ProphetBecomeFollower()
}

// Adapter prophet adapter
type Adapter interface {
	// NewResource return a new resource
	NewResource() Resource
	// NewContainer return a new container
	NewContainer() Container
	// FetchLeaderResources fetch loacle leader resource
	FetchLeaderResources() []uint64
	// FetchResourceHB fetch resource HB
	FetchResourceHB(id uint64) *ResourceHeartbeatReq
	// FetchContainerHB fetch container HB
	FetchContainerHB() *ContainerHeartbeatReq
	// ResourceHBInterval fetch resource HB interface
	ResourceHBInterval() time.Duration
	// ContainerHBInterval fetch container HB interface
	ContainerHBInterval() time.Duration
	// HBHandler HB hander
	HBHandler() HeartbeatHandler
}

// Prophet is the distributed scheduler and coordinator
type Prophet interface {
	// Start start the prophet instance, this will start the lead election, heartbeat loop and listen requests
	Start()
	// Stop stop the prophet instance
	Stop()
	// GetStore returns the Store
	GetStore() Store
	// GetRPC returns the RPC client
	GetRPC() RPC
	// GetEtcdClient returns the internal etcd instance
	GetEtcdClient() *clientv3.Client
	// StorageNode returns true if the current node is storage node
	StorageNode() bool
}

type defaultProphet struct {
	sync.Mutex
	adapter     Adapter
	opts        *options
	cfg         *Cfg
	store       Store
	rt          *Runtime
	coordinator *Coordinator

	tcpL      *goetty.Server
	runner    *Runner
	completeC chan struct{}
	rpc       *simpleRPC
	bizCodec  *codec

	wn          *watcherNotifier
	resourceHBC chan uint64

	// about leader election
	node              *Node
	elector           Elector
	electorCancelFunc context.CancelFunc
	leader            *Node
	leaderFlag        int64
	signature         string
	notifyOnce        sync.Once
}

// NewProphet returns a prophet instance
func NewProphet(name string, adapter Adapter, opts ...Option) Prophet {
	value := &options{cfg: &Cfg{}}
	for _, opt := range opts {
		opt(value)
	}
	value.adjust()

	p := new(defaultProphet)
	p.opts = value
	p.cfg = value.cfg
	p.adapter = adapter
	p.bizCodec = &codec{adapter: adapter}
	p.leaderFlag = 0
	p.node = &Node{
		Name: name,
		Addr: p.cfg.RPCAddr,
	}
	p.signature = p.node.marshal()
	p.elector, _ = NewElector(p.opts.client, WithLeaderLeaseSeconds(p.opts.cfg.LeaseTTL))
	p.store = newEtcdStore(value.client, adapter, p.signature, p.elector)
	p.runner = NewRunner()
	p.coordinator = newCoordinator(value.cfg, p.runner, p.rt)
	p.tcpL = goetty.NewServer(p.cfg.RPCAddr,
		goetty.WithServerDecoder(goetty.NewIntLengthFieldBasedDecoder(p.bizCodec)),
		goetty.WithServerEncoder(goetty.NewIntLengthFieldBasedEncoder(p.bizCodec)))
	p.completeC = make(chan struct{})
	p.rpc = newSimpleRPC(p)
	p.resourceHBC = make(chan uint64, 512)

	return p
}

func (p *defaultProphet) Start() {
	p.startListen()
	p.startLeaderLoop()
	p.startResourceHeartbeatLoop()
	p.startContainerHeartbeatLoop()
}

func (p *defaultProphet) Stop() {
	p.tcpL.Stop()
	p.runner.Stop()
	p.electorCancelFunc()
	p.elector.Stop(math.MaxUint64)
	p.opts.client.Close()
	if p.opts.etcd != nil {
		p.opts.etcd.Close()
	}
}

func (p *defaultProphet) GetStore() Store {
	return p.store
}

func (p *defaultProphet) GetRPC() RPC {
	return p.rpc
}

func (p *defaultProphet) GetEtcdClient() *clientv3.Client {
	return p.opts.client
}

func (p *defaultProphet) StorageNode() bool {
	return p.opts.cfg.StorageNode
}
