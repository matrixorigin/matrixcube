package prophet

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/task"
	"github.com/matrixorigin/matrixcube/components/prophet/cluster"
	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/election"
	"github.com/matrixorigin/matrixcube/components/prophet/join"
	"github.com/matrixorigin/matrixcube/components/prophet/member"
	"github.com/matrixorigin/matrixcube/components/prophet/option"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/hbstream"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

var (
	rootPath      = "/prophet"
	clusterIDPath = "/prophet/cluster_id"
)

// Prophet is the distributed scheduler and coordinator
type Prophet interface {
	// Start start the prophet instance, this will start the lead election, heartbeat loop and listen requests
	Start()
	// Stop stop the prophet instance
	Stop()
	// GetStorage returns the storage
	GetStorage() storage.Storage
	// GetClient returns the prophet client
	GetClient() Client
	// GetLeader returns leader
	GetLeader() *metapb.Member
	// GetMember returns self
	GetMember() *member.Member
	// GetConfig returns cfg
	GetConfig() *config.Config
	// GetClusterID return cluster id
	GetClusterID() uint64
}

type defaultProphet struct {
	sync.Mutex

	ctx            context.Context
	cancel         context.CancelFunc
	cfg            *config.Config
	persistOptions *config.PersistOptions
	runner         *task.Runner
	wn             *watcherNotifier

	// about leader election
	etcd       *embed.Etcd
	elector    election.Elector
	member     *member.Member
	notifyOnce sync.Once
	completeC  chan struct{}

	// cluster
	clusterID    uint64
	storage      storage.Storage
	basicCluster *core.BasicCluster
	cluster      *cluster.RaftCluster

	// rpc
	hbStreams  *hbstream.HeartbeatStreams
	trans      goetty.NetApplication
	client     Client
	clientOnce sync.Once
}

// NewProphet returns a prophet instance
func NewProphet(cfg *config.Config) Prophet {
	var elector election.Elector
	var etcdClient *clientv3.Client
	var etcd *embed.Etcd
	var err error
	ctx, cancel := context.WithCancel(context.Background())

	if cfg.StorageNode {
		join.PrepareJoinCluster(cfg)
		etcdClient, etcd, err = startEmbedEtcd(ctx, cfg)
		if err != nil {
			util.GetLogger().Fatalf("start embed etcd failed with %+v", err)
		}
	} else {
		etcdClient, err = clientv3.New(clientv3.Config{
			Endpoints:        cfg.ExternalEtcd,
			AutoSyncInterval: time.Second * 30,
			DialTimeout:      etcdTimeout,
		})
		if err != nil {
			util.GetLogger().Fatalf("create external etcd client failed with %+v", err)
		}
	}

	elector, err = election.NewElector(etcdClient,
		election.WithLeaderLeaseSeconds(cfg.LeaderLease),
		election.WithEmbedEtcd(etcd))
	if err != nil {
		util.GetLogger().Fatalf("create elector failed with %+v", err)
	}

	p := &defaultProphet{}
	p.cfg = cfg
	p.persistOptions = config.NewPersistOptions(cfg)
	p.ctx = ctx
	p.cancel = cancel
	p.elector = elector
	p.etcd = etcd
	p.member = member.NewMember(etcdClient, etcd, elector, cfg.StorageNode, p.enableLeader, p.disableLeader)
	p.runner = task.NewRunner()
	p.completeC = make(chan struct{})
	return p
}

func (p *defaultProphet) Start() {
	if err := p.initClusterID(); err != nil {
		util.GetLogger().Fatalf("init cluster failed with %+v", err)
	}

	p.member.MemberInfo(p.cfg.Name, p.cfg.RPCAddr)
	p.storage = storage.NewStorage(rootPath,
		storage.NewEtcdKV(rootPath, p.elector.Client(), p.member.GetLeadership()),
		p.cfg.Adapter)
	p.basicCluster = core.NewBasicCluster(p.cfg.Adapter.NewResource)
	p.cluster = cluster.NewRaftCluster(p.ctx, rootPath, p.clusterID, p.elector.Client(), p.cfg.Adapter, p.cfg.ResourceStateChangedHandler)
	p.hbStreams = hbstream.NewHeartbeatStreams(p.ctx, p.clusterID, p.cluster)

	p.startSystemMonitor(context.Background())
	p.startListen()
	p.startLeaderLoop()
}

func (p *defaultProphet) Stop() {
	p.trans.Stop()
	p.runner.Stop()
	p.member.Stop()
	p.cancel()
	p.elector.Client().Close()
	if p.etcd != nil {
		p.etcd.Close()
	}
}

func (p *defaultProphet) GetStorage() storage.Storage {
	return p.storage
}

func (p *defaultProphet) GetClient() Client {
	return p.client
}

func (p *defaultProphet) GetLeader() *metapb.Member {
	return p.member.GetLeader()
}

func (p *defaultProphet) GetMember() *member.Member {
	return p.member
}

func (p *defaultProphet) GetClusterID() uint64 {
	return p.clusterID
}

func (p *defaultProphet) initClusterID() error {
	// Get any cluster key to parse the cluster ID.
	resp, err := util.GetEtcdResp(p.elector.Client(), clusterIDPath)
	if err != nil {
		return err
	}

	// If no key exist, generate a random cluster ID.
	if len(resp.Kvs) == 0 {
		p.clusterID, err = initOrGetClusterID(p.elector.Client(), clusterIDPath)
		return err
	}
	p.clusterID, err = typeutil.BytesToUint64(resp.Kvs[0].Value)
	return err
}

func initOrGetClusterID(c *clientv3.Client, key string) (uint64, error) {
	ctx, cancel := context.WithTimeout(c.Ctx(), option.DefaultRequestTimeout)
	defer cancel()

	// Generate a random cluster ID.
	ts := uint64(time.Now().Unix())
	clusterID := (ts << 32) + uint64(rand.Uint32())
	value := typeutil.Uint64ToBytes(clusterID)

	// Multiple Prophets may try to init the cluster ID at the same time.
	// Only one Prophet can commit this transaction, then other Prophets can get
	// the committed cluster ID.
	resp, err := c.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(value))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return 0, err
	}

	// Txn commits ok, return the generated cluster ID.
	if resp.Succeeded {
		return clusterID, nil
	}

	// Otherwise, parse the committed cluster ID.
	if len(resp.Responses) == 0 {
		return 0, errors.New("etcd Txn failed")
	}

	response := resp.Responses[0].GetResponseRange()
	if response == nil || len(response.Kvs) != 1 {
		return 0, errors.New("etcd Txn failed")
	}

	return typeutil.BytesToUint64(response.Kvs[0].Value)
}

// imple raft cluster server interface methods

func (p *defaultProphet) GetConfig() *config.Config {
	return p.cfg
}

func (p *defaultProphet) GetPersistOptions() *config.PersistOptions {
	return p.persistOptions
}

func (p *defaultProphet) GetHBStreams() *hbstream.HeartbeatStreams {
	return p.hbStreams
}

func (p *defaultProphet) GetRaftCluster() *cluster.RaftCluster {
	if p.cluster == nil || !p.cluster.IsRunning() {
		return nil
	}
	return p.cluster
}

func (p *defaultProphet) GetBasicCluster() *core.BasicCluster {
	return p.basicCluster
}

func (p *defaultProphet) startSystemMonitor(ctx context.Context) {
	go StartMonitor(ctx, time.Now, func() {
		util.GetLogger().Error("system time jumps backward")
	})
}
