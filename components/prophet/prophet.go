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
	"math/rand"
	"sync"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/cluster"
	pconfig "github.com/matrixorigin/matrixcube/components/prophet/config"
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
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/util/task"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

var (
	rootPath      = "/prophet"
	clusterIDPath = "/prophet/cluster_id"

	initClusterMaxRetryTimes = 10
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
	GetConfig() *pconfig.Config
	// GetClusterID return cluster id
	GetClusterID() uint64
	// GetBasicCluster returns basic cluster
	GetBasicCluster() *core.BasicCluster
}

type defaultProphet struct {
	logger         *zap.Logger
	ctx            context.Context
	cancel         context.CancelFunc
	cfg            *config.Config
	persistOptions *pconfig.PersistOptions
	runner         *task.Runner
	stopOnce       sync.Once

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

	// job task ctx
	jobMu struct {
		sync.RWMutex
		jobs map[metapb.JobType]metapb.Job
	}

	mu struct {
		sync.RWMutex
		wn *watcherNotifier
	}
}

// NewProphet returns a prophet instance
func NewProphet(cfg *config.Config) Prophet {
	logger := log.Adjust(cfg.Logger).Named("prophet").With(log.NodeField(cfg.Prophet.Name))
	var elector election.Elector
	var etcdClient *clientv3.Client
	var etcd *embed.Etcd
	var err error
	ctx, cancel := context.WithCancel(context.Background())

	if cfg.Prophet.StorageNode {
		etcdClient, etcd, err = join.PrepareJoinCluster(ctx, cfg, logger)
		if err != nil {
			logger.Fatal("fail to start embed etcd", zap.Error(err))
		}
	} else {
		etcdClient, err = clientv3.New(clientv3.Config{
			Endpoints:        cfg.Prophet.ExternalEtcd,
			AutoSyncInterval: time.Second * 30,
			DialTimeout:      time.Second * 10,
		})
		if err != nil {
			logger.Fatal("fail to create external etcd client",
				zap.Error(err))
		}
	}

	elector, err = election.NewElector(etcdClient,
		election.WithLeaderLeaseSeconds(cfg.Prophet.LeaderLease),
		election.WithEmbedEtcd(etcd))
	if err != nil {
		logger.Fatal("fail to create elector", zap.Error(err))
	}

	p := &defaultProphet{}
	p.logger = logger
	p.cfg = cfg
	p.persistOptions = pconfig.NewPersistOptions(&cfg.Prophet, logger)
	p.ctx = ctx
	p.cancel = cancel
	p.elector = elector
	p.etcd = etcd
	p.member = member.NewMember(etcdClient, etcd, elector, cfg.Prophet.StorageNode, p.enableLeader, p.disableLeader, logger)
	p.runner = task.NewRunner()
	p.completeC = make(chan struct{})
	p.jobMu.jobs = make(map[metapb.JobType]metapb.Job)
	return p
}

func (p *defaultProphet) Start() {
	var err error
	for i := 0; i < initClusterMaxRetryTimes; i++ {
		if err = p.initClusterID(); err == nil {
			break
		}
	}
	if err != nil {
		p.logger.Fatal("fail to init cluster",
			zap.Error(err))
	}

	p.member.MemberInfo(p.cfg.Prophet.Name, p.cfg.Prophet.AdvertiseRPCAddr)
	p.storage = storage.NewStorage(rootPath,
		storage.NewEtcdKV(rootPath, p.elector.Client(), p.member.GetLeadership()),
		p.cfg.Prophet.Adapter)
	p.basicCluster = core.NewBasicCluster(p.cfg.Prophet.Adapter.NewResource, p.logger)
	p.cluster = cluster.NewRaftCluster(p.ctx, rootPath, p.clusterID, p.elector.Client(), p.cfg.Prophet.Adapter,
		p.cfg.Prophet.ResourceStateChangedHandler, p.logger)
	p.hbStreams = hbstream.NewHeartbeatStreams(p.ctx, p.clusterID, p.cluster, p.logger)

	p.startSystemMonitor()
	p.startListen()
	p.startLeaderLoop()
}

func (p *defaultProphet) Stop() {
	p.stopOnce.Do(func() {
		if p.client != nil {
			p.client.Close()
		}
		p.trans.Stop()
		p.runner.Stop()
		p.member.Stop()
		p.cancel()
		p.elector.Client().Close()
		if p.etcd != nil {
			p.etcd.Close()
		}
	})
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

func (p *defaultProphet) GetConfig() *pconfig.Config {
	return &p.cfg.Prophet
}

func (p *defaultProphet) GetPersistOptions() *pconfig.PersistOptions {
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

func (p *defaultProphet) startSystemMonitor() {
	go StartMonitor(p.ctx, time.Now, func() {
		p.logger.Error("system time jumps backward")
	}, p.logger)
}
