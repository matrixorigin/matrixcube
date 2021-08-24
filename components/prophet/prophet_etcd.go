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
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/option"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"go.etcd.io/etcd/client/pkg/v3/types"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

var (
	etcdTimeout = time.Second * 3
	// listMemberRetryTimes is the retry times of list member.
	listMemberRetryTimes = 20
)

func startEmbedEtcd(ctx context.Context, cfg *config.Config) (*clientv3.Client, *embed.Etcd, error) {
	etcdCfg, err := cfg.GenEmbedEtcdConfig()
	if err != nil {
		return nil, nil, err
	}

	newCtx, cancel := context.WithTimeout(ctx, option.EtcdStartTimeout)
	defer cancel()

	etcd, err := embed.StartEtcd(etcdCfg)
	if err != nil {
		return nil, nil, err
	}

	// Check cluster ID
	urlMap, err := types.NewURLsMap(cfg.EmbedEtcd.InitialCluster)
	if err != nil {
		return nil, nil, err
	}

	if err = util.CheckClusterID(etcd.Server.Cluster().ID(), urlMap); err != nil {
		return nil, nil, err
	}

	select {
	// Wait etcd until it is ready to use
	case <-etcd.Server.ReadyNotify():
	case <-newCtx.Done():
		return nil, nil, errors.New("context cancaled")
	}

	endpoints := []string{etcdCfg.ACUrls[0].String()}
	util.GetLogger().Infof("create etcd v3 client with endpoints %+v", endpoints)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:        endpoints,
		AutoSyncInterval: time.Second * 30,
		DialTimeout:      etcdTimeout,
	})
	if err != nil {
		return nil, nil, err
	}

	for i := 0; i < listMemberRetryTimes; i++ {
		etcdServerID := uint64(etcd.Server.ID())
		etcdMembers, err := util.ListEtcdMembers(client)
		if err != nil {
			return nil, nil, err
		}
		for _, m := range etcdMembers.Members {
			if etcdServerID == m.ID && m.Name == cfg.Name {
				return client, etcd, nil
			}
		}
		time.Sleep(time.Second * 1)
	}

	util.GetLogger().Fatalf("start etcd server timeout")
	return nil, nil, nil
}
