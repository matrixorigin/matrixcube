// Copyright 2020 PingCAP, Inc.
// Modifications copyright (C) 2021 MatrixOrigin.
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

package join

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/option"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/vfs"
	"go.etcd.io/etcd/client/pkg/v3/types"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

var (
	etcdTimeout = time.Second * 3
	// listMemberRetryTimes is the retry times of list member.
	listMemberRetryTimes = 20
)

const (
	// privateDirMode grants owner to make/remove files inside the directory.
	privateDirMode = 0700
)

// PrepareJoinCluster sends MemberAdd command to Prophet cluster,
// and returns the initial configuration of the Prophet cluster.
//
// TL;TR: The join functionality is safe. With data, join does nothing, w/o data
//        and it is not a member of cluster, join does MemberAdd, it returns an
//        error if Prophet tries to join itself, missing data or join a duplicated Prophet.
//
// Etcd automatically re-joins the cluster if there is a data directory. So
// first it checks if there is a data directory or not. If there is, it returns
// an empty string (etcd will get the correct configurations from the data
// directory.)
//
// If there is no data directory, there are following cases:
//
//  - A new Prophet joins an existing cluster.
//      What join does: MemberAdd, MemberList, then generate initial-cluster.
//
//  - A failed Prophet re-joins the previous cluster.
//      What join does: return an error. (etcd reports: raft log corrupted,
//                      truncated, or lost?)
//
//  - A deleted Prophet joins to previous cluster.
//      What join does: MemberAdd, MemberList, then generate initial-cluster.
//                      (it is not in the member list and there is no data, so
//                       we can treat it as a new Prophet.)
//
// If there is a data directory, there are following special cases:
//
//  - A failed Prophet tries to join the previous cluster but it has been deleted
//    during its downtime.
//      What join does: return "" (etcd will connect to other peers and find
//                      that the Prophet itself has been removed.)
//
//  - A deleted Prophet joins the previous cluster.
//      What join does: return "" (as etcd will read data directory and find
//                      that the Prophet itself has been removed, so an empty string
//                      is fine.)
func PrepareJoinCluster(ctx context.Context, cfg *config.Config) (*clientv3.Client, *embed.Etcd, error) {
	// - A Prophet tries to join itself.
	if cfg.EmbedEtcd.Join == "" {
		return startEmbedEtcd(ctx, cfg)
	}

	if cfg.EmbedEtcd.Join == cfg.EmbedEtcd.AdvertiseClientUrls {
		util.GetLogger().Fatalf("join self is forbidden")
	}
	fs := cfg.FS
	filePath := fs.PathJoin(cfg.DataDir, "join")
	// Read the persist join config
	if _, err := fs.Stat(filePath); !vfs.IsNotExist(err) {
		f, err := fs.Open(filePath)
		if err != nil {
			util.GetLogger().Fatalf("read the join config failed with %+v",
				err)
		}
		defer f.Close()
		s, err := ioutil.ReadAll(f)
		if err != nil {
			util.GetLogger().Fatalf("read the join config failed with %+v",
				err)
		}
		cfg.EmbedEtcd.InitialCluster = strings.TrimSpace(string(s))
		cfg.EmbedEtcd.InitialClusterState = embed.ClusterStateFlagExisting
		return startEmbedEtcd(ctx, cfg)
	}

	initialCluster := ""
	// Cases with data directory.
	if isDataExist(fs, fs.PathJoin(cfg.DataDir, "member")) {
		cfg.EmbedEtcd.InitialCluster = initialCluster
		cfg.EmbedEtcd.InitialClusterState = embed.ClusterStateFlagExisting
		return startEmbedEtcd(ctx, cfg)
	}

	// Below are cases without data directory.
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(cfg.EmbedEtcd.Join, ","),
		DialTimeout: option.DefaultDialTimeout,
	})
	if err != nil {
		util.GetLogger().Fatalf("create etcd client failed with %+v",
			err)
	}
	defer client.Close()

	for {
		util.GetLogger().Infof("%s(%s) begin to check etcd members",
			cfg.Name,
			cfg.DataDir)
		checkMembers(client, cfg)
		util.GetLogger().Infof("%s(%s) end to check etcd members",
			cfg.Name,
			cfg.DataDir)

		var prophets []string
		// - A new Prophet joins an existing cluster.
		// - A deleted Prophet joins to previous cluster.
		{
			for {
				// First adds member through the API
				resp, err := util.AddEtcdMember(client, []string{cfg.EmbedEtcd.AdvertisePeerUrls})
				if err != nil {
					util.GetLogger().Errorf("%s(%s) add member to embed etcd failed with %+v, retry later",
						cfg.Name,
						cfg.DataDir,
						err)
					time.Sleep(time.Millisecond * 500)
					continue
				}

				util.GetLogger().Infof("%s(%s) added into embed etcd cluster with resp %+v",
					cfg.Name,
					cfg.DataDir,
					resp)

				for _, m := range resp.Members {
					if m.Name != "" {
						for _, u := range m.PeerURLs {
							prophets = append(prophets, fmt.Sprintf("%s=%s", m.Name, u))
						}
					}
				}
				break
			}
		}

		prophets = append(prophets, fmt.Sprintf("%s=%s", cfg.Name, cfg.EmbedEtcd.AdvertisePeerUrls))
		initialCluster = strings.Join(prophets, ",")
		cfg.EmbedEtcd.InitialCluster = initialCluster
		cfg.EmbedEtcd.InitialClusterState = embed.ClusterStateFlagExisting

		c, e, err := startEmbedEtcd(ctx, cfg)
		if err != nil && strings.Contains(err.Error(), "member count is unequal") {
			continue
		}
		if err != nil {
			return c, e, err
		}

		err = fs.MkdirAll(cfg.DataDir, privateDirMode)
		if err != nil && !vfs.IsExist(err) {
			util.GetLogger().Fatalf("create data path failed with %+v",
				err)
		}

		f, err := fs.Create(filePath)
		if err != nil {
			util.GetLogger().Fatalf("write data path failed with %+v",
				err)
		}
		defer f.Close()
		_, err = f.Write([]byte(cfg.EmbedEtcd.InitialCluster))
		if err != nil {
			util.GetLogger().Fatalf("write data path failed with %+v",
				err)
		}

		return c, e, nil
	}
}

func checkMembers(client *clientv3.Client, cfg *config.Config) {
OUTER:
	for {
		listResp, err := util.ListEtcdMembers(client)
		if err != nil {
			util.GetLogger().Errorf("list embed etcd members failed with %+v, retry later",
				err)
			time.Sleep(time.Second)
			continue
		}

		for _, m := range listResp.Members {
			if len(m.Name) == 0 {
				// A new member added, but not started
				util.GetLogger().Warningf("there is a member that has not joined successfully")
				time.Sleep(time.Second)
				continue OUTER
			}
			// - A failed Prophet re-joins the previous cluster.
			if m.Name == cfg.Name {
				util.GetLogger().Fatalf("missing data or join a duplicated prophet")
			}
		}

		return
	}
}

func isDataExist(fs vfs.FS, d string) bool {
	names, err := fs.List(d)
	if vfs.IsNotExist(err) {
		return false
	}

	if err != nil {
		util.GetLogger().Errorf("open directory %s failed with %+v", d, err)
		return false
	}

	return len(names) != 0
}

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
