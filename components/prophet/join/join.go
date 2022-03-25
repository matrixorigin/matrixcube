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

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/option"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/vfs"
	"go.etcd.io/etcd/client/pkg/v3/types"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

var (
	etcdTimeout = time.Second * 3
	// listMemberRetryTimes is the retry times of list member.
	listMemberRetryTimes = 20
	listMemberInterval   = time.Second * 5
)

const (
	// privateDirMode grants owner to make/remove files inside the directory.
	privateDirMode = 0700
)

// StartEmbedEtcd starts the embedded etcd cluster.
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
func StartEmbedEtcd(ctx context.Context, cfg *config.Config, logger *zap.Logger) (*clientv3.Client, *embed.Etcd, error) {
	logger = log.Adjust(logger)

	// - A Prophet tries to join itself.
	if cfg.Prophet.EmbedEtcd.Join == "" ||
		(cfg.Prophet.EmbedEtcd.InitialCluster != "" &&
			len(strings.Split(cfg.Prophet.EmbedEtcd.InitialCluster, ",")) > 1) {
		return startEmbedEtcd(ctx, cfg, logger)
	}

	if cfg.Prophet.EmbedEtcd.Join == cfg.Prophet.EmbedEtcd.AdvertiseClientUrls {
		logger.Fatal("join self is forbidden")
	}
	fs := cfg.FS
	filePath := fs.PathJoin(cfg.Prophet.DataDir, "join")
	// Read the persist join config
	if _, err := fs.Stat(filePath); !vfs.IsNotExist(err) {
		f, err := fs.Open(filePath)
		if err != nil {
			logger.Fatal("fail to read the join config",
				zap.Error(err))
		}
		defer f.Close()
		s, err := ioutil.ReadAll(f)
		if err != nil {
			logger.Fatal("fail to read the join config",
				zap.Error(err))
		}
		cfg.Prophet.EmbedEtcd.InitialCluster = strings.TrimSpace(string(s))
		cfg.Prophet.EmbedEtcd.InitialClusterState = embed.ClusterStateFlagExisting
		return startEmbedEtcd(ctx, cfg, logger)
	}

	initialCluster := ""
	// Cases with data directory.
	if isDataExist(fs, fs.PathJoin(cfg.Prophet.DataDir, "member"), logger) {
		cfg.Prophet.EmbedEtcd.InitialCluster = initialCluster
		cfg.Prophet.EmbedEtcd.InitialClusterState = embed.ClusterStateFlagExisting
		return startEmbedEtcd(ctx, cfg, logger)
	}

	// Below are cases without data directory.
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(cfg.Prophet.EmbedEtcd.Join, ","),
		DialTimeout: option.DefaultDialTimeout,
		Logger:      logger,
	})
	if err != nil {
		logger.Fatal("create etcd client",
			zap.Error(err))
	}
	defer client.Close()

	for {
		checkMembers(client, cfg, logger)

		var prophets []string
		// - A new Prophet joins an existing cluster.
		// - A deleted Prophet joins to previous cluster.
		{
			for {
				// First adds member through the API
				resp, err := util.AddEtcdMember(client, []string{cfg.Prophet.EmbedEtcd.AdvertisePeerUrls})
				if err != nil {
					logger.Error("fail to add member to embed etcd, retry later",
						zap.Error(err))
					time.Sleep(time.Millisecond * 500)
					continue
				}

				logger.Info("added into embed etcd cluster", zap.Any("resp", resp))

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

		prophets = append(prophets, fmt.Sprintf("%s=%s", cfg.Prophet.Name, cfg.Prophet.EmbedEtcd.AdvertisePeerUrls))
		initialCluster = strings.Join(prophets, ",")
		cfg.Prophet.EmbedEtcd.InitialCluster = initialCluster
		cfg.Prophet.EmbedEtcd.InitialClusterState = embed.ClusterStateFlagExisting

		c, e, err := startEmbedEtcd(ctx, cfg, logger)
		if err != nil && strings.Contains(err.Error(), "member count is unequal") {
			logger.Error("failed to start etcd", zap.Error(err))
			continue
		}
		if err != nil {
			return c, e, err
		}

		err = fs.MkdirAll(cfg.Prophet.DataDir, privateDirMode)
		if err != nil && !vfs.IsExist(err) {
			logger.Fatal("fail to create data path",
				zap.Error(err))
		}

		f, err := fs.Create(filePath)
		if err != nil {
			logger.Fatal("fail to write data path",
				zap.Error(err))
		}
		defer f.Close()
		_, err = f.Write([]byte(cfg.Prophet.EmbedEtcd.InitialCluster))
		if err != nil {
			logger.Fatal("fail to write data path",
				zap.Error(err))
		}

		return c, e, nil
	}
}

func checkMembers(client *clientv3.Client, cfg *config.Config, logger *zap.Logger) {
OUTER:
	for {
		listResp, err := util.ListEtcdMembers(client)
		if err != nil {
			logger.Error("fail to list embed etcd members, retry later",
				zap.Error(err))
			time.Sleep(time.Second)
			continue
		}

		for _, m := range listResp.Members {
			if len(m.Name) == 0 {
				// A new member added, but not started
				logger.Warn("there is a member that has not joined successfully")
				time.Sleep(time.Second)
				continue OUTER
			}
			// - A failed Prophet re-joins the previous cluster.
			if m.Name == cfg.Prophet.Name {
				logger.Fatal("missing data or join a duplicated prophet")
			}
		}

		return
	}
}

func isDataExist(fs vfs.FS, d string, logger *zap.Logger) bool {
	names, err := fs.List(d)
	if vfs.IsNotExist(err) {
		return false
	}

	if err != nil {
		logger.Error("fail to open directory",
			zap.String("dir", d),
			zap.Error(err))
		return false
	}

	return len(names) != 0
}

func startEmbedEtcd(ctx context.Context, cfg *config.Config, logger *zap.Logger) (*clientv3.Client, *embed.Etcd, error) {
	etcdCfg, err := cfg.Prophet.GenEmbedEtcdConfig(logger)
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
	urlMap, err := types.NewURLsMap(cfg.Prophet.EmbedEtcd.InitialCluster)
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

	var endpoints []string
	for _, u := range etcdCfg.ACUrls {
		endpoints = append(endpoints, u.String())
	}
	logger.Info("start to create etcd v3 client",
		zap.Strings("endpoints", endpoints))

	client, err := clientv3.New(clientv3.Config{
		Endpoints:        endpoints,
		AutoSyncInterval: time.Second * 5,
		DialTimeout:      etcdTimeout,
		Logger:           logger,
	})
	if err != nil {
		return nil, nil, err
	}

	for i := 0; i < listMemberRetryTimes; i++ {
		etcdServerID := uint64(etcd.Server.ID())
		etcdMembers, err := util.ListEtcdMembers(client)
		if err == nil {
			for _, m := range etcdMembers.Members {
				if etcdServerID == m.ID && m.Name == cfg.Prophet.Name {
					return client, etcd, nil
				}
			}

			logger.Error("failed to check members, current node not found")
		} else {
			logger.Error("failed to list embed etcd member, retry later",
				zap.Error(err))
		}

		time.Sleep(listMemberInterval)
	}

	logger.Fatal("start etcd server timeout")
	return nil, nil, nil
}
