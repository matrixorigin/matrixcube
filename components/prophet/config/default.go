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

package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/vfs"
	"go.etcd.io/etcd/embed"
)

const (
	defaultMaxReplicas                 = 3
	defaultMaxSnapshotCount            = 3
	defaultMaxPendingPeerCount         = 16
	defaultMaxMergeResourceSize        = 20
	defaultMaxMergeResourceKeys        = 200000
	defaultSplitMergeInterval          = 1 * time.Hour
	defaultPatrolResourceInterval      = 100 * time.Millisecond
	defaultMaxContainerDownTime        = 30 * time.Minute
	defaultLeaderScheduleLimit         = 4
	defaultResourceScheduleLimit       = 2048
	defaultReplicaScheduleLimit        = 64
	defaultMergeScheduleLimit          = 8
	defaultHotResourceScheduleLimit    = 4
	defaultTolerantSizeRatio           = 0
	defaultLowSpaceRatio               = 0.8
	defaultHighSpaceRatio              = 0.7
	defaultResourceScoreFormulaVersion = "v2"
	// defaultHotResourceCacheHitsThreshold is the low hit number threshold of the
	// hot resource.
	defaultHotResourceCacheHitsThreshold = 3
	defaultSchedulerMaxWaitingOperator   = 5
	defaultLeaderSchedulePolicy          = "count"
	defaultContainerLimitMode            = "manual"
	defaultEnableJointConsensus          = false
	defaultEnableCrossTableMerge         = true
)

var (
	defaultLocationLabels = []string{}
	// DefaultContainerLimit is the default container limit of add peer and remove peer.
	DefaultContainerLimit = ContainerLimit{AddPeer: 15, RemovePeer: 15}
)

const (
	defaultLeaderLease             = int64(3)
	defaultNextRetryDelay          = time.Second
	defaultCompactionMode          = "periodic"
	defaultAutoCompactionRetention = "1h"
	defaultQuotaBackendBytes       = typeutil.ByteSize(8 * 1024 * 1024 * 1024) // 8GB

	defaultName                = "prophet"
	defaultRPCAddr             = "127.0.0.1:10001"
	defaultRPCTimeout          = time.Second * 10
	defaultClientUrls          = "http://127.0.0.1:2379"
	defaultPeerUrls            = "http://127.0.0.1:2380"
	defaultInitialClusterState = embed.ClusterStateFlagNew
	defaultInitialClusterToken = "prophet-cluster"

	// etcd use 100ms for heartbeat and 1s for election timeout.
	// We can enlarge both a little to reduce the network aggression.
	// now embed etcd use TickMs for heartbeat, we will update
	// after embed etcd decouples tick and heartbeat.
	defaultTickInterval = 500 * time.Millisecond
	// embed etcd has a check that `5 * tick > election`
	defaultElectionInterval = 3000 * time.Millisecond

	defaultStrictlyMatchLabel   = false
	defaultEnablePlacementRules = true
	defaultEnableGRPCGateway    = true
	defaultDisableErrorVerbose  = true
)

// Adjust adjust configuration
func (c *Config) Adjust(meta *toml.MetaData, reloading bool) error {
	configMetaData := newConfigMetadata(meta)
	if err := configMetaData.CheckUndecoded(); err != nil {
		return err
	}

	if c.Name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return err
		}
		adjustString(&c.Name, fmt.Sprintf("%s-%s", defaultName, hostname))
	}
	adjustString(&c.DataDir, fmt.Sprintf("/tmp/prophet/default.%s", c.Name))
	adjustPath(&c.DataDir)
	adjustString(&c.RPCAddr, defaultRPCAddr)
	adjustDuration(&c.RPCTimeout, defaultRPCTimeout)

	if err := c.Validate(); err != nil {
		return err
	}

	if c.StorageNode {
		adjustString(&c.EmbedEtcd.ClientUrls, defaultClientUrls)
		adjustString(&c.EmbedEtcd.AdvertiseClientUrls, c.EmbedEtcd.ClientUrls)
		adjustString(&c.EmbedEtcd.PeerUrls, defaultPeerUrls)
		adjustString(&c.EmbedEtcd.AdvertisePeerUrls, c.EmbedEtcd.PeerUrls)

		if len(c.EmbedEtcd.InitialCluster) == 0 {
			// The advertise peer urls may be http://127.0.0.1:2380,http://127.0.0.1:2381
			// so the initial cluster is prophet=http://127.0.0.1:2380,prophet=http://127.0.0.1:2381
			items := strings.Split(c.EmbedEtcd.AdvertisePeerUrls, ",")

			sep := ""
			for _, item := range items {
				c.EmbedEtcd.InitialCluster += fmt.Sprintf("%s%s=%s", sep, c.Name, item)
				sep = ","
			}
		}

		adjustString(&c.EmbedEtcd.InitialClusterState, defaultInitialClusterState)
		adjustString(&c.EmbedEtcd.AutoCompactionMode, defaultCompactionMode)
		adjustString(&c.EmbedEtcd.AutoCompactionRetention, defaultAutoCompactionRetention)
		if !configMetaData.IsDefined("quota-backend-bytes") {
			c.EmbedEtcd.QuotaBackendBytes = defaultQuotaBackendBytes
		}
		adjustDuration(&c.EmbedEtcd.TickInterval, defaultTickInterval)
		adjustDuration(&c.EmbedEtcd.ElectionInterval, defaultElectionInterval)

		if len(c.EmbedEtcd.Join) > 0 {
			if _, err := url.Parse(c.EmbedEtcd.Join); err != nil {
				return err
			}
		}
	}

	adjustInt64(&c.LeaderLease, defaultLeaderLease)

	if err := c.Schedule.adjust(configMetaData.Child("schedule"), reloading); err != nil {
		return err
	}
	if err := c.Replication.adjust(configMetaData.Child("replication")); err != nil {
		return err
	}

	if c.FS == nil {
		c.FS = vfs.Default
	}

	return nil
}

// Validate is used to validate if some configurations are right.
func (c *Config) Validate() error {
	// if c.Join != "" && c.InitialCluster != "" {
	// 	return errors.New("-initial-cluster and -join can not be provided at the same time")
	// }
	// dataDir, err := filepath.Abs(c.DataDir)
	// if err != nil {
	// 	return errors.WithStack(err)
	// }
	// logFile, err := filepath.Abs(c.Log.File.Filename)
	// if err != nil {
	// 	return errors.WithStack(err)
	// }
	// rel, err := filepath.Rel(dataDir, filepath.Dir(logFile))
	// if err != nil {
	// 	return errors.WithStack(err)
	// }
	// if !strings.HasPrefix(rel, "..") {
	// 	return errors.New("log directory shouldn't be the subdirectory of data directory")
	// }

	return nil
}
