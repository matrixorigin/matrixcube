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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/matrixorigin/matrixcube/components/prophet/limit"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"go.etcd.io/etcd/embed"
)

// Config the prophet configuration
type Config struct {
	Name       string            `toml:"name" json:"name"`
	DataDir    string            `toml:"data-dir"`
	RPCAddr    string            `toml:"rpc-addr"`
	RPCTimeout typeutil.Duration `toml:"rpc-timeout"`

	// etcd configuration
	StorageNode  bool            `toml:"storage-node"`
	ExternalEtcd []string        `toml:"external-etcd"`
	EmbedEtcd    EmbedEtcdConfig `toml:"embed-etcd"`

	// LeaderLease time, if leader doesn't update its TTL
	// in etcd after lease time, etcd will expire the leader key
	// and other servers can campaign the leader again.
	// Etcd only supports seconds TTL, so here is second too.
	LeaderLease int64 `toml:"lease" json:"lease"`

	Schedule      ScheduleConfig      `toml:"schedule" json:"schedule"`
	Replication   ReplicationConfig   `toml:"replication" json:"replication"`
	LabelProperty LabelPropertyConfig `toml:"label-property" json:"label-property"`

	Handler                         metadata.RoleChangeHandler                                                      `toml:"-" json:"-"`
	Adapter                         metadata.Adapter                                                                `toml:"-" json:"-"`
	ResourceStateChangedHandler     func(res metadata.Resource, from metapb.ResourceState, to metapb.ResourceState) `toml:"-" json:"-"`
	ContainerHeartbeatDataProcessor ContainerHeartbeatDataProcessor                                                 `toml:"-" json:"-"`

	// Job processor register
	jobMu struct {
		sync.RWMutex
		jobProcessors map[metapb.JobType]JobProcessor
	} `toml:"-" json:"-"`

	// Only test can change them.
	DisableStrictReconfigCheck bool `toml:"-" json:"-"`
	// DisableResponse skip all client request
	DisableResponse bool `toml:"-" json:"-"`
	// EnableResponseNotLeader return not leader error for all client request
	EnableResponseNotLeader bool      `toml:"-" json:"-"`
	TestCtx                 *sync.Map `toml:"-" json:"-"`
}

// NewConfig creates a new config.
func NewConfig() *Config {
	return &Config{}
}

// NewConfigWithFile new config with config file
func NewConfigWithFile(file string) (*Config, error) {
	c := NewConfig()

	// Load config file if specified.
	meta, err := toml.DecodeFile(file, c)
	if err != nil {
		return nil, err
	}

	err = c.Adjust(&meta, false)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// GenEmbedEtcdConfig gen embed etcd config
func (c *Config) GenEmbedEtcdConfig() (*embed.Config, error) {
	cfg := embed.NewConfig()
	cfg.Name = c.Name
	cfg.Dir = c.DataDir
	cfg.WalDir = ""
	cfg.InitialCluster = c.EmbedEtcd.InitialCluster
	cfg.ClusterState = c.EmbedEtcd.InitialClusterState
	cfg.EnablePprof = true
	cfg.PreVote = c.EmbedEtcd.PreVote
	cfg.StrictReconfigCheck = !c.DisableStrictReconfigCheck
	cfg.TickMs = uint(c.EmbedEtcd.TickInterval.Duration / time.Millisecond)
	cfg.ElectionMs = uint(c.EmbedEtcd.ElectionInterval.Duration / time.Millisecond)
	cfg.AutoCompactionMode = c.EmbedEtcd.AutoCompactionMode
	cfg.AutoCompactionRetention = c.EmbedEtcd.AutoCompactionRetention
	cfg.QuotaBackendBytes = int64(c.EmbedEtcd.QuotaBackendBytes)

	var err error
	cfg.LPUrls, err = util.ParseUrls(c.EmbedEtcd.PeerUrls)
	if err != nil {
		return nil, err
	}

	cfg.APUrls, err = util.ParseUrls(c.EmbedEtcd.AdvertisePeerUrls)
	if err != nil {
		return nil, err
	}

	cfg.LCUrls, err = util.ParseUrls(c.EmbedEtcd.ClientUrls)
	if err != nil {
		return nil, err
	}

	cfg.ACUrls, err = util.ParseUrls(c.EmbedEtcd.AdvertiseClientUrls)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

// EmbedEtcdConfig embed etcd config
type EmbedEtcdConfig struct {
	Join                string `toml:"join"`
	ClientUrls          string `toml:"client-urls"`
	PeerUrls            string `toml:"peer-urls"`
	AdvertiseClientUrls string `toml:"advertise-client-urls"`
	AdvertisePeerUrls   string `toml:"advertise-peer-urls"`
	InitialCluster      string `toml:"initial-cluster"`
	InitialClusterState string `toml:"initial-cluster-state"`
	// TickInterval is the interval for etcd Raft tick.
	TickInterval typeutil.Duration `toml:"tick-interval"`
	// ElectionInterval is the interval for etcd Raft election.
	ElectionInterval typeutil.Duration `toml:"election-interval"`
	// Prevote is true to enable Raft Pre-Vote.
	// If enabled, Raft runs an additional election phase
	// to check whether it would get enough votes to win
	// an election, thus minimizing disruptions.
	PreVote bool `toml:"enable-prevote"`
	// AutoCompactionMode is either 'periodic' or 'revision'. The default value is 'periodic'.
	AutoCompactionMode string `toml:"auto-compaction-mode"`
	// AutoCompactionRetention is either duration string with time unit
	// (e.g. '5m' for 5-minute), or revision unit (e.g. '5000').
	// If no time unit is provided and compaction mode is 'periodic',
	// the unit defaults to hour. For example, '5' translates into 5-hour.
	// The default retention is 1 hour.
	// Before etcd v3.3.x, the type of retention is int. We add 'v2' suffix to make it backward compatible.
	AutoCompactionRetention string `toml:"auto-compaction-retention"`
	// QuotaBackendBytes Raise alarms when backend size exceeds the given quota. 0 means use the default quota.
	// the default size is 2GB, the maximum is 8GB.
	QuotaBackendBytes typeutil.ByteSize `toml:"quota-backend-bytes" json:"quota-backend-bytes"`
}

// ScheduleConfig is the schedule configuration.
type ScheduleConfig struct {
	// If the snapshot count of one container is greater than this value,
	// it will never be used as a source or target container.
	MaxSnapshotCount    uint64 `toml:"max-snapshot-count" json:"max-snapshot-count"`
	MaxPendingPeerCount uint64 `toml:"max-pending-peer-count" json:"max-pending-peer-count"`
	// If both the size of resource is smaller than MaxMergeResourceSize
	// and the number of rows in resource is smaller than MaxMergeResourceKeys,
	// it will try to merge with adjacent resources.
	MaxMergeResourceSize uint64 `toml:"max-merge-resource-size" json:"max-merge-resource-size"`
	MaxMergeResourceKeys uint64 `toml:"max-merge-resource-keys" json:"max-merge-resource-keys"`
	// SplitMergeInterval is the minimum interval time to permit merge after split.
	SplitMergeInterval typeutil.Duration `toml:"split-merge-interval" json:"split-merge-interval"`
	// EnableOneWayMerge is the option to enable one way merge. This means a resource can only be merged into the next resource of it.
	EnableOneWayMerge bool `toml:"enable-one-way-merge" json:"enable-one-way-merge,string"`
	// EnableCrossTableMerge is the option to enable cross table merge. This means two resources can be merged with different table IDs.
	// This option only works when key type is "table".
	EnableCrossTableMerge bool `toml:"enable-cross-table-merge" json:"enable-cross-table-merge,string"`
	// PatrolResourceInterval is the interval for scanning resource during patrol.
	PatrolResourceInterval typeutil.Duration `toml:"patrol-resource-interval" json:"patrol-resource-interval"`
	// MaxContainerDownTime is the max duration after which
	// a container will be considered to be down if it hasn't reported heartbeats.
	MaxContainerDownTime typeutil.Duration `toml:"max-container-down-time" json:"max-container-down-time"`
	// LeaderScheduleLimit is the max coexist leader schedules.
	LeaderScheduleLimit uint64 `toml:"leader-schedule-limit" json:"leader-schedule-limit"`
	// LeaderSchedulePolicy is the option to balance leader, there are some policies supported: ["count", "size"], default: "count"
	LeaderSchedulePolicy string `toml:"leader-schedule-policy" json:"leader-schedule-policy"`
	// ResourceScheduleLimit is the max coexist resource schedules.
	ResourceScheduleLimit uint64 `toml:"resource-schedule-limit" json:"resource-schedule-limit"`
	// ReplicaScheduleLimit is the max coexist replica schedules.
	ReplicaScheduleLimit uint64 `toml:"replica-schedule-limit" json:"replica-schedule-limit"`
	// MergeScheduleLimit is the max coexist merge schedules.
	MergeScheduleLimit uint64 `toml:"merge-schedule-limit" json:"merge-schedule-limit"`
	// HotResourceScheduleLimit is the max coexist hot resource schedules.
	HotResourceScheduleLimit uint64 `toml:"hot-resource-schedule-limit" json:"hot-resource-schedule-limit"`
	// HotResourceCacheHitsThreshold is the cache hits threshold of the hot resource.
	// If the number of times a resource hits the hot cache is greater than this
	// threshold, it is considered a hot resource.
	HotResourceCacheHitsThreshold uint64 `toml:"hot-resource-cache-hits-threshold" json:"hot-resource-cache-hits-threshold"`
	// ContainerLimit is the limit of scheduling for containers.
	ContainerLimit map[uint64]ContainerLimitConfig `toml:"container-limit" json:"container-limit"`
	// TolerantSizeRatio is the ratio of buffer size for balance scheduler.
	TolerantSizeRatio float64 `toml:"tolerant-size-ratio" json:"tolerant-size-ratio"`
	//
	//      high space stage         transition stage           low space stage
	//   |--------------------|-----------------------------|-------------------------|
	//   ^                    ^                             ^                         ^
	//   0       HighSpaceRatio * capacity       LowSpaceRatio * capacity          capacity
	//
	// LowSpaceRatio is the lowest usage ratio of container which regraded as low space.
	// When in low space, container resource score increases to very large and varies inversely with available size.
	LowSpaceRatio float64 `toml:"low-space-ratio" json:"low-space-ratio"`
	// HighSpaceRatio is the highest usage ratio of container which regraded as high space.
	// High space means there is a lot of spare capacity, and container resource score varies directly with used size.
	HighSpaceRatio float64 `toml:"high-space-ratio" json:"high-space-ratio"`
	// ResourceScoreFormulaVersion is used to control the formula used to calculate resource score.
	ResourceScoreFormulaVersion string `toml:"resource-score-formula-version" json:"resource-score-formula-version"`
	// SchedulerMaxWaitingOperator is the max coexist operators for each scheduler.
	SchedulerMaxWaitingOperator uint64 `toml:"scheduler-max-waiting-operator" json:"scheduler-max-waiting-operator"`

	// EnableRemoveDownReplica is the option to enable replica checker to remove down replica.
	EnableRemoveDownReplica bool `toml:"enable-remove-down-replica" json:"enable-remove-down-replica,string"`
	// EnableReplaceOfflineReplica is the option to enable replica checker to replace offline replica.
	EnableReplaceOfflineReplica bool `toml:"enable-replace-offline-replica" json:"enable-replace-offline-replica,string"`
	// EnableMakeUpReplica is the option to enable replica checker to make up replica.
	EnableMakeUpReplica bool `toml:"enable-make-up-replica" json:"enable-make-up-replica,string"`
	// EnableRemoveExtraReplica is the option to enable replica checker to remove extra replica.
	EnableRemoveExtraReplica bool `toml:"enable-remove-extra-replica" json:"enable-remove-extra-replica,string"`
	// EnableLocationReplacement is the option to enable replica checker to move replica to a better location.
	EnableLocationReplacement bool `toml:"enable-location-replacement" json:"enable-location-replacement,string"`
	// EnableDebugMetrics is the option to enable debug metrics.
	EnableDebugMetrics bool `toml:"enable-debug-metrics" json:"enable-debug-metrics,string"`
	// EnableJointConsensus is the option to enable using joint consensus as a operator step.
	EnableJointConsensus bool `toml:"enable-joint-consensus" json:"enable-joint-consensus,string"`

	// Schedulers support for loading customized schedulers
	Schedulers SchedulerConfigs `toml:"schedulers" json:"schedulers-v2"` // json v2 is for the sake of compatible upgrade

	// Only used to display
	SchedulersPayload map[string]interface{} `toml:"schedulers-payload" json:"schedulers-payload"`

	// ContainerLimitMode can be auto or manual, when set to auto,
	// Prophet tries to change the container limit values according to
	// the load state of the cluster dynamically. User can
	// overwrite the auto-tuned value by pd-ctl, when the value
	// is overwritten, the value is fixed until it is deleted.
	// Default: manual
	ContainerLimitMode string `toml:"container-limit-mode" json:"container-limit-mode"`
}

// SchedulerConfigs is a slice of customized scheduler configuration.
type SchedulerConfigs []SchedulerConfig

// SchedulerConfig is customized scheduler configuration
type SchedulerConfig struct {
	Type        string   `toml:"type" json:"type"`
	Args        []string `toml:"args" json:"args"`
	Disable     bool     `toml:"disable" json:"disable"`
	ArgsPayload string   `toml:"args-payload" json:"args-payload"`
}

// DefaultSchedulers are the schedulers be created by default.
// If these schedulers are not in the persistent configuration, they
// will be created automatically when reloading.
var DefaultSchedulers = SchedulerConfigs{
	{Type: "balance-resource"},
	{Type: "balance-leader"},
	{Type: "hot-resource"},
	{Type: "label"},
}

// IsDefaultScheduler checks whether the scheduler is enable by default.
func IsDefaultScheduler(typ string) bool {
	for _, c := range DefaultSchedulers {
		if typ == c.Type {
			return true
		}
	}
	return false
}

// ContainerLimitConfig is a config about scheduling rate limit of different types for a container.
type ContainerLimitConfig struct {
	AddPeer    float64 `toml:"add-peer" json:"add-peer"`
	RemovePeer float64 `toml:"remove-peer" json:"remove-peer"`
}

// Clone returns a cloned scheduling configuration.
func (c *ScheduleConfig) Clone() *ScheduleConfig {
	schedulers := append(c.Schedulers[:0:0], c.Schedulers...)
	var containerLimit map[uint64]ContainerLimitConfig
	if c.ContainerLimit != nil {
		containerLimit = make(map[uint64]ContainerLimitConfig, len(c.ContainerLimit))
		for k, v := range c.ContainerLimit {
			containerLimit[k] = v
		}
	}
	cfg := *c
	cfg.ContainerLimit = containerLimit
	cfg.Schedulers = schedulers
	cfg.SchedulersPayload = nil
	return &cfg
}

func (c *ScheduleConfig) adjust(meta *configMetaData, reloading bool) error {
	if !meta.IsDefined("max-snapshot-count") {
		adjustUint64(&c.MaxSnapshotCount, defaultMaxSnapshotCount)
	}
	if !meta.IsDefined("max-pending-peer-count") {
		adjustUint64(&c.MaxPendingPeerCount, defaultMaxPendingPeerCount)
	}
	if !meta.IsDefined("max-merge-resource-size") {
		adjustUint64(&c.MaxMergeResourceSize, defaultMaxMergeResourceSize)
	}
	if !meta.IsDefined("max-merge-resource-keys") {
		adjustUint64(&c.MaxMergeResourceKeys, defaultMaxMergeResourceKeys)
	}
	adjustDuration(&c.SplitMergeInterval, defaultSplitMergeInterval)
	adjustDuration(&c.PatrolResourceInterval, defaultPatrolResourceInterval)
	adjustDuration(&c.MaxContainerDownTime, defaultMaxContainerDownTime)
	if !meta.IsDefined("leader-schedule-limit") {
		adjustUint64(&c.LeaderScheduleLimit, defaultLeaderScheduleLimit)
	}
	if !meta.IsDefined("resource-schedule-limit") {
		adjustUint64(&c.ResourceScheduleLimit, defaultResourceScheduleLimit)
	}
	if !meta.IsDefined("replica-schedule-limit") {
		adjustUint64(&c.ReplicaScheduleLimit, defaultReplicaScheduleLimit)
	}
	if !meta.IsDefined("merge-schedule-limit") {
		adjustUint64(&c.MergeScheduleLimit, defaultMergeScheduleLimit)
	}
	if !meta.IsDefined("hot-resource-schedule-limit") {
		adjustUint64(&c.HotResourceScheduleLimit, defaultHotResourceScheduleLimit)
	}
	if !meta.IsDefined("hot-resource-cache-hits-threshold") {
		adjustUint64(&c.HotResourceCacheHitsThreshold, defaultHotResourceCacheHitsThreshold)
	}
	if !meta.IsDefined("tolerant-size-ratio") {
		adjustFloat64(&c.TolerantSizeRatio, defaultTolerantSizeRatio)
	}
	if !meta.IsDefined("scheduler-max-waiting-operator") {
		adjustUint64(&c.SchedulerMaxWaitingOperator, defaultSchedulerMaxWaitingOperator)
	}
	if !meta.IsDefined("leader-schedule-policy") {
		adjustString(&c.LeaderSchedulePolicy, defaultLeaderSchedulePolicy)
	}
	if !meta.IsDefined("container-limit-mode") {
		adjustString(&c.ContainerLimitMode, defaultContainerLimitMode)
	}
	if !meta.IsDefined("enable-joint-consensus") {
		c.EnableJointConsensus = defaultEnableJointConsensus
	}
	if !meta.IsDefined("enable-cross-table-merge") {
		c.EnableCrossTableMerge = defaultEnableCrossTableMerge
	}
	adjustFloat64(&c.LowSpaceRatio, defaultLowSpaceRatio)
	adjustFloat64(&c.HighSpaceRatio, defaultHighSpaceRatio)

	// new cluster:v2, old cluster:v1
	if !meta.IsDefined("resource-score-formula-version") && !reloading {
		adjustString(&c.ResourceScoreFormulaVersion, defaultResourceScoreFormulaVersion)
	}

	adjustSchedulers(&c.Schedulers, DefaultSchedulers)

	if c.ContainerLimit == nil {
		c.ContainerLimit = make(map[uint64]ContainerLimitConfig)
	}

	return c.Validate()
}

// Validate is used to validate if some scheduling configurations are right.
func (c *ScheduleConfig) Validate() error {
	if c.TolerantSizeRatio < 0 {
		return errors.New("tolerant-size-ratio should be nonnegative")
	}
	if c.LowSpaceRatio < 0 || c.LowSpaceRatio > 1 {
		return errors.New("low-space-ratio should between 0 and 1")
	}
	if c.HighSpaceRatio < 0 || c.HighSpaceRatio > 1 {
		return errors.New("high-space-ratio should between 0 and 1")
	}
	if c.LowSpaceRatio <= c.HighSpaceRatio {
		return errors.New("low-space-ratio should be larger than high-space-ratio")
	}
	for _, scheduleConfig := range c.Schedulers {
		if !IsSchedulerRegistered(scheduleConfig.Type) {
			return fmt.Errorf("create func of %v is not registered, maybe misspelled", scheduleConfig.Type)
		}
	}
	return nil
}

// ReplicationConfig is the replication configuration.
type ReplicationConfig struct {
	// MaxReplicas is the number of replicas for each resource.
	MaxReplicas uint64 `toml:"max-replicas" json:"max-replicas"`

	// The label keys specified the location of a container.
	// The placement priorities is implied by the order of label keys.
	// For example, ["zone", "rack"] means that we should place replicas to
	// different zones first, then to different racks if we don't have enough zones.
	LocationLabels typeutil.StringSlice `toml:"location-labels" json:"location-labels"`
	// StrictlyMatchLabel strictly checks if the label of your storage application is matched with LocationLabels.
	StrictlyMatchLabel bool `toml:"strictly-match-label" json:"strictly-match-label,string"`

	// When PlacementRules feature is enabled. MaxReplicas, LocationLabels and IsolationLabels are not used any more.
	EnablePlacementRules bool `toml:"enable-placement-rules" json:"enable-placement-rules,string"`

	// IsolationLevel is used to isolate replicas explicitly and forcibly if it's not empty.
	// Its value must be empty or one of LocationLabels.
	// Example:
	// location-labels = ["zone", "rack", "host"]
	// isolation-level = "zone"
	// With configuration like above, Prophet ensure that all replicas be placed in different zones.
	// Even if a zone is down, Prophet will not try to make up replicas in other zone
	// because other zones already have replicas on it.
	IsolationLevel string `toml:"isolation-level" json:"isolation-level"`

	// Groups resources groups
	Groups []uint64 `toml:"groups" json:"groups"`
}

// Clone makes a deep copy of the config.
func (c *ReplicationConfig) Clone() *ReplicationConfig {
	locationLabels := append(c.LocationLabels[:0:0], c.LocationLabels...)
	cfg := *c
	cfg.LocationLabels = locationLabels
	return &cfg
}

// Validate is used to validate if some replication configurations are right.
func (c *ReplicationConfig) Validate() error {
	foundIsolationLevel := false
	for _, label := range c.LocationLabels {
		err := ValidateLabels([]metapb.Pair{{Key: label}})
		if err != nil {
			return err
		}
		// IsolationLevel should be empty or one of LocationLabels
		if !foundIsolationLevel && label == c.IsolationLevel {
			foundIsolationLevel = true
		}
	}
	if c.IsolationLevel != "" && !foundIsolationLevel {
		return errors.New("isolation-level must be one of location-labels or empty")
	}
	return nil
}

func (c *ReplicationConfig) adjust(meta *configMetaData) error {
	adjustUint64(&c.MaxReplicas, defaultMaxReplicas)
	if !meta.IsDefined("enable-placement-rules") {
		c.EnablePlacementRules = defaultEnablePlacementRules
	}
	if !meta.IsDefined("strictly-match-label") {
		c.StrictlyMatchLabel = defaultStrictlyMatchLabel
	}
	if !meta.IsDefined("location-labels") {
		c.LocationLabels = defaultLocationLabels
	}
	if !meta.IsDefined("groups") {
		if len(c.Groups) == 0 {
			c.Groups = []uint64{0}
		}
	}
	return c.Validate()
}

// LabelPropertyConfig is the config section to set properties to container labels.
type LabelPropertyConfig map[string][]ContainerLabel

// ContainerLabel is the config item of LabelPropertyConfig.
type ContainerLabel struct {
	Key   string `toml:"key" json:"key"`
	Value string `toml:"value" json:"value"`
}

// Clone returns a cloned label property configuration.
func (c LabelPropertyConfig) Clone() LabelPropertyConfig {
	m := make(map[string][]ContainerLabel, len(c))
	for k, sl := range c {
		sl2 := make([]ContainerLabel, 0, len(sl))
		sl2 = append(sl2, sl...)
		m[k] = sl2
	}
	return m
}

// ContainerLimit is the default limit of adding peer and removing peer when putting containers.
type ContainerLimit struct {
	mu sync.RWMutex
	// AddPeer is the default rate of adding peers for container limit (per minute).
	AddPeer float64
	// RemovePeer is the default rate of removing peers for container limit (per minute).
	RemovePeer float64
}

// SetDefaultContainerLimit sets the default container limit for a given type.
func (sl *ContainerLimit) SetDefaultContainerLimit(typ limit.Type, ratePerMin float64) {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	switch typ {
	case limit.AddPeer:
		sl.AddPeer = ratePerMin
	case limit.RemovePeer:
		sl.RemovePeer = ratePerMin
	}
}

// GetDefaultContainerLimit gets the default container limit for a given type.
func (sl *ContainerLimit) GetDefaultContainerLimit(typ limit.Type) float64 {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	switch typ {
	case limit.AddPeer:
		return sl.AddPeer
	case limit.RemovePeer:
		return sl.RemovePeer
	default:
		panic("invalid type")
	}
}

// ContainerHeartbeatDataProcessor process store heartbeat data, collect, store and process customize data
type ContainerHeartbeatDataProcessor interface {
	// Start init all customize data if the current node became the prophet leader
	Start(storage.Storage) error
	// Stop clear all customize data at current node, and other node became leader and will call `Start`
	Stop(storage.Storage) error
	// HandleHeartbeatReq handle the data from store heartbeat at the prophet leader node
	HandleHeartbeatReq(id uint64, data []byte, store storage.Storage) (responseData []byte, err error)
}
