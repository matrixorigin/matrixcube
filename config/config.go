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

package config

import (
	"path"
	"time"

	"github.com/matrixorigin/matrixcube/aware"
	"github.com/matrixorigin/matrixcube/components/log"
	pconfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/transport"
	"github.com/matrixorigin/matrixcube/vfs"
	"go.uber.org/zap"
)

var (
	kb = 1024
	mb = 1024 * kb

	defaultGroups                   uint64 = 1
	defaultSendRaftBatchSize        uint64 = 64
	defaultMaxConcurrencySnapChunks uint64 = 8
	defaultSnapChunkSize                   = 4 * mb
	defaultRaftMaxWorkers           uint64 = 64
	defaultRaftElectionTick                = 10
	defaultRaftHeartbeatTick               = 2
	defaultShardSplitCheckDuration         = time.Second * 30
	defaultShardStateCheckDuration         = time.Second * 60
	defaultCompactLogCheckDuration         = time.Second * 60
	defaultMaxEntryBytes                   = 10 * mb
	defaultShardCapacityBytes       uint64 = uint64(96 * mb)
	defaultMaxAllowTransferLag      uint64 = 2
	defaultCompactThreshold         uint64 = 256
	defaultRaftTickDuration                = time.Second
	defaultMaxPeerDownTime                 = time.Minute * 30
	defaultShardHeartbeatDuration          = time.Second * 2
	defaultStoreHeartbeatDuration          = time.Second * 10
	defaultMaxInflightMsgs                 = 8
	defaultDataPath                        = "/tmp/matrixcube"
	defaultSnapshotDirName                 = "snapshots"
	defaultProphetDirName                  = "prophet"
	defaultRaftAddr                        = "127.0.0.1:20001"
	defaultRPCAddr                         = "127.0.0.1:20002"
)

// Config matrixcube config
type Config struct {
	RaftAddr            string     `toml:"addr-raft"`
	AdvertiseRaftAddr   string     `toml:"addr-advertise-raft"`
	ClientAddr          string     `toml:"addr-client"`
	AdvertiseClientAddr string     `toml:"addr-advertise-client"`
	DataPath            string     `toml:"dir-data"`
	DeployPath          string     `toml:"dir-deploy"`
	Version             string     `toml:"version"`
	GitHash             string     `toml:"githash"`
	Labels              [][]string `toml:"labels"`
	// Capacity max capacity can use
	Capacity           typeutil.ByteSize `toml:"capacity"`
	UseMemoryAsStorage bool              `toml:"use-memory-as-storage"`
	ShardGroups        uint64            `toml:"shard-groups"`
	Replication        ReplicationConfig `toml:"replication"`
	Snapshot           SnapshotConfig    `toml:"snapshot"`
	// Raft raft config
	Raft RaftConfig `toml:"raft"`
	// Worker worker config
	Worker WorkerConfig `toml:"worker"`
	// Prophet prophet config
	Prophet pconfig.Config `toml:"prophet"`
	// Storage config
	Storage StorageConfig
	// Customize config
	Customize CustomizeConfig
	// Logger logger used in cube
	Logger *zap.Logger `json:"-" toml:"-"`
	// Metric Config
	Metric metric.Cfg `toml:"metric"`
	// FS used in MatrixCube
	FS vfs.FS `json:"-" toml:"-"`
	// Test only used in testing
	Test TestConfig
}

// Adjust adjust
func (c *Config) Adjust() {
	c.validate()

	if c.FS == nil {
		c.FS = vfs.Default
	}

	if c.RaftAddr == "" {
		c.RaftAddr = defaultRaftAddr
	}

	if c.AdvertiseRaftAddr == "" {
		c.AdvertiseRaftAddr = c.RaftAddr
	}

	if c.ClientAddr == "" {
		c.ClientAddr = defaultRPCAddr
	}

	if c.AdvertiseClientAddr == "" {
		c.AdvertiseClientAddr = c.ClientAddr
	}

	if c.DataPath == "" {
		c.DataPath = defaultDataPath
	}

	if c.DeployPath == "" {
		c.DeployPath = "not set"
	}

	if c.GitHash == "" {
		c.DeployPath = "not set"
	}

	if c.ShardGroups == 0 {
		c.ShardGroups = defaultGroups
	}

	(&c.Snapshot).adjust()
	(&c.Replication).adjust()
	(&c.Raft).adjust(uint64(c.Replication.ShardCapacityBytes))
	c.Prophet.DataDir = path.Join(c.DataPath, defaultProphetDirName)
	c.Prophet.ContainerHeartbeatDataProcessor = c.Customize.CustomStoreHeartbeatDataProcessor
	(&c.Prophet).Adjust(nil, false)
	(&c.Worker).adjust()

	if c.Test.ShardStateAware != nil {
		if c.Customize.CustomShardStateAwareFactory != nil {
			c.Test.ShardStateAware.SetWrapper(c.Customize.CustomShardStateAwareFactory())
		}

		c.Customize.CustomShardStateAwareFactory = func() aware.ShardStateAware {
			return c.Test.ShardStateAware
		}
	}

	if c.Test.Shards == nil {
		c.Test.Shards = make(map[uint64]*TestShardConfig)
	}

	c.Logger = log.Adjust(c.Logger).Named("cube")
}

func (c *Config) validate() {
	if c.Storage.DataStorageFactory == nil {
		panic("missing Config.Storage.DataStorageFactory")
	}

	if c.Storage.ForeachDataStorageFunc == nil {
		panic("missing Config.Storage.ForeachDataStorageFunc")
	}
}

// SnapshotDir returns snapshot dir
func (c *Config) SnapshotDir() string {
	return path.Join(c.DataPath, defaultSnapshotDirName)
}

// GetModuleLogger returns logger with named module name
func (c *Config) GetModuleLogger(name string, options ...zap.Option) *zap.Logger {
	return log.Adjust(c.Logger, options...).Named(name)
}

// ReplicationConfig replication config
type ReplicationConfig struct {
	MaxPeerDownTime         typeutil.Duration `toml:"max-peer-down-time"`
	ShardHeartbeatDuration  typeutil.Duration `toml:"shard-heartbeat-duration"`
	StoreHeartbeatDuration  typeutil.Duration `toml:"store-heartbeat-duration"`
	ShardSplitCheckDuration typeutil.Duration `toml:"shard-split-check-duration"`
	ShardStateCheckDuration typeutil.Duration `toml:"shard-state-check-duration"`
	CompactLogCheckDuration typeutil.Duration `toml:"compact-log-check-duration"`
	DisableShardSplit       bool              `toml:"disable-shard-split"`
	AllowRemoveLeader       bool              `toml:"allow-remove-leader"`
	ShardCapacityBytes      typeutil.ByteSize `toml:"shard-capacity-bytes"`
	ShardSplitCheckBytes    typeutil.ByteSize `toml:"shard-split-check-bytes"`
}

func (c *ReplicationConfig) adjust() {
	if c.MaxPeerDownTime.Duration == 0 {
		c.MaxPeerDownTime.Duration = defaultMaxPeerDownTime
	}

	if c.ShardHeartbeatDuration.Duration == 0 {
		c.ShardHeartbeatDuration.Duration = defaultShardHeartbeatDuration
	}

	if c.StoreHeartbeatDuration.Duration == 0 {
		c.StoreHeartbeatDuration.Duration = defaultStoreHeartbeatDuration
	}

	if c.ShardSplitCheckDuration.Duration == 0 {
		c.ShardSplitCheckDuration.Duration = defaultShardSplitCheckDuration
	}

	if c.ShardStateCheckDuration.Duration == 0 {
		c.ShardStateCheckDuration.Duration = defaultShardStateCheckDuration
	}

	if c.CompactLogCheckDuration.Duration == 0 {
		c.CompactLogCheckDuration.Duration = defaultCompactLogCheckDuration
	}

	if c.ShardCapacityBytes == 0 {
		c.ShardCapacityBytes = typeutil.ByteSize(defaultShardCapacityBytes)
	}

	if c.ShardSplitCheckBytes == 0 {
		c.ShardSplitCheckBytes = c.ShardCapacityBytes * 80 / 100
	}
}

// SnapshotConfig snapshot config
type SnapshotConfig struct {
	MaxConcurrencySnapChunks uint64            `toml:"max-concurrency-snap-chunks"`
	SnapChunkSize            typeutil.ByteSize `toml:"snap-chunk-size"`
}

func (c *SnapshotConfig) adjust() {
	if c.MaxConcurrencySnapChunks == 0 {
		c.MaxConcurrencySnapChunks = defaultMaxConcurrencySnapChunks
	}

	if c.SnapChunkSize == 0 {
		c.SnapChunkSize = typeutil.ByteSize(defaultSnapChunkSize)
	}
}

// WorkerConfig worker config
type WorkerConfig struct {
	RaftEventWorkers uint64 `toml:"raft-event-workers"`
}

func (c *WorkerConfig) adjust() {
	if c.RaftEventWorkers == 0 {
		c.RaftEventWorkers = defaultRaftMaxWorkers
	}
}

// ShardConfig shard config
type ShardConfig struct {
	// SplitCheckInterval interval to check shard whether need to be split or not.
	SplitCheckInterval typeutil.Duration `toml:"split-check-interval"`
	// SplitCheckDiff when size change of shard exceed the diff since last check, it
	// will be checked again whether it should be split.
	SplitCheckDiff uint64 `toml:"split-check-diff"`
}

// RaftConfig raft config
type RaftConfig struct {
	// TickInterval raft tick interval
	TickInterval typeutil.Duration `toml:"tick-interval"`
	// HeartbeatTicks how many ticks to send raft heartbeat message
	HeartbeatTicks int `toml:"heartbeat-ticks"`
	// ElectionTimeoutTicks how many ticks to send election message
	ElectionTimeoutTicks int `toml:"election-timeout-ticks"`
	// MaxSizePerMsg max bytes per raft message
	MaxSizePerMsg typeutil.ByteSize `toml:"max-size-per-msg"`
	// MaxInflightMsgs max raft message count in a raft rpc
	MaxInflightMsgs int `toml:"max-inflight-msgs"`
	// MaxEntryBytes max bytes of entry in a proposal message
	MaxEntryBytes typeutil.ByteSize `toml:"max-entry-bytes"`
	// SendRaftBatchSize raft message sender count
	SendRaftBatchSize uint64 `toml:"send-raft-batch-size"`
	// RaftLog raft log 配置
	RaftLog RaftLogConfig `toml:"raft-log"`
}

// GetElectionTimeoutDuration returns ElectionTimeoutTicks * TickInterval
func (c *RaftConfig) GetElectionTimeoutDuration() time.Duration {
	return time.Duration(c.ElectionTimeoutTicks) * c.TickInterval.Duration
}

// GetHeartbeatDuration returns HeartbeatTicks * TickInterval
func (c *RaftConfig) GetHeartbeatDuration() time.Duration {
	return time.Duration(c.HeartbeatTicks) * c.TickInterval.Duration
}

func (c *RaftConfig) adjust(shardCapacityBytes uint64) {
	if c.TickInterval.Duration == 0 {
		c.TickInterval.Duration = defaultRaftTickDuration
	}

	if c.HeartbeatTicks == 0 {
		c.HeartbeatTicks = defaultRaftHeartbeatTick
	}

	if c.ElectionTimeoutTicks == 0 {
		c.ElectionTimeoutTicks = defaultRaftElectionTick
	}

	if c.MaxInflightMsgs == 0 {
		c.MaxInflightMsgs = defaultMaxInflightMsgs
	}

	if c.SendRaftBatchSize == 0 {
		c.SendRaftBatchSize = defaultSendRaftBatchSize
	}

	if c.MaxEntryBytes == 0 {
		c.MaxEntryBytes = typeutil.ByteSize(defaultMaxEntryBytes)
	}

	(&c.RaftLog).adjust(shardCapacityBytes)
}

// RaftLogConfig raft log config
type RaftLogConfig struct {
	DisableSync         bool   `toml:"disable-sync"`
	CompactThreshold    uint64 `toml:"compact-threshold"`
	MaxAllowTransferLag uint64 `toml:"max-allow-transfer-lag"`
	ForceCompactCount   uint64 `toml:"force-compact-log-count"`
	ForceCompactBytes   uint64 `toml:"force-compact-log-bytes"`
}

func (c *RaftLogConfig) adjust(shardCapacityBytes uint64) {
	if c.MaxAllowTransferLag == 0 {
		c.MaxAllowTransferLag = defaultMaxAllowTransferLag
	}

	if c.CompactThreshold == 0 {
		c.CompactThreshold = defaultCompactThreshold
	}

	if c.ForceCompactCount == 0 {
		c.ForceCompactCount = shardCapacityBytes * uint64(mb) * 3 / 4 / uint64(kb)
	}

	if c.ForceCompactBytes == 0 {
		c.ForceCompactBytes = shardCapacityBytes * 3 / 4
	}
}

// StorageConfig storage config
type StorageConfig struct {

	// DataStorageFactory is a storage factory  to store application's data
	DataStorageFactory func(group uint64) storage.DataStorage `json:"-" toml:"-"`
	// ForeachDataStorageFunc do in every storage
	ForeachDataStorageFunc func(cb func(storage.DataStorage)) `json:"-" toml:"-"`
}

// CustomizeConfig customize config
type CustomizeConfig struct {
	// CustomShardStateAwareFactory is a factory func to create aware.ShardStateAware to handled shard life cycle.
	CustomShardStateAwareFactory func() aware.ShardStateAware `json:"-" toml:"-"`
	// CustomInitShardsFactory is a factory func to provide init shards to cube to bootstrap the cluster.
	CustomInitShardsFactory func() []meta.Shard `json:"-" toml:"-"`
	// CustomStoreHeartbeatDataProcessor process store heartbeat data, collect, store and process customize data
	CustomStoreHeartbeatDataProcessor StoreHeartbeatDataProcessor `json:"-" toml:"-"`
	// CustomShardPoolShardFactory is factory create a shard used by shard pool, `start, end and unique` is created by
	// `ShardPool` based on `offsetInPool`, these can be modified, provided that the only non-conflict.
	CustomShardPoolShardFactory func(g uint64, start, end []byte, unique string, offsetInPool uint64) meta.Shard `json:"-" toml:"-"`
	// CustomTransportFilter transport filter
	CustomTransportFilter func(meta.RaftMessage) bool `json:"-" toml:"-"`
	// CustomWrapNewTransport wraps new transports
	CustomWrapNewTransport func(transport.Trans) transport.Trans `json:"-" toml:"-"`
}

// GetLabels returns lables
func (c *Config) GetLabels() []metapb.Pair {
	var labels []metapb.Pair
	for _, kv := range c.Labels {
		labels = append(labels, metapb.Pair{
			Key:   kv[0],
			Value: kv[1],
		})
	}

	return labels
}

// StoreHeartbeatDataProcessor process store heartbeat data, collect, store and process customize data
type StoreHeartbeatDataProcessor interface {
	pconfig.ContainerHeartbeatDataProcessor

	// HandleHeartbeatRsp handle the data from store heartbeat at the each worker node
	HandleHeartbeatRsp(data []byte) error
	// CollectData collect data at every heartbeat
	CollectData() []byte
}

// TestConfig all test config
type TestConfig struct {
	// ShardStateAware is a ShardStateAware wrapper for the aware which created by
	// `CustomizeConfig.CustomShardStateAwareFactory`
	ShardStateAware aware.TestShardStateAware `json:"-" toml:"-"`
	// ReplicaSetSnapshotJobWait sleep before set snapshot job
	ReplicaSetSnapshotJobWait time.Duration
	// SaveDynamicallyShardInitStateWait wait before save dynamically shard init state
	SaveDynamicallyShardInitStateWait time.Duration
	// ShardPoolCreateWaitC waiting delay for shard creation
	ShardPoolCreateWaitC chan struct{} `json:"-" toml:"-"`
	// Shards test config for shards
	Shards map[uint64]*TestShardConfig `json:"-" toml:"-"`
}

// TestShardConfig shard test config
type TestShardConfig struct {
	// SkipSaveRaftApplyState skip save raft apply state to metastorage after applied a raft log
	SkipSaveRaftApplyState bool
	// SkipApply skip apply any raft log
	SkipApply bool
}
