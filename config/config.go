package config

import (
	"log"
	"path"
	"time"

	pconfig "github.com/deepfabric/prophet/config"
	"github.com/deepfabric/prophet/pb/metapb"
	"github.com/deepfabric/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/aware"
	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/snapshot"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/transport"
)

var (
	kb = 1024
	mb = 1024 * kb

	defaultGroups                   uint64 = 1
	defaultSendRaftBatchSize        uint64 = 64
	defaultMaxConcurrencySnapChunks uint64 = 8
	defaultSnapChunkSize                   = 4 * mb
	defaultApplyWorkerCount         uint64 = 32
	defaultSendRaftMsgWorkerCount   uint64 = 8
	defaultRaftMaxWorkers           uint64 = 32
	defaultRaftElectionTick                = 10
	defaultRaftHeartbeatTick               = 2
	defaultRaftLogCompactDuration          = time.Second * 30
	defaultShardSplitCheckDuration         = time.Second * 30
	defaultEnsureNewSuardInterval          = time.Second * 10
	defaultMaxProposalBytes                = 10 * mb
	defaultShardCapacityBytes       uint64 = uint64(96 * mb)
	defaultMaxAllowTransferLogLag   uint64 = 2
	defaultRaftThresholdCompactLog  uint64 = 256
	defaultRaftTickDuration                = time.Second
	defaultMaxPeerDownTime                 = time.Minute * 30
	defaultShardHeartbeatDuration          = time.Second * 2
	defaultStoreHeartbeatDuration          = time.Second * 10
	defaultMaxInflightMsgs                 = 8
	defaultDataPath                        = "/tmp/beehive"
	defaultSnapshotDirName                 = "snapshots"
	defaultProphetDirName                  = "prophet"
	defaultRaftAddr                        = "127.0.0.1:20001"
	defaultRPCAddr                         = "127.0.0.1:20002"
)

// Config beehive config
type Config struct {
	RaftAddr   string     `toml:"addr-raft"`
	ClientAddr string     `toml:"addr-client"`
	DataPath   string     `toml:"dir-data"`
	DeployPath string     `toml:"dir-deploy"`
	Version    string     `toml:"version"`
	GitHash    string     `toml:"githash"`
	Labels     [][]string `toml:"labels"`
	// Capacity max capacity can use
	Capacity           typeutil.ByteSize `toml:"capacity"`
	UseMemoryAsStorage bool              `toml:"use-memory-as-storage"`
	ShardGroups        uint64            `toml:"shard-groups"`
	Replication        ReplicationConfig `toml:"replication"`
	Snapshot           SnapshotConfig    `toml:"snapshot"`
	// Raft raft config
	Raft RaftConfig `toml:"raft"`
	// Storage config
	Storage StorageConfig `toml:"storage"`
	// Worker config
	Worker WorkerConfig `toml:"worker"`
	// Customize config
	Customize CustomizeConfig
	// Prophet config
	Prophet pconfig.Config `toml:"prophet"`
	// Metric Config
	Metric metric.Cfg `toml:"metric"`
}

// Adjust adjust
func (c *Config) Adjust() {
	c.validate()

	if c.RaftAddr == "" {
		c.RaftAddr = defaultRaftAddr
	}

	if c.ClientAddr == "" {
		c.ClientAddr = defaultRPCAddr
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
	(&c.Prophet).Adjust(nil, false)
	(&c.Worker).adjust()
}

func (c *Config) validate() {
	if c.Storage.DataStorageFactory == nil {
		log.Panicf("missing Config.Storage.DataStorageFactory")
	}

	if c.Storage.MetaStorage == nil {
		log.Panicf("missing Config.Storage.MetaStorage")
	}

	if c.Storage.ForeachDataStorageFunc == nil {
		log.Panicf("missing Config.Storage.ForeachDataStorageFunc")
	}
}

// SnapshotDir returns snapshot dir
func (c *Config) SnapshotDir() string {
	return path.Join(c.DataPath, defaultSnapshotDirName)
}

// ReplicationConfig replication config
type ReplicationConfig struct {
	MaxPeerDownTime         typeutil.Duration `toml:"max-peer-down-time"`
	ShardHeartbeatDuration  typeutil.Duration `toml:"shard-heartbeat-duration"`
	StoreHeartbeatDuration  typeutil.Duration `toml:"store-heartbeat-duration"`
	ShardSplitCheckDuration typeutil.Duration `toml:"shard-split-check-duration"`
	DisableShardSplit       bool              `toml:"disable-shard-split"`
	AllowRemoveLeader       bool              `toml:"allow-remove-leader"`
	ShardCapacityBytes      typeutil.ByteSize `toml:"shard-capacity-bytes"`
	ShardSplitCheckBytes    typeutil.ByteSize `toml:"shard-split-check-bytes"`
	EnsureNewShardInterval  typeutil.Duration `toml:"ensure-new-shard-duration"`
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

	if c.ShardCapacityBytes == 0 {
		c.ShardCapacityBytes = typeutil.ByteSize(defaultShardCapacityBytes)
	}

	if c.ShardSplitCheckBytes == 0 {
		c.ShardSplitCheckBytes = c.ShardCapacityBytes * 80 / 100
	}

	if c.EnsureNewShardInterval.Duration == 0 {
		c.EnsureNewShardInterval.Duration = defaultEnsureNewSuardInterval
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
	ApplyWorkerCount       uint64 `toml:"apply-worker-count"`
	SendRaftMsgWorkerCount uint64 `toml:"send-raft-msg-worker-count"`
	RaftMaxWorkers         uint64 `toml:"raft-max-workers"`
}

func (c *WorkerConfig) adjust() {
	if c.ApplyWorkerCount == 0 {
		c.ApplyWorkerCount = defaultApplyWorkerCount
	}

	if c.SendRaftMsgWorkerCount == 0 {
		c.SendRaftMsgWorkerCount = defaultSendRaftMsgWorkerCount
	}

	if c.RaftMaxWorkers == 0 {
		c.RaftMaxWorkers = defaultRaftMaxWorkers
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
	// EnablePreVote minimizes disruption when a partitioned node rejoins the cluster by using a two phase election
	EnablePreVote bool `toml:"enable-pre-vote"`
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

	SendRaftBatchSize uint64 `toml:"send-raft-batch-size"`
	MaxProposalBytes  typeutil.ByteSize

	RaftLog RaftLogConfig `toml:"raft-log"`
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

	if c.MaxProposalBytes == 0 {
		c.MaxProposalBytes = typeutil.ByteSize(defaultMaxProposalBytes)
	}

	(&c.RaftLog).adjust(shardCapacityBytes)
}

// RaftLogConfig raft log config
type RaftLogConfig struct {
	DisableSyncRaftLog            bool
	RaftLogCompactDuration        typeutil.Duration
	MaxAllowTransferLogLag        uint64
	RaftThresholdCompactLog       uint64
	MaxRaftLogCountToForceCompact uint64
	MaxRaftLogBytesToForceCompact uint64
	MaxRaftLogCompactProtectLag   uint64
	DisableRaftLogCompactProtect  []uint64
}

func (c *RaftLogConfig) adjust(shardCapacityBytes uint64) {
	if c.RaftLogCompactDuration.Duration == 0 {
		c.RaftLogCompactDuration.Duration = defaultRaftLogCompactDuration
	}

	if c.MaxAllowTransferLogLag == 0 {
		c.MaxAllowTransferLogLag = defaultMaxAllowTransferLogLag
	}

	if c.RaftThresholdCompactLog == 0 {
		c.RaftThresholdCompactLog = defaultRaftThresholdCompactLog
	}

	if c.MaxRaftLogCountToForceCompact == 0 {
		c.MaxRaftLogCountToForceCompact = shardCapacityBytes * uint64(mb) * 3 / 4 / uint64(kb)
	}

	if c.MaxRaftLogBytesToForceCompact == 0 {
		c.MaxRaftLogBytesToForceCompact = shardCapacityBytes * 3 / 4
	}

	if c.MaxRaftLogCompactProtectLag == 0 {
		c.MaxRaftLogCompactProtectLag = shardCapacityBytes * uint64(mb) / 256 / 16
	}
}

// StorageConfig storage config
type StorageConfig struct {
	// MetaStorage used to store raft, shards and store's metadata
	MetaStorage storage.MetadataStorage
	// DataStorageFactory is a storage factory  to store application's data
	DataStorageFactory func(group uint64, shardID uint64) storage.DataStorage
	// DataMoveFunc move data from a storage to others
	DataMoveFunc func(bhmetapb.Shard, []bhmetapb.Shard) error
	// ForeachDataStorageFunc do in every storage
	ForeachDataStorageFunc func(cb func(storage.DataStorage))
}

// CustomizeConfig customize config
type CustomizeConfig struct {
	CustomShardStateAwareFactory func() aware.ShardStateAware
	CustomInitShardsFactory      func() []bhmetapb.Shard
	CustomSnapshotManagerFactory func() snapshot.SnapshotManager
	CustomTransportFactory       func() transport.Transport
	CustomSnapshotDataCreateFunc func(string, bhmetapb.Shard) error
	CustomSnapshotDataApplyFunc  func(string, bhmetapb.Shard) error
	CustomSplitCheckFunc         func(bhmetapb.Shard) (uint64, uint64, [][]byte, error)
	CustomSplitCompletedFunc     func(*bhmetapb.Shard, *bhmetapb.Shard)
	CustomCanReadLocalFunc       func(bhmetapb.Shard) bool
	CustomShardAddHandleFunc     func(bhmetapb.Shard) error
	CustomWriteBatchFunc         func(uint64) command.CommandWriteBatch
	CustomReadBatchFunc          func(uint64) command.CommandReadBatch
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
