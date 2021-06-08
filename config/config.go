package config

import (
	"log"
	"path"
	"time"

	"github.com/matrixorigin/matrixcube/aware"
	pconfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
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
	defaultCompactDuration                 = time.Second * 30
	defaultShardSplitCheckDuration         = time.Second * 30
	defaultShardStateCheckDuration         = time.Second * 60
	defaultMaxEntryBytes                   = 10 * mb
	defaultShardCapacityBytes       uint64 = uint64(96 * mb)
	defaultMaxAllowTransferLag      uint64 = 2
	defaultCompactThreshold         uint64 = 256
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
	// Worker worker config
	Worker WorkerConfig `toml:"worker"`
	// Prophet prophet config
	Prophet pconfig.Config `toml:"prophet"`
	// Metric Config
	Metric metric.Cfg `toml:"metric"`
	// Storage config
	Storage StorageConfig
	// Worker config
	// Customize config
	Customize CustomizeConfig
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
	ShardStateCheckDuration typeutil.Duration `toml:"shard-state-check-duration"`
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
	ApplyWorkerCount       uint64 `toml:"raft-apply-worker"`
	SendRaftMsgWorkerCount uint64 `toml:"raft-msg-worker"`
	RaftEventWorkers       uint64 `toml:"raft-event-workers"`
}

func (c *WorkerConfig) adjust() {
	if c.ApplyWorkerCount == 0 {
		c.ApplyWorkerCount = defaultApplyWorkerCount
	}

	if c.SendRaftMsgWorkerCount == 0 {
		c.SendRaftMsgWorkerCount = defaultSendRaftMsgWorkerCount
	}

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
	// SendRaftBatchSize raft message sender count
	SendRaftBatchSize uint64 `toml:"send-raft-batch-size"`
	// RaftLog raft log 配置
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

	if c.MaxEntryBytes == 0 {
		c.MaxEntryBytes = typeutil.ByteSize(defaultMaxEntryBytes)
	}

	(&c.RaftLog).adjust(shardCapacityBytes)
}

// RaftLogConfig raft log config
type RaftLogConfig struct {
	DisableSync           bool              `toml:"disable-sync"`
	CompactDuration       typeutil.Duration `toml:"compact-duration"`
	CompactThreshold      uint64            `toml:"compact-threshold"`
	DisableCompactProtect []uint64          `toml:"disable-compact-protect"`
	MaxAllowTransferLag   uint64            `toml:"max-allow-transfer-lag"`
	ForceCompactCount     uint64
	ForceCompactBytes     uint64
	CompactProtectLag     uint64
}

func (c *RaftLogConfig) adjust(shardCapacityBytes uint64) {
	if c.CompactDuration.Duration == 0 {
		c.CompactDuration.Duration = defaultCompactDuration
	}

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

	if c.CompactProtectLag == 0 {
		c.CompactProtectLag = shardCapacityBytes * uint64(mb) / 256 / 16
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
	// CustomShardStateAwareFactory is a factory func to create aware.ShardStateAware to handled shard life cycle.
	CustomShardStateAwareFactory func() aware.ShardStateAware
	// CustomInitShardsFactory is a factory func to provide init shards to cube to bootstrap the cluster.
	CustomInitShardsFactory func() []bhmetapb.Shard
	// CustomSnapshotManagerFactory is a factory func to create a snapshot.SnapshotManager to handle snapshot by youself.
	CustomSnapshotManagerFactory func() snapshot.SnapshotManager
	// CustomTransportFactory is a factory func to create a transport.Transport to handle raft rpc by youself.
	CustomTransportFactory func() transport.Transport
	// CustomSnapshotDataCreateFuncFactory is factory create a func which called by cube if a snapshot need to create.
	CustomSnapshotDataCreateFuncFactory func(group uint64) func(dataPath string, shard bhmetapb.Shard) error
	// CustomSnapshotDataApplyFuncFactory is factory create a func which called by cube if a snapshot need to apply.
	CustomSnapshotDataApplyFuncFactory func(group uint64) func(dataPath string, shard bhmetapb.Shard) error
	// CustomSplitCheckFuncFactory  is factory create a func which called periodically by cube to check if the shard needs to be split
	CustomSplitCheckFuncFactory func(group uint64) func(bhmetapb.Shard) (totalSize uint64, totalKeys uint64, splitKeys [][]byte, err error)
	// CustomSplitCompletedFuncFactory  is factory create a func which called by cube when the split operation of the shard is completed.
	// We can update the attributes of old and news shards in this func
	CustomSplitCompletedFuncFactory func(group uint64) func(old *bhmetapb.Shard, news []bhmetapb.Shard)
	// CustomCanReadLocalFunc returns true if the shard can read without raft
	CustomCanReadLocalFunc func(bhmetapb.Shard) bool
	// CustomAdjustCompactFunc is factory create a func which used to adjust raft log compactIdx
	CustomAdjustCompactFuncFactory func(group uint64) func(shard bhmetapb.Shard, compactIndex uint64) (newCompactIdx uint64, err error)
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
