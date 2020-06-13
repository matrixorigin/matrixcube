package raftstore

import (
	"path/filepath"
	"time"

	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/prophet"
	"github.com/fagongzi/log"
)

var (
	logger = log.NewLoggerWithPrefix("[beehive-raftstore]")
)

var (
	kb = 1024
	mb = 1024 * kb

	defaultGroups                       uint64 = 1
	defaultSendRaftBatchSize            uint64 = 64
	defaultInitShards                   uint64 = 1
	defaultMaxConcurrencySnapChunks     uint64 = 8
	defaultSnapChunkSize                       = 4 * mb
	defaultApplyWorkerCount             uint64 = 32
	defaultSendRaftMsgWorkerCount       uint64 = 8
	defaultRaftMaxWorkers               uint64 = 32
	defaultRaftElectionTick                    = 10
	defaultRaftHeartbeatTick                   = 2
	defaultRaftMaxBytesPerMsg                  = 4 * mb
	defaultRaftMaxInflightMsgCount             = 32
	defaultRaftLogCompactDuration              = time.Second * 30
	defaultShardSplitCheckDuration             = time.Second * 30
	defaultMaxProposalBytes                    = 8 * mb
	defaultShardCapacityBytes           uint64 = uint64(96 * mb)
	defaultMaxAllowTransferLogLag       uint64 = 2
	defaultRaftThresholdCompactLog      uint64 = 256
	defaultMaxConcurrencyWritesPerShard uint64 = 10000
	defaultRaftTickDuration                    = time.Second
	defaultMaxPeerDownTime                     = time.Minute * 30
	defaultShardHeartbeatDuration              = time.Second * 2
	defaultStoreHeartbeatDuration              = time.Second * 10
	defaultDataPath                            = "/tmp/beehive"
	defaultSnapshotDirName                     = "snapshots"
	defaultProphetDirName                      = "prophet"
)

// Option options
type Option func(*options)

type options struct {
	dataPath                      string
	labels                        []metapb.Label
	locationLabels                []string
	snapshotDirName               string
	initShards                    uint64
	sendRaftBatchSize             uint64
	maxConcurrencySnapChunks      uint64
	snapChunkSize                 int
	applyWorkerCount              uint64
	sendRaftMsgWorkerCount        uint64
	maxPeerDownTime               time.Duration
	shardHeartbeatDuration        time.Duration
	storeHeartbeatDuration        time.Duration
	useMemoryAsStorage            bool
	raftMaxWorkers                uint64
	raftPreVote                   bool
	raftTickDuration              time.Duration
	raftElectionTick              int
	raftHeartbeatTick             int
	raftMaxBytesPerMsg            uint64
	raftMaxInflightMsgCount       int
	raftLogCompactDuration        time.Duration
	shardSplitCheckDuration       time.Duration
	disableShardSplit             bool
	disableSyncRaftLog            bool
	maxProposalBytes              int
	maxAllowTransferLogLag        uint64
	raftThresholdCompactLog       uint64
	maxRaftLogCountToForceCompact uint64
	maxRaftLogBytesToForceCompact uint64
	maxRaftLogCompactProtectLag   uint64
	disableRaftLogCompactProtect  []uint64
	shardCapacityBytes            uint64
	shardSplitCheckBytes          uint64
	maxConcurrencyWritesPerShard  uint64
	ensureNewShardInterval        time.Duration
	snapshotManager               SnapshotManager
	trans                         Transport
	rpc                           RPC
	writeBatchFunc                func() CommandWriteBatch
	readBatchFunc                 func() CommandReadBatch
	shardStateAware               ShardStateAware
	shardAddHandleFunc            func(metapb.Shard) error
	shardAllowRebalanceFunc       func(metapb.Shard) bool
	shardAllowTransferLeaderFunc  func(metapb.Shard) bool
	groups                        uint64

	customInitShardCreateFunc    func() []metapb.Shard
	customSnapshotDataCreateFunc func(string, metapb.Shard) error
	customSnapshotDataApplyFunc  func(string, metapb.Shard) error
	customSplitCheckFunc         func(metapb.Shard) ([]byte, bool)
	customSplitCompletedFunc     func(*metapb.Shard, *metapb.Shard)
	customCanReadLocalFunc       func(metapb.Shard) bool

	prophetOptions []prophet.Option
}

func (opts *options) adjust() {
	if opts.sendRaftBatchSize == 0 {
		opts.sendRaftBatchSize = defaultSendRaftBatchSize
	}

	if opts.dataPath == "" {
		opts.dataPath = defaultDataPath
	}

	if opts.snapshotDirName == "" {
		opts.snapshotDirName = defaultSnapshotDirName
	}

	if opts.initShards == 0 {
		opts.initShards = defaultInitShards
	}

	if opts.maxConcurrencySnapChunks == 0 {
		opts.maxConcurrencySnapChunks = defaultMaxConcurrencySnapChunks
	}

	if opts.snapChunkSize == 0 {
		opts.snapChunkSize = defaultSnapChunkSize
	}

	if opts.applyWorkerCount == 0 {
		opts.applyWorkerCount = defaultApplyWorkerCount
	}

	if opts.sendRaftMsgWorkerCount == 0 {
		opts.sendRaftMsgWorkerCount = defaultSendRaftMsgWorkerCount
	}

	if opts.maxPeerDownTime == 0 {
		opts.maxPeerDownTime = defaultMaxPeerDownTime
	}

	if opts.storeHeartbeatDuration == 0 {
		opts.storeHeartbeatDuration = defaultStoreHeartbeatDuration
	}

	if opts.shardHeartbeatDuration == 0 {
		opts.shardHeartbeatDuration = defaultShardHeartbeatDuration
	}

	if opts.raftMaxWorkers == 0 {
		opts.raftMaxWorkers = defaultRaftMaxWorkers
	}

	if opts.raftElectionTick == 0 {
		opts.raftElectionTick = defaultRaftElectionTick
	}

	if opts.raftHeartbeatTick == 0 {
		opts.raftHeartbeatTick = defaultRaftHeartbeatTick
	}

	if opts.raftMaxInflightMsgCount == 0 {
		opts.raftMaxInflightMsgCount = defaultRaftMaxInflightMsgCount
	}

	if opts.raftTickDuration == 0 {
		opts.raftTickDuration = defaultRaftTickDuration
	}

	if opts.raftLogCompactDuration == 0 {
		opts.raftLogCompactDuration = defaultRaftLogCompactDuration
	}

	if opts.shardSplitCheckDuration == 0 {
		opts.shardSplitCheckDuration = defaultShardSplitCheckDuration
	}

	if opts.shardCapacityBytes == 0 {
		opts.shardCapacityBytes = defaultShardCapacityBytes
	}

	if opts.shardSplitCheckBytes == 0 {
		opts.shardSplitCheckBytes = opts.shardCapacityBytes * 80 / 100
	}

	if opts.ensureNewShardInterval == 0 {
		opts.ensureNewShardInterval = time.Second * 10
	}

	if opts.maxRaftLogCountToForceCompact == 0 {
		opts.maxRaftLogCountToForceCompact = opts.shardCapacityBytes * uint64(mb) * 3 / 4 / uint64(kb)
	}

	if opts.maxRaftLogBytesToForceCompact == 0 {
		opts.maxRaftLogBytesToForceCompact = opts.shardCapacityBytes * 3 / 4
	}

	if opts.maxRaftLogCompactProtectLag == 0 {
		opts.maxRaftLogCompactProtectLag = opts.shardCapacityBytes * uint64(mb) / 256 / 16
	}

	if opts.maxProposalBytes == 0 {
		opts.maxProposalBytes = defaultMaxProposalBytes
	}

	if opts.maxAllowTransferLogLag == 0 {
		opts.maxAllowTransferLogLag = defaultMaxAllowTransferLogLag
	}

	if opts.raftThresholdCompactLog == 0 {
		opts.raftThresholdCompactLog = defaultRaftThresholdCompactLog
	}

	if opts.maxConcurrencyWritesPerShard == 0 {
		opts.maxConcurrencyWritesPerShard = defaultMaxConcurrencyWritesPerShard
	}

	if opts.groups == 0 {
		opts.groups = defaultGroups
	}
}

func (opts *options) snapshotDir() string {
	return filepath.Join(opts.dataPath, opts.snapshotDirName)
}

func (opts *options) prophetDir() string {
	return filepath.Join(opts.dataPath, defaultProphetDirName)
}

// WithDataPath set the path to store the raft log, snapshots and other metadata
func WithDataPath(value string) Option {
	return func(opts *options) {
		opts.dataPath = value
	}
}

// WithSendRaftBatchSize how many raft messages in a batch to send to other node
func WithSendRaftBatchSize(value uint64) Option {
	return func(opts *options) {
		opts.sendRaftBatchSize = value
	}
}

// WithMaxProposalBytes the maximum bytes per proposal, if exceeded this value, application will
// receive `RaftEntryTooLarge` error
func WithMaxProposalBytes(value int) Option {
	return func(opts *options) {
		opts.maxProposalBytes = value
	}
}

// WithSnapshotLimit limit the speed and size of snapshots to transfer,
// to avoid taking up too much bandwidth, the snapshot is split into a number of chunks,
// the `maxConcurrencySnapChunks` controls the number of chunks sent concurrently,
// the `snapChunkSize` controls the bytes of each chunk
func WithSnapshotLimit(maxConcurrencySnapChunks uint64, snapChunkSize int) Option {
	return func(opts *options) {
		opts.maxConcurrencySnapChunks = maxConcurrencySnapChunks
		opts.snapChunkSize = snapChunkSize
	}
}

// WithApplyWorkerCount goroutine number of apply raft log, for performance reasons,
// the raft log apply is executed asynchronously, so the system uses a fixed-size coroutine
// pool to apply the raft log of all the shards.
func WithApplyWorkerCount(value uint64) Option {
	return func(opts *options) {
		opts.applyWorkerCount = value
	}
}

// WithSendRaftMsgWorkerCount goroutine number of send raft message,
// the system sends the raft message to the corresponding goroutine according to the shard ID,
// each goroutine is responsible for a group of stores, only one tcp connection between two stores.
func WithSendRaftMsgWorkerCount(value uint64) Option {
	return func(opts *options) {
		opts.sendRaftMsgWorkerCount = value
	}
}

// WithMaxPeerDownTime In all replicas of a shard, when any message that does not receive
// a replica is exceeded this value, the system will remove the replica and the scheduler
// will choose a new store to create a new replica.
func WithMaxPeerDownTime(value time.Duration) Option {
	return func(opts *options) {
		opts.maxPeerDownTime = value
	}
}

// WithShardHeartbeatDuration reporting the shard information to the scheduler's heartbeat time.
func WithShardHeartbeatDuration(value time.Duration) Option {
	return func(opts *options) {
		opts.shardHeartbeatDuration = value
	}
}

// WithStoreHeartbeatDuration reporting the store information to the scheduler's heartbeat time.
func WithStoreHeartbeatDuration(value time.Duration) Option {
	return func(opts *options) {
		opts.storeHeartbeatDuration = value
	}
}

// WithMemoryAsStorage use memory to store the Application's KV data, the scheduler collects the
// memory usage of the store node and balances the shards in the cluster, otherwise use disk.
func WithMemoryAsStorage() Option {
	return func(opts *options) {
		opts.useMemoryAsStorage = true
	}
}

// WithRaftMaxWorkers set raft workers
func WithRaftMaxWorkers(value uint64) Option {
	return func(opts *options) {
		opts.raftMaxWorkers = value
	}
}

// WithRaftTickDuration raft tick time interval.
func WithRaftTickDuration(value time.Duration) Option {
	return func(opts *options) {
		opts.raftTickDuration = value
	}
}

// WithRaftElectionTick how many ticks to perform timeout elections.
func WithRaftElectionTick(value int) Option {
	return func(opts *options) {
		opts.raftElectionTick = value
	}
}

// WithRaftHeartbeatTick how many ticks to perform raft headrtbeat.
func WithRaftHeartbeatTick(value int) Option {
	return func(opts *options) {
		opts.raftHeartbeatTick = value
	}
}

// WithRaftMaxBytesPerMsg the maximum bytes per raft message.
func WithRaftMaxBytesPerMsg(value uint64) Option {
	return func(opts *options) {
		opts.raftMaxBytesPerMsg = value
	}
}

// WithRaftMaxInflightMsgCount the maximum inflight messages in raft append RPC.
func WithRaftMaxInflightMsgCount(value int) Option {
	return func(opts *options) {
		opts.raftMaxInflightMsgCount = value
	}
}

// WithRaftLogCompactDuration compact raft log time interval.
func WithRaftLogCompactDuration(value time.Duration) Option {
	return func(opts *options) {
		opts.raftLogCompactDuration = value
	}
}

// WithShardSplitCheckDuration check the interval of shard split
func WithShardSplitCheckDuration(value time.Duration) Option {
	return func(opts *options) {
		opts.shardSplitCheckDuration = value
	}
}

// WithDisableShardSplit disable split the shard, by default, the system checks all shards,
// when these shards are found to exceed the maximum storage threshold, they will perform split.
func WithDisableShardSplit() Option {
	return func(opts *options) {
		opts.disableShardSplit = true
	}
}

// WithDisableSyncRaftLog disable sync operations every time you write raft log to disk.
func WithDisableSyncRaftLog() Option {
	return func(opts *options) {
		opts.disableSyncRaftLog = true
	}
}

// WithMaxAllowTransferLogLag If the number of logs of the follower node behind the leader node exceeds
// this value, the transfer leader will not be accepted to this node.
func WithMaxAllowTransferLogLag(value uint64) Option {
	return func(opts *options) {
		opts.maxAllowTransferLogLag = value
	}
}

// WithRaftThresholdCompactLog The leader node periodically compact the replicates the logs
// that have been copied to the follower node, to prevent this operation from being too frequent,
// limit the number of raft logs that will be compacted.
func WithRaftThresholdCompactLog(value uint64) Option {
	return func(opts *options) {
		opts.raftThresholdCompactLog = value
	}
}

// WithMaxRaftLogCountToForceCompact the maximum number of raft logs that the leader node has been applied,
// if exceeded this value, the leader node will force compact log to last applied raft log index, so the
// follower node may receive a snapshot.
func WithMaxRaftLogCountToForceCompact(value uint64) Option {
	return func(opts *options) {
		opts.maxRaftLogCountToForceCompact = value
	}
}

// WithMaxRaftLogBytesToForceCompact the maximum bytes of raft logs that the leader node has been applied,
// if exceeded this value, the leader node will force compact log to last applied raft log index, so the
// follower node may receive a snapshot.
func WithMaxRaftLogBytesToForceCompact(value uint64) Option {
	return func(opts *options) {
		opts.maxRaftLogBytesToForceCompact = value
	}
}

// WithMaxRaftLogCompactProtectLag If the leader node triggers the force compact raft log, the compact index
// is the last applied raft log index of leader node, to avoid sending snapshots to a smaller delayed follower
// in the future, set a protected value.
func WithMaxRaftLogCompactProtectLag(value uint64) Option {
	return func(opts *options) {
		opts.maxRaftLogCompactProtectLag = value
	}
}

// WithShardCapacityBytes the maximum bytes per shard
func WithShardCapacityBytes(value uint64) Option {
	return func(opts *options) {
		opts.shardCapacityBytes = value
	}
}

// WithShardSplitCheckBytes if the shard storaged size exceeded this value, the split check will started
func WithShardSplitCheckBytes(value uint64) Option {
	return func(opts *options) {
		opts.shardSplitCheckBytes = value
	}
}

// WithSnapshotManager set the snapshot manager interface, this will used for create, apply, write, received
// and clean snapshots
func WithSnapshotManager(value SnapshotManager) Option {
	return func(opts *options) {
		opts.snapshotManager = value
	}
}

// WithTransport set the transport to send, receive raft message and snapshot message.
func WithTransport(value Transport) Option {
	return func(opts *options) {
		opts.trans = value
	}
}

// WithWriteBatchFunc the factory function to create applciation commands batch processor. By default
// the raftstore will process command one by one.
func WithWriteBatchFunc(value func() CommandWriteBatch) Option {
	return func(opts *options) {
		opts.writeBatchFunc = value
	}
}

// WithReadBatchFunc the factory function to create applciation commands batch processor. By default
// the raftstore will process command one by one.
func WithReadBatchFunc(value func() CommandReadBatch) Option {
	return func(opts *options) {
		opts.readBatchFunc = value
	}
}

// WithInitShards how many shards will be created on bootstrap the cluster
func WithInitShards(value uint64) Option {
	return func(opts *options) {
		opts.initShards = value
	}
}

// WithLabels Give the current node a set of labels, and specify which label names are used to identify
// the location. The scheduler will create a replicas of the shard in a different location based on these
// location labels to achieve high availability.
func WithLabels(locationLabel []string, labels []metapb.Label) Option {
	return func(opts *options) {
		opts.locationLabels = locationLabel
		opts.labels = labels
	}
}

// WithRPC set the rpc implemention to serve request and sent response
func WithRPC(value RPC) Option {
	return func(opts *options) {
		opts.rpc = value
	}
}

// WithProphetOptions set prophet options
func WithProphetOptions(value ...prophet.Option) Option {
	return func(opts *options) {
		opts.prophetOptions = append(opts.prophetOptions, value...)
	}
}

// WithShardStateAware set shard state aware, the application will received shard event like:
// create, destory, become leader, become follower and so on.
func WithShardStateAware(value ShardStateAware) Option {
	return func(opts *options) {
		opts.shardStateAware = value
	}
}

// WithShardAddHandleFun set shard added handle func
func WithShardAddHandleFun(value func(metapb.Shard) error) Option {
	return func(opts *options) {
		opts.shardAddHandleFunc = value
	}
}

// WithEnsureNewShardInterval set ensureNewShardInterval
func WithEnsureNewShardInterval(value time.Duration) Option {
	return func(opts *options) {
		opts.ensureNewShardInterval = value
	}
}

// WithMaxConcurrencyWritesPerShard limit the write speed per shard
func WithMaxConcurrencyWritesPerShard(maxConcurrencyWritesPerShard uint64) Option {
	return func(opts *options) {
		opts.maxConcurrencyWritesPerShard = maxConcurrencyWritesPerShard
	}
}

// WithSchedulerFunc set the scheduler contorl func
func WithSchedulerFunc(shardAllowRebalanceFunc, shardAllowTransferLeaderFunc func(metapb.Shard) bool) Option {
	return func(opts *options) {
		opts.shardAllowRebalanceFunc = shardAllowRebalanceFunc
		opts.shardAllowTransferLeaderFunc = shardAllowTransferLeaderFunc
	}
}

// WithGroups set group count
func WithGroups(value uint64) Option {
	return func(opts *options) {
		opts.groups = value
	}
}

// WithRaftPreVote enable raft pre-vote
func WithRaftPreVote(value bool) Option {
	return func(opts *options) {
		opts.raftPreVote = value
	}
}

// WithDisableRaftLogCompactProtect set disable compact protect
func WithDisableRaftLogCompactProtect(groups ...uint64) Option {
	return func(opts *options) {
		opts.disableRaftLogCompactProtect = append(opts.disableRaftLogCompactProtect, groups...)
	}
}

// WithCustomInitShardCreateFunc set custom initShardCreateFunc
func WithCustomInitShardCreateFunc(value func() []metapb.Shard) Option {
	return func(opts *options) {
		opts.customInitShardCreateFunc = value
	}
}

// WithCustomSnapshotDataFunc set custom snapshot func
func WithCustomSnapshotDataFunc(createFunc, applyFunc func(string, metapb.Shard) error) Option {
	return func(opts *options) {
		opts.customSnapshotDataCreateFunc = createFunc
		opts.customSnapshotDataApplyFunc = applyFunc
	}
}

// WithCustomSplitCheckFunc set customSplitCheckFunc
func WithCustomSplitCheckFunc(value func(metapb.Shard) ([]byte, bool)) Option {
	return func(opts *options) {
		opts.customSplitCheckFunc = value
	}
}

// WithCustomSplitCompletedFunc set customSplitCompletedFunc
func WithCustomSplitCompletedFunc(value func(*metapb.Shard, *metapb.Shard)) Option {
	return func(opts *options) {
		opts.customSplitCompletedFunc = value
	}
}

// WithCustomCanReadLocalFunc set customCanReadLocalFunc
func WithCustomCanReadLocalFunc(value func(metapb.Shard) bool) Option {
	return func(opts *options) {
		opts.customCanReadLocalFunc = value
	}
}
