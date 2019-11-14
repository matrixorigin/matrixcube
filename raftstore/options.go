package raftstore

import (
	"fmt"
	"time"

	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/fagongzi/log"
)

var (
	logger = log.NewLoggerWithPrefix("[beehive-raftstore]")
)

var (
	kb = 1024
	mb = 1024 * kb

	defaultSendRaftBatchSize          uint64 = 64
	defaultProposalBatchSize          uint64 = 64
	defaultInitShards                 uint64 = 1
	defaultMaxConcurrencySnapChunks          = 8
	defaultSnapChunkSize                     = 4 * kb
	defaultApplyWorkerCount           uint64 = 32
	defaultSendRaftMsgWorkerCount            = 8
	defaultSendSnapshotMsgWorkerCount        = 4
	defaultRaftElectionTick                  = 10
	defaultRaftHeartbeatTick                 = 2
	defaultRaftMaxBytesPerMsg                = 4 * mb
	defaultRaftMaxInflightMsgCount           = 32
	defaultRaftLogCompactDuration            = time.Second * 30
	defaultShardSplitCheckDuration           = time.Second * 30
	defaultMaxProposalBytes                  = int64(8 * mb)
	defaultShardCapacityBytes         uint64 = uint64(96 * mb)
	defaultMaxAllowTransferLogLag     uint64 = 5
	defaultRaftThresholdCompactLog    uint64 = 128
	defaultRaftTickDuration                  = time.Second
	defaultMaxPeerDownTime                   = time.Minute * 30
	defaultShardHeartbeatDuration            = time.Second * 2
	defaultStoreHeartbeatDuration            = time.Second * 10
	defaultDataPath                          = "/tmp/beehive"
	defaultSnapshotDirName                   = "snapshots"
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
	proposalBatchSize             uint64
	maxConcurrencySnapChunks      int
	snapChunkSize                 int
	applyWorkerCount              uint64
	sendRaftMsgWorkerCount        int
	sendSnapshotMsgWorkerCount    int
	maxPeerDownTime               time.Duration
	shardHeartbeatDuration        time.Duration
	storeHeartbeatDuration        time.Duration
	useMemoryAsStorage            bool
	raftTickDuration              time.Duration
	raftElectionTick              int
	raftHeartbeatTick             int
	raftMaxBytesPerMsg            uint64
	raftMaxInflightMsgCount       int
	raftLogCompactDuration        time.Duration
	shardSplitCheckDuration       time.Duration
	disableShardSplit             bool
	disableSyncRaftLog            bool
	maxProposalBytes              int64
	maxAllowTransferLogLag        uint64
	raftThresholdCompactLog       uint64
	maxRaftLogCountToForceCompact uint64
	maxRaftLogBytesToForceCompact uint64
	maxRaftLogCompactProtectLag   uint64
	shardCapacityBytes            uint64
	shardSplitCheckBytes          uint64
	snapshotManager               SnapshotManager
	trans                         Transport
	rpc                           RPC
	cleanDataFunc                 func(uint64) error
	writeBatchFunc                func() CommandWriteBatch
}

func (opts *options) adjust() {
	if opts.sendRaftBatchSize == 0 {
		opts.sendRaftBatchSize = defaultSendRaftBatchSize
	}

	if opts.proposalBatchSize == 0 {
		opts.sendRaftBatchSize = defaultProposalBatchSize
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

	if opts.sendSnapshotMsgWorkerCount == 0 {
		opts.sendSnapshotMsgWorkerCount = defaultSendSnapshotMsgWorkerCount
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
}

func (opts *options) snapshotDir() string {
	return fmt.Sprintf("%s/%s", opts.dataPath, opts.snapshotDirName)
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

// WithProposalBatchSize how many request commands in a raft proposal
func WithProposalBatchSize(value uint64) Option {
	return func(opts *options) {
		opts.proposalBatchSize = value
	}
}

// WithMaxProposalBytes the maximum bytes per proposal, if exceeded this value, application will
// receive `RaftEntryTooLarge` error
func WithMaxProposalBytes(value int64) Option {
	return func(opts *options) {
		opts.maxProposalBytes = value
	}
}

// WithSnapshotLimit limit the speed and size of snapshots to transfer,
// to avoid taking up too much bandwidth, the snapshot is split into a number of chunks,
// the `maxConcurrencySnapChunks` controls the number of chunks sent concurrently,
// the `snapChunkSize` controls the bytes of each chunk
func WithSnapshotLimit(maxConcurrencySnapChunks, snapChunkSize int) Option {
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
func WithSendRaftMsgWorkerCount(value int) Option {
	return func(opts *options) {
		opts.sendRaftMsgWorkerCount = value
	}
}

// WithSendSnapshotMsgWorkerCount goroutine number of send snapshots,
// the system sends the snapshots to the corresponding goroutine according to the shard ID,
// each goroutine is responsible for a group of stores, only one tcp connection between two stores,
// Note, the TCP connection that send snapshots and send raft messages are isolated from each other.
func WithSendSnapshotMsgWorkerCount(value int) Option {
	return func(opts *options) {
		opts.sendSnapshotMsgWorkerCount = value
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
func WithMemoryAsStorage(value bool) Option {
	return func(opts *options) {
		opts.useMemoryAsStorage = value
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

// WithCleanDataFunc the handler funcation to clean the shard's data
func WithCleanDataFunc(value func(uint64) error) Option {
	return func(opts *options) {
		opts.cleanDataFunc = value
	}
}

// WithWriteBatchFunc the factory function to create applciation commands batch processor. By default
// the raftstore will process command one by one.
func WithWriteBatchFunc(value func() CommandWriteBatch) Option {
	return func(opts *options) {
		opts.writeBatchFunc = value
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
func WithRPC(rpc RPC) Option {
	return func(opts *options) {
		opts.rpc = rpc
	}
}
