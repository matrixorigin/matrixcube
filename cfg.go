package beehive

import (
	"errors"
	"flag"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/coreos/etcd/clientv3"
	"github.com/deepfabric/beehive/metric"
	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/raftstore"
	"github.com/deepfabric/beehive/storage"
	"github.com/deepfabric/prophet"
	"github.com/fagongzi/util/format"
)

var (
	file = flag.String("beehive-cfg", "./beehive.toml", "The beehive configuration file")
)

type cfg struct {
	Name                          string     `toml:"name"`
	RaftAddr                      string     `toml:"raftAddr"`
	RPCAddr                       string     `toml:"rpcAddr"`
	DataPath                      string     `toml:"dataPath"`
	Labels                        [][]string `toml:"labels"`
	LocationLabels                []string   `toml:"locationLabels"`
	InitShards                    uint64     `toml:"initShards"`
	Groups                        uint64     `toml:"groups"`
	ShardCapacityBytes            uint64     `toml:"shardCapacityBytes"`
	ShardSplitCheckDuration       int64      `toml:"shardSplitCheckDuration"`
	ApplyWorkerCount              uint64     `toml:"applyWorkerCount"`
	SendRaftMsgWorkerCount        uint64     `toml:"sendRaftMsgWorkerCount"`
	DisableShardSplit             bool       `toml:"disableShardSplit"`
	DisableSyncRaftLog            bool       `toml:"disableSyncRaftLog"`
	UseMemoryAsStorage            bool       `toml:"useMemoryAsStorage"`
	SendRaftBatchSize             uint64     `toml:"sendRaftBatchSize"`
	MaxProposalBytes              int        `toml:"maxProposalBytes"`
	MaxConcurrencyWritesPerShard  uint64     `toml:"maxConcurrencyWritesPerShard"`
	MaxConcurrencySnapChunks      uint64     `toml:"maxConcurrencySnapChunks"`
	MaxRaftLogCountToForceCompact uint64     `toml:"maxRaftLogCountToForceCompact"`
	MaxRaftLogBytesToForceCompact uint64     `toml:"maxRaftLogBytesToForceCompact"`
	MaxRaftLogCompactProtectLag   uint64     `toml:"maxRaftLogCompactProtectLag"`
	SnapChunkSize                 int        `toml:"snapChunkSize"`
	MaxPeerDownTime               int64      `toml:"maxPeerDownTime"`
	ShardHeartbeatDuration        int64      `toml:"shardHeartbeatDuration"`
	StoreHeartbeatDuration        int64      `toml:"storeHeartbeatDuration"`
	MaxAllowTransferLogLag        uint64     `toml:"maxAllowTransferLogLag"`
	RaftMaxWorkers                uint64     `toml:"raftMaxWorkers"`
	RaftTickDuration              int64      `toml:"raftTickDuration"`
	RaftPreVote                   bool       `toml:"raftPreVote"`
	RaftElectionTick              int        `toml:"raftElectionTick"`
	RaftHeartbeatTick             int        `toml:"raftHeartbeatTick"`
	RaftMaxBytesPerMsg            uint64     `toml:"raftMaxBytesPerMsg"`
	RaftMaxInflightMsgCount       int        `toml:"raftMaxInflightMsgCount"`
	RaftLogCompactDuration        int64      `toml:"raftLogCompactDuration"`
	RaftThresholdCompactLog       uint64     `toml:"raftThresholdCompactLog"`

	Prophet ProphetCfg `toml:"prophet"`
	Metric  metric.Cfg `toml:"metric"`
}

// ProphetCfg prophet cfg
type ProphetCfg struct {
	RPCAddr                     string   `toml:"rpcAddr"`
	StoreMetadata               bool     `toml:"storeMetadata"`
	ClientAddr                  string   `toml:"clientAddr"`
	PeerAddr                    string   `toml:"peerAddr"`
	Seed                        string   `toml:"seed"`
	Clusters                    []string `toml:"clusters"`
	LeaderLeaseTTL              int64    `toml:"leaderLeaseTTL"`
	MaxRPCCons                  int      `toml:"maxRPCCons"`
	MaxRPCConnIdle              int64    `toml:"maxRPCConnIdle"`
	MaxRPCTimeout               int64    `toml:"maxRPCTimeout"`
	CountResourceReplicas       int      `toml:"countResourceReplicas"`
	MaxScheduleRetries          int      `toml:"maxScheduleRetries"`
	MaxScheduleInterval         int64    `toml:"maxScheduleInterval"`
	MinScheduleInterval         int64    `toml:"minScheduleInterval"`
	TimeoutWaitOperatorComplete int64    `toml:"timeoutWaitOperatorComplete"`
	MaxFreezeScheduleInterval   int64    `toml:"maxFreezeScheduleInterval"`
	MaxRebalanceLeader          uint64   `toml:"maxRebalanceLeader"`
	MaxRebalanceReplica         uint64   `toml:"maxRebalanceReplica"`
	MaxScheduleReplica          uint64   `toml:"maxScheduleReplica"`
	MaxLimitSnapshotsCount      uint64   `toml:"maxLimitSnapshotsCount"`
	MinAvailableStorageUsedRate int      `toml:"minAvailableStorageUsedRate"`
}

func (c *cfg) getLabels() []metapb.Label {
	var labels []metapb.Label
	for _, kv := range c.Labels {
		labels = append(labels, metapb.Label{
			Key:   kv[0],
			Value: kv[1],
		})
	}

	return labels
}

// CreateRaftStoreFromFile create raftstore from a toml configuration file.
func CreateRaftStoreFromFile(dataPath string,
	metadataStorage storage.MetadataStorage,
	dataStorages []storage.DataStorage,
	opts ...raftstore.Option) (raftstore.Store, error) {
	if !flag.Parsed() {
		flag.Parse()
	}

	c := cfg{}
	_, err := toml.DecodeFile(*file, &c)
	if err != nil {
		return nil, err
	}

	c.DataPath = dataPath
	if c.Name == "" {
		return nil, errors.New("name is required")
	}

	if c.RaftAddr == "" {
		return nil, errors.New("raft address is required")
	}

	if c.RPCAddr == "" {
		return nil, errors.New("rpc address is required")
	}

	opts = append(opts, raftstore.WithDataPath(c.DataPath))
	if len(c.Labels) != 0 {
		opts = append(opts, raftstore.WithLabels(c.LocationLabels, c.getLabels()))
	}

	if c.InitShards > 0 {
		opts = append(opts, raftstore.WithInitShards(c.InitShards))
	}

	if c.ShardCapacityBytes > 0 {
		opts = append(opts, raftstore.WithShardCapacityBytes(c.ShardCapacityBytes*1024*1024))
	}

	if c.ShardSplitCheckDuration > 0 {
		opts = append(opts, raftstore.WithShardSplitCheckDuration(time.Duration(c.ShardSplitCheckDuration)*time.Second))
	}

	if c.ApplyWorkerCount > 0 {
		opts = append(opts, raftstore.WithApplyWorkerCount(c.ApplyWorkerCount))
	}

	if c.SendRaftMsgWorkerCount > 0 {
		opts = append(opts, raftstore.WithSendRaftMsgWorkerCount(c.SendRaftMsgWorkerCount))
	}

	if c.DisableShardSplit {
		opts = append(opts, raftstore.WithDisableShardSplit())
	}

	if c.DisableSyncRaftLog {
		opts = append(opts, raftstore.WithDisableSyncRaftLog())
	}

	if c.UseMemoryAsStorage {
		opts = append(opts, raftstore.WithMemoryAsStorage())
	}

	if c.SendRaftBatchSize > 0 {
		opts = append(opts, raftstore.WithSendRaftBatchSize(c.SendRaftBatchSize))
	}

	if c.MaxProposalBytes > 0 {
		opts = append(opts, raftstore.WithMaxProposalBytes(c.MaxProposalBytes*1024*1024))
	}

	if c.MaxConcurrencySnapChunks > 0 && c.SnapChunkSize > 0 {
		opts = append(opts, raftstore.WithSnapshotLimit(c.MaxConcurrencySnapChunks, c.SnapChunkSize*1024*1024))
	}

	if c.MaxRaftLogCountToForceCompact > 0 {
		opts = append(opts, raftstore.WithMaxRaftLogCountToForceCompact(c.MaxRaftLogCountToForceCompact))
	}

	if c.MaxRaftLogBytesToForceCompact > 0 {
		opts = append(opts, raftstore.WithMaxRaftLogBytesToForceCompact(c.MaxRaftLogBytesToForceCompact*1024*1024))
	}

	if c.MaxRaftLogCompactProtectLag > 0 {
		opts = append(opts, raftstore.WithMaxRaftLogCompactProtectLag(c.MaxRaftLogCompactProtectLag))
	}

	if c.MaxPeerDownTime > 0 {
		opts = append(opts, raftstore.WithMaxPeerDownTime(time.Duration(c.MaxPeerDownTime)*time.Minute))
	}

	if c.ShardHeartbeatDuration > 0 {
		opts = append(opts, raftstore.WithShardHeartbeatDuration(time.Duration(c.ShardHeartbeatDuration)*time.Second))
	}

	if c.StoreHeartbeatDuration > 0 {
		opts = append(opts, raftstore.WithStoreHeartbeatDuration(time.Duration(c.StoreHeartbeatDuration)*time.Second))
	}

	if c.MaxAllowTransferLogLag > 0 {
		opts = append(opts, raftstore.WithMaxAllowTransferLogLag(c.MaxAllowTransferLogLag))
	}

	if c.RaftMaxWorkers > 0 {
		opts = append(opts, raftstore.WithRaftMaxWorkers(c.RaftMaxWorkers))
	}

	if c.RaftTickDuration > 0 {
		opts = append(opts, raftstore.WithRaftTickDuration(time.Duration(c.RaftTickDuration)*time.Millisecond))
	}

	if c.RaftElectionTick > 0 {
		opts = append(opts, raftstore.WithRaftElectionTick(c.RaftElectionTick))
	}

	if c.RaftHeartbeatTick > 0 {
		opts = append(opts, raftstore.WithRaftHeartbeatTick(c.RaftHeartbeatTick))
	}

	if c.RaftMaxBytesPerMsg > 0 {
		opts = append(opts, raftstore.WithRaftMaxBytesPerMsg(c.RaftMaxBytesPerMsg*1024*1024))
	}

	if c.RaftMaxInflightMsgCount > 0 {
		opts = append(opts, raftstore.WithRaftMaxInflightMsgCount(c.RaftMaxInflightMsgCount))
	}

	if c.RaftLogCompactDuration > 0 {
		opts = append(opts, raftstore.WithRaftLogCompactDuration(time.Duration(c.RaftLogCompactDuration)*time.Second))
	}

	if c.RaftThresholdCompactLog > 0 {
		opts = append(opts, raftstore.WithRaftThresholdCompactLog(c.RaftThresholdCompactLog))
	}

	if c.MaxConcurrencyWritesPerShard > 0 {
		opts = append(opts, raftstore.WithMaxConcurrencyWritesPerShard(c.MaxConcurrencyWritesPerShard))
	}

	if c.Groups > 0 {
		opts = append(opts, raftstore.WithGroups(c.Groups))
	}

	prophetOptions, err := getProphetOptions(&c)
	if err != nil {
		return nil, err
	}

	opts = append(opts, raftstore.WithRaftPreVote(c.RaftPreVote))
	opts = append(opts, raftstore.WithProphetOptions(prophetOptions...))

	metric.StartPush(c.Metric)
	return raftstore.NewStore(raftstore.Cfg{
		Name:            c.Name,
		RaftAddr:        c.RaftAddr,
		RPCAddr:         c.RPCAddr,
		MetadataStorage: metadataStorage,
		DataStorages:    dataStorages,
	}, opts...), nil
}

func getProphetOptions(c *cfg) ([]prophet.Option, error) {
	var opts []prophet.Option

	if c.Prophet.RPCAddr == "" {
		return nil, errors.New("[prophet].rpcAddr is required")
	}

	opts = append(opts, prophet.WithRPCAddr(c.Prophet.RPCAddr))
	if c.Prophet.LeaderLeaseTTL > 0 {
		opts = append(opts, prophet.WithLeaseTTL(c.Prophet.LeaderLeaseTTL))
	}

	if c.Prophet.MaxScheduleRetries > 0 {
		opts = append(opts, prophet.WithMaxScheduleRetries(c.Prophet.MaxScheduleRetries))
	}

	if c.Prophet.MaxScheduleInterval > 0 {
		opts = append(opts, prophet.WithMaxScheduleInterval(time.Second*time.Duration(c.Prophet.MaxScheduleInterval)))
	}

	if c.Prophet.MinScheduleInterval > 0 {
		opts = append(opts, prophet.WithMinScheduleInterval(time.Millisecond*time.Duration(c.Prophet.MinScheduleInterval)))
	}

	if c.Prophet.TimeoutWaitOperatorComplete > 0 {
		opts = append(opts, prophet.WithTimeoutWaitOperatorComplete(time.Minute*time.Duration(c.Prophet.TimeoutWaitOperatorComplete)))
	}

	if c.Prophet.MaxFreezeScheduleInterval > 0 {
		opts = append(opts, prophet.WithMaxFreezeScheduleInterval(time.Second*time.Duration(c.Prophet.MaxFreezeScheduleInterval)))
	}

	if c.MaxPeerDownTime > 0 {
		opts = append(opts, prophet.WithMaxAllowContainerDownDuration(time.Duration(c.MaxPeerDownTime)*time.Minute))
	}

	if c.Prophet.MaxRebalanceLeader > 0 {
		opts = append(opts, prophet.WithMaxRebalanceLeader(c.Prophet.MaxRebalanceLeader))
	}

	if c.Prophet.MaxRebalanceReplica > 0 {
		opts = append(opts, prophet.WithMaxRebalanceReplica(c.Prophet.MaxRebalanceReplica))
	}

	if c.Prophet.CountResourceReplicas > 0 {
		opts = append(opts, prophet.WithCountResourceReplicas(c.Prophet.CountResourceReplicas))
	}

	if c.Prophet.MaxScheduleReplica > 0 {
		opts = append(opts, prophet.WithMaxScheduleReplica(c.Prophet.MaxScheduleReplica))
	}

	if c.Prophet.MaxLimitSnapshotsCount > 0 {
		opts = append(opts, prophet.WithMaxLimitSnapshotsCount(c.Prophet.MaxLimitSnapshotsCount))
	}

	if c.Prophet.MinAvailableStorageUsedRate > 0 {
		opts = append(opts, prophet.WithMinAvailableStorageUsedRate(c.Prophet.MinAvailableStorageUsedRate))
	}

	if c.Prophet.MaxRPCCons > 0 {
		opts = append(opts, prophet.WithMaxRPCCons(c.Prophet.MaxRPCCons))
	}

	if c.Prophet.MaxRPCConnIdle > 0 {
		opts = append(opts, prophet.WithMaxRPCConnIdle(time.Second*time.Duration(c.Prophet.MaxRPCConnIdle)))
	}

	if c.Prophet.MaxRPCTimeout > 0 {
		opts = append(opts, prophet.WithMaxRPCTimeout(time.Second*time.Duration(c.Prophet.MaxRPCTimeout)))
	}

	clusterOptions, err := getCluster(c)
	if err != nil {
		return nil, err
	}

	opts = append(opts, clusterOptions...)
	return opts, nil
}

func getCluster(c *cfg) ([]prophet.Option, error) {
	var opts []prophet.Option

	if c.Prophet.StoreMetadata && c.Prophet.ClientAddr == "" {
		return nil, errors.New("[prophet].clientAddr is required while [prophet].storeMetadata is true")
	}

	if c.Prophet.StoreMetadata && c.Prophet.PeerAddr == "" {
		return nil, errors.New("[prophet].peerAddr is required while [prophet].storeMetadata is true")
	}

	if !c.Prophet.StoreMetadata && len(c.Prophet.Clusters) == 0 {
		return nil, errors.New("[prophet].clusters is required while [prophet].storeMetadata is false")
	}

	if c.Prophet.StoreMetadata {
		clientPort, err := getPort(c.Prophet.ClientAddr)
		if err != nil {
			return nil, err
		}

		peerPort, err := getPort(c.Prophet.PeerAddr)
		if err != nil {
			return nil, err
		}

		embedEtcdCfg := &prophet.EmbeddedEtcdCfg{}
		embedEtcdCfg.DataPath = filepath.Join(c.DataPath, "cluster")
		embedEtcdCfg.Name = c.Name
		embedEtcdCfg.URLsClient = fmt.Sprintf("http://0.0.0.0:%d", clientPort)
		embedEtcdCfg.URLsPeer = fmt.Sprintf("http://0.0.0.0:%d", peerPort)
		embedEtcdCfg.URLsAdvertiseClient = fmt.Sprintf("http://%s", c.Prophet.ClientAddr)
		embedEtcdCfg.URLsAdvertisePeer = fmt.Sprintf("http://%s", c.Prophet.PeerAddr)
		if c.Prophet.Seed != "" {
			embedEtcdCfg.Join = fmt.Sprintf("http://%s", c.Prophet.Seed)
		}

		opts = append(opts, prophet.WithEmbeddedEtcd(embedEtcdCfg))
	} else {
		var addrs []string
		for _, addr := range c.Prophet.Clusters {
			addrs = append(addrs, fmt.Sprintf("http://%s", addr))
		}

		client, err := clientv3.New(clientv3.Config{
			Endpoints:   addrs,
			DialTimeout: prophet.DefaultTimeout,
		})
		if err != nil {
			return nil, err
		}

		opts = append(opts, prophet.WithExternalEtcd(client))
	}

	return opts, nil
}

func getPort(addr string) (int, error) {
	ipAndPort := strings.Split(addr, ":")
	if len(ipAndPort) != 2 {
		return 0, fmt.Errorf("invalid ip and port address %s", ipAndPort)
	}

	if ipAndPort[0] == "" {
		return 0, fmt.Errorf("invalid ip and port address %s", ipAndPort)
	}

	return format.ParseStrInt(ipAndPort[1])
}
