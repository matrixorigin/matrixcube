package metric

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	raftReadyCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "beehive",
			Subsystem: "raftstore",
			Name:      "raft_ready_handled_total",
			Help:      "Total number of raft ready handled.",
		}, []string{"type"})

	raftMsgsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "beehive",
			Subsystem: "raftstore",
			Name:      "raft_sent_msg_total",
			Help:      "Total number of raft ready sent messages.",
		}, []string{"type"})

	raftCommandCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "beehive",
			Subsystem: "raftstore",
			Name:      "command_normal_total",
			Help:      "Total number of normal commands received.",
		}, []string{"type"})

	raftAdminCommandCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "beehive",
			Subsystem: "raftstore",
			Name:      "command_admin_total",
			Help:      "Total number of admin commands processed.",
		}, []string{"type", "status"})
)

// IncComandCount inc the command received
func IncComandCount(cmd string) {
	raftCommandCounter.WithLabelValues(cmd).Inc()
}

// AddRaftReadySendCount add raft ready to sent raft message
func AddRaftReadySendCount(value uint64) {
	raftReadyCounter.WithLabelValues("send").Add(float64(value))
}

// AddRaftReadyCommitCount add raft ready to commit raft log
func AddRaftReadyCommitCount(value uint64) {
	raftReadyCounter.WithLabelValues("commit").Add(float64(value))
}

// AddRaftReadyAppendCount add raft ready to append raft log
func AddRaftReadyAppendCount(value uint64) {
	raftReadyCounter.WithLabelValues("append").Add(float64(value))
}

// AddRaftReadySnapshotCount add raft ready to append raft log
func AddRaftReadySnapshotCount(value uint64) {
	raftReadyCounter.WithLabelValues("snapshot").Add(float64(value))
}

// AddRaftAppendMsgsCount add raft append msgs
func AddRaftAppendMsgsCount(value uint64) {
	raftMsgsCounter.WithLabelValues("append").Add(float64(value))
}

// AddRaftAppendRespMsgsCount add raft append resp msgs
func AddRaftAppendRespMsgsCount(value uint64) {
	raftMsgsCounter.WithLabelValues("append-resp").Add(float64(value))
}

// AddRaftVoteMsgsCount add raft vote msgs
func AddRaftVoteMsgsCount(value uint64) {
	raftMsgsCounter.WithLabelValues("vote").Add(float64(value))
}

// AddRaftVoteRespMsgsCount add raft vote resp msgs
func AddRaftVoteRespMsgsCount(value uint64) {
	raftMsgsCounter.WithLabelValues("vote-resp").Add(float64(value))
}

// AddRaftSnapshotMsgsCount add raft snapshot msgs
func AddRaftSnapshotMsgsCount(value uint64) {
	raftMsgsCounter.WithLabelValues("snapshot").Add(float64(value))
}

// AddRaftHeartbeatMsgsCount add raft heatbeat msgs
func AddRaftHeartbeatMsgsCount(value uint64) {
	raftMsgsCounter.WithLabelValues("heartbeat").Add(float64(value))
}

// AddRaftHeartbeatRespMsgsCount add raft heatbeat resp msgs
func AddRaftHeartbeatRespMsgsCount(value uint64) {
	raftMsgsCounter.WithLabelValues("heartbeat-resp").Add(float64(value))
}

// AddRaftTransferLeaderMsgsCount add raft heatbeat msgs
func AddRaftTransferLeaderMsgsCount(value uint64) {
	raftMsgsCounter.WithLabelValues("transfer").Add(float64(value))
}

// AddRaftProposalReadLocalCount add read local
func AddRaftProposalReadLocalCount(value uint64) {
	raftMsgsCounter.WithLabelValues("read-local").Add(float64(value))
}

// AddRaftProposalReadIndexCount add read index
func AddRaftProposalReadIndexCount(value uint64) {
	raftMsgsCounter.WithLabelValues("read-index").Add(float64(value))
}

// AddRaftProposalNormalCount add normal
func AddRaftProposalNormalCount(value uint64) {
	raftMsgsCounter.WithLabelValues("normal").Add(float64(value))
}

// AddRaftProposalTransferLeaderCount add transfer leader
func AddRaftProposalTransferLeaderCount(value uint64) {
	raftMsgsCounter.WithLabelValues("transfer").Add(float64(value))
}

// AddRaftProposalConfChangeCount add conf change
func AddRaftProposalConfChangeCount(value uint64) {
	raftMsgsCounter.WithLabelValues("conf").Add(float64(value))
}

// AddRaftAdminCommandConfChangeCount admin command of conf change
func AddRaftAdminCommandConfChangeCount(value uint64) {
	raftAdminCommandCounter.WithLabelValues("conf", "total").Add(float64(value))
}

// AddRaftAdminCommandConfChangeSucceedCount admin command of conf change succeed
func AddRaftAdminCommandConfChangeSucceedCount(value uint64) {
	raftAdminCommandCounter.WithLabelValues("add", "succeed").Add(float64(value))
}

// AddRaftAdminCommandConfChangeRejectCount admin command of conf change been rejected
func AddRaftAdminCommandConfChangeRejectCount(value uint64) {
	raftAdminCommandCounter.WithLabelValues("add", "rejected").Add(float64(value))
}

// AddRaftAdminCommandSplitCount admin command of split shard
func AddRaftAdminCommandSplitCount(value uint64) {
	raftAdminCommandCounter.WithLabelValues("split", "total").Add(float64(value))
}

// AddRaftAdminCommandSplitSucceedCount admin command of split shard succeed
func AddRaftAdminCommandSplitSucceedCount(value uint64) {
	raftAdminCommandCounter.WithLabelValues("split", "succeed").Add(float64(value))
}

// AddRaftAdminCommandCompactCount admin command of compact raft log
func AddRaftAdminCommandCompactCount(value uint64) {
	raftAdminCommandCounter.WithLabelValues("compact", "succeed").Add(float64(value))
}

// AddRaftAdminCommandCompactSucceedCount admin command of compact raft log succeed
func AddRaftAdminCommandCompactSucceedCount(value uint64) {
	raftAdminCommandCounter.WithLabelValues("compact", "succeed").Add(float64(value))
}
