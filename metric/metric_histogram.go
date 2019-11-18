package metric

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	raftProposalSizeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "beehive",
			Subsystem: "raftstore",
			Name:      "raft_proposal_log_bytes",
			Help:      "Bucketed histogram of peer proposing log size.",
			Buckets:   []float64{256.0, 512.0, 1024.0, 4096.0, 65536.0, 262144.0, 524288.0, 1048576.0, 2097152.0, 4194304.0, 8388608.0, 16777216.0},
		})

	raftLogAppendDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "beehive",
			Subsystem: "raftstore",
			Name:      "raft_log_append_duration_seconds",
			Help:      "Bucketed histogram of peer appending log duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	raftLogApplyDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "beehive",
			Subsystem: "raftstore",
			Name:      "raft_log_apply_duration_seconds",
			Help:      "Bucketed histogram of peer appending log duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	snapshotSizeHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "beehive",
			Subsystem: "raftstore",
			Name:      "snapshot_size_bytes",
			Help:      "Bytes of per snapshot.",
			Buckets:   prometheus.ExponentialBuckets(1024.0, 2.0, 22),
		})

	snapshotBuildingDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "beehive",
			Subsystem: "raftstore",
			Name:      "snapshot_building_duration_seconds",
			Help:      "Bucketed histogram of snapshot build time duration.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2.0, 20),
		})

	snapshotSendingDurationHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "beehive",
			Subsystem: "raftstore",
			Name:      "snapshot_sending_duration_seconds",
			Help:      "Bucketed histogram of server send snapshots duration.",
		})

	raftLogLagHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "beehive",
			Subsystem: "raftstore",
			Name:      "raft_log_lag",
			Help:      "Bucketed histogram of log lag in a shard.",
			Buckets:   []float64{2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 5120.0, 10240.0},
		})
)

// ObserveProposalBytes observe bytes per raft proposal
func ObserveProposalBytes(size int64) {
	raftProposalSizeHistogram.Observe(float64(size))
}

// ObserveSnapshotBytes observe bytes per snapshot
func ObserveSnapshotBytes(size int64) {
	snapshotSizeHistogram.Observe(float64(size))
}

// ObserveSnapshotBuildingDuration observe building seconds per snapshot
func ObserveSnapshotBuildingDuration(start time.Time) {
	snapshotBuildingDurationHistogram.Observe(time.Now().Sub(start).Seconds())
}

// ObserveSnapshotSendingDuration observe seconds per snapshot
func ObserveSnapshotSendingDuration(start time.Time) {
	snapshotSendingDurationHistogram.Observe(time.Now().Sub(start).Seconds())
}

// ObserveRaftLogAppendDuration observe seconds raft log append
func ObserveRaftLogAppendDuration(start time.Time) {
	raftLogAppendDurationHistogram.Observe(time.Now().Sub(start).Seconds())
}

// ObserveRaftLogApplyDuration observe seconds raft log apply
func ObserveRaftLogApplyDuration(start time.Time) {
	raftLogApplyDurationHistogram.Observe(time.Now().Sub(start).Seconds())
}

// ObserveRaftLogLag observe raft log lag
func ObserveRaftLogLag(size uint64) {
	raftLogLagHistogram.Observe(float64(size))
}
