package metric

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	registry = prometheus.NewRegistry()
)

// MustRegister Delegate the prometheus MustRegister
func MustRegister(cs ...prometheus.Collector) {
	registry.MustRegister(cs...)
}

func init() {
	registry.MustRegister(queueGauge)
	registry.MustRegister(batchGauge)
	registry.MustRegister(storeStorageGauge)
	registry.MustRegister(shardCountGauge)

	registry.MustRegister(raftReadyCounter)
	registry.MustRegister(raftMsgsCounter)
	registry.MustRegister(raftCommandCounter)
	registry.MustRegister(raftAdminCommandCounter)

	registry.MustRegister(raftLogLagHistogram)
	registry.MustRegister(raftLogAppendDurationHistogram)
	registry.MustRegister(raftLogApplyDurationHistogram)
	registry.MustRegister(raftProposalSizeHistogram)
	registry.MustRegister(snapshotSizeHistogram)
	registry.MustRegister(snapshotBuildingDurationHistogram)
	registry.MustRegister(snapshotSendingDurationHistogram)
}
