package metric

import (
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	prometheus.MustRegister(queueGauge)
	prometheus.MustRegister(batchGauge)
	prometheus.MustRegister(storeStorageGauge)
	prometheus.MustRegister(shardCountGauge)

	prometheus.MustRegister(raftReadyCounter)
	prometheus.MustRegister(raftMsgsCounter)
	prometheus.MustRegister(raftProposalCounter)
	prometheus.MustRegister(raftCommandCounter)
	prometheus.MustRegister(raftAdminCommandCounter)

	prometheus.MustRegister(snapshotSizeHistogram)
	prometheus.MustRegister(snapshotSendingDurationHistogram)
}
