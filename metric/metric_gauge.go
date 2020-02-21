package metric

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	queueGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "beehive",
			Subsystem: "raftstore",
			Name:      "queue_size",
			Help:      "Total size of queue size.",
		}, []string{"type"})

	batchGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "beehive",
			Subsystem: "raftstore",
			Name:      "batch_size",
			Help:      "Total size of batch size.",
		}, []string{"type"})

	shardCountGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "beehive",
			Subsystem: "raftstore",
			Name:      "store_shard_total",
			Help:      "Total number of store shards.",
		}, []string{"type"})

	storeStorageGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "beehive",
			Subsystem: "raftstore",
			Name:      "store_storage_bytes",
			Help:      "Size of raftstore storage.",
		}, []string{"type"})
)

// SetRaftMsgQueueMetric set send raft message queue size
func SetRaftMsgQueueMetric(size int64) {
	queueGauge.WithLabelValues("sent-raft").Set(float64(size))
}

// SetRaftTickQueueMetric set raft tick queue size
func SetRaftTickQueueMetric(size int64) {
	queueGauge.WithLabelValues("raft-tick").Set(float64(size))
}

// SetRaftReportQueueMetric set raft report queue size
func SetRaftReportQueueMetric(size int64) {
	queueGauge.WithLabelValues("raft-tick").Set(float64(size))
}

// SetRaftStepQueueMetric set raft step queue size
func SetRaftStepQueueMetric(size int64) {
	queueGauge.WithLabelValues("raft-step").Set(float64(size))
}

// SetRaftRequestQueueMetric set raft request queue size
func SetRaftRequestQueueMetric(size int64) {
	queueGauge.WithLabelValues("raft-request").Set(float64(size))
}

// SetRaftApplyResultQueueMetric set raft apply result queue size
func SetRaftApplyResultQueueMetric(size int64) {
	queueGauge.WithLabelValues("raft-apply-result").Set(float64(size))
}

// SetRaftSnapQueueMetric set send raft snapshot queue size
func SetRaftSnapQueueMetric(size int64) {
	queueGauge.WithLabelValues("sent-snap").Set(float64(size))
}

// SetRaftProposalBatchMetric set proposal batch size
func SetRaftProposalBatchMetric(size int64) {
	batchGauge.WithLabelValues("proposal").Set(float64(size))
}

// SetShardsOnStore set the shards count  and leader shards count on the current store
func SetShardsOnStore(leader int, count int) {
	shardCountGauge.WithLabelValues("shards").Set(float64(count))
	shardCountGauge.WithLabelValues("leader").Set(float64(leader))
}

// SetStorageOnStore set total and free storage on the current store
func SetStorageOnStore(total uint64, free uint64) {
	storeStorageGauge.WithLabelValues("total").Set(float64(total))
	storeStorageGauge.WithLabelValues("free").Set(float64(free))
}
