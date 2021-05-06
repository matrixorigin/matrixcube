package statistics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	hotCacheStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prophet",
			Subsystem: "hotcache",
			Name:      "status",
			Help:      "Status of the hotspot.",
		}, []string{"name", "container", "type"})

	containerStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prophet",
			Subsystem: "scheduler",
			Name:      "container_status",
			Help:      "Container status for schedule",
		}, []string{"address", "container", "type"})

	resourceStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prophet",
			Subsystem: "resources",
			Name:      "status",
			Help:      "Status of the resources.",
		}, []string{"type"})

	clusterStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prophet",
			Subsystem: "cluster",
			Name:      "status",
			Help:      "Status of the cluster.",
		}, []string{"type"})

	placementStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prophet",
			Subsystem: "cluster",
			Name:      "placement_status",
			Help:      "Status of the cluster placement.",
		}, []string{"type", "name"})

	configStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prophet",
			Subsystem: "config",
			Name:      "status",
			Help:      "Status of the scheduling configurations.",
		}, []string{"type"})

	resourceLabelLevelGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prophet",
			Subsystem: "resources",
			Name:      "label_level",
			Help:      "Number of resources in the different label level.",
		}, []string{"type"})
	readByteHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "prophet",
			Subsystem: "scheduler",
			Name:      "read_byte_hist",
			Help:      "The distribution of resource read bytes",
			Buckets:   prometheus.ExponentialBuckets(1, 8, 12),
		})
	writeByteHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "prophet",
			Subsystem: "scheduler",
			Name:      "write_byte_hist",
			Help:      "The distribution of resource write bytes",
			Buckets:   prometheus.ExponentialBuckets(1, 8, 12),
		})
	readKeyHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "prophet",
			Subsystem: "scheduler",
			Name:      "read_key_hist",
			Help:      "The distribution of resource read keys",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		})
	writeKeyHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "prophet",
			Subsystem: "scheduler",
			Name:      "write_key_hist",
			Help:      "The distribution of resource write keys",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 18),
		})
	resourceHeartbeatIntervalHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "prophet",
			Subsystem: "scheduler",
			Name:      "resource_heartbeat_interval_hist",
			Help:      "Bucketed histogram of the batch size of handled requests.",
			Buckets:   prometheus.LinearBuckets(0, 30, 20),
		})
	containerHeartbeatIntervalHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "prophet",
			Subsystem: "scheduler",
			Name:      "container_heartbeat_interval_hist",
			Help:      "Bucketed histogram of the batch size of handled requests.",
			Buckets:   prometheus.LinearBuckets(0, 5, 12),
		})
)

func init() {
	prometheus.MustRegister(hotCacheStatusGauge)
	prometheus.MustRegister(containerStatusGauge)
	prometheus.MustRegister(resourceStatusGauge)
	prometheus.MustRegister(clusterStatusGauge)
	prometheus.MustRegister(placementStatusGauge)
	prometheus.MustRegister(configStatusGauge)
	prometheus.MustRegister(resourceLabelLevelGauge)
	prometheus.MustRegister(readByteHist)
	prometheus.MustRegister(readKeyHist)
	prometheus.MustRegister(writeKeyHist)
	prometheus.MustRegister(writeByteHist)
	prometheus.MustRegister(resourceHeartbeatIntervalHist)
	prometheus.MustRegister(containerHeartbeatIntervalHist)
}
