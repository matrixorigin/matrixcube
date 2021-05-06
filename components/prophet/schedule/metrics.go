package schedule

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	operatorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prophet",
			Subsystem: "schedule",
			Name:      "operators_count",
			Help:      "Counter of schedule operators.",
		}, []string{"type", "event"})

	operatorDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "prophet",
			Subsystem: "schedule",
			Name:      "finish_operators_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of finished operator.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 16),
		}, []string{"type"})

	operatorWaitCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prophet",
			Subsystem: "schedule",
			Name:      "operators_waiting_count",
			Help:      "Counter of schedule waiting operators.",
		}, []string{"type", "event"})

	operatorWaitDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "prophet",
			Subsystem: "schedule",
			Name:      "waiting_operators_duration_seconds",
			Help:      "Bucketed histogram of waiting time (s) of operator for being promoted.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 16),
		}, []string{"type"})

	containerLimitAvailableGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prophet",
			Subsystem: "schedule",
			Name:      "container_limit_available",
			Help:      "available limit rate of container.",
		}, []string{"container", "limit_type"})

	containerLimitRateGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prophet",
			Subsystem: "schedule",
			Name:      "container_limit_rate",
			Help:      "the limit rate of container.",
		}, []string{"container", "limit_type"})

	containerLimitCostCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prophet",
			Subsystem: "schedule",
			Name:      "container_limit_cost",
			Help:      "limit rate cost of container.",
		}, []string{"container", "limit_type"})
)

func init() {
	prometheus.MustRegister(operatorCounter)
	prometheus.MustRegister(operatorDuration)
	prometheus.MustRegister(operatorWaitDuration)
	prometheus.MustRegister(containerLimitAvailableGauge)
	prometheus.MustRegister(containerLimitRateGauge)
	prometheus.MustRegister(containerLimitCostCounter)
	prometheus.MustRegister(operatorWaitCounter)
}
