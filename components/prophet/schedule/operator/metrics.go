package operator

import "github.com/prometheus/client_golang/prometheus"

var (
	operatorStepDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "prophet",
			Subsystem: "schedule",
			Name:      "finish_operator_steps_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of finished operator step.",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 16),
		}, []string{"type"})

	// OperatorLimitCounter exposes the counter when meeting limit.
	OperatorLimitCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prophet",
			Subsystem: "schedule",
			Name:      "operator_limit",
			Help:      "Counter of operator meeting limit",
		}, []string{"type", "name"})
)

func init() {
	prometheus.MustRegister(operatorStepDuration)
	prometheus.MustRegister(OperatorLimitCounter)
}
