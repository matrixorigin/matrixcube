package checker

import "github.com/prometheus/client_golang/prometheus"

var (
	checkerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prophet",
			Subsystem: "checker",
			Name:      "event_count",
			Help:      "Counter of checker events.",
		}, []string{"type", "name"})
)

func init() {
	prometheus.MustRegister(checkerCounter)
}
