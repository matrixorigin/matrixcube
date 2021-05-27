package filter

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	filterCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prophet",
			Subsystem: "schedule",
			Name:      "filter",
			Help:      "Counter of the filter",
		}, []string{"action", "address", "container", "scope", "type", "source", "target"})
)

func init() {
	prometheus.MustRegister(filterCounter)
}
