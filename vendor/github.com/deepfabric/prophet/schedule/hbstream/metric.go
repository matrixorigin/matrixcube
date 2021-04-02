package hbstream

import "github.com/prometheus/client_golang/prometheus"

var (
	heartbeatStreamCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prophet",
			Subsystem: "hbstream",
			Name:      "resource_message",
			Help:      "Counter of message hbstream sent.",
		}, []string{"address", "container", "type", "status"})
)

func init() {
	prometheus.MustRegister(heartbeatStreamCounter)
}
