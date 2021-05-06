package schedulers

import (
	"github.com/prometheus/client_golang/prometheus"
)

var schedulerCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "prophet",
		Subsystem: "scheduler",
		Name:      "event_count",
		Help:      "Counter of scheduler events.",
	}, []string{"type", "name"})

var schedulerStatus = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "prophet",
		Subsystem: "scheduler",
		Name:      "inner_status",
		Help:      "Inner status of the scheduler.",
	}, []string{"type", "name"})

var hotPeerSummary = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "prophet",
		Subsystem: "scheduler",
		Name:      "hot_peers_summary",
		Help:      "Hot peers summary for each container",
	}, []string{"type", "container"})

var opInfluenceStatus = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "prophet",
		Subsystem: "scheduler",
		Name:      "op_influence",
		Help:      "Container status for schedule",
	}, []string{"scheduler", "container", "type"})

var tolerantResourceStatus = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "prophet",
		Subsystem: "scheduler",
		Name:      "tolerant_resource",
		Help:      "Container status for schedule",
	}, []string{"scheduler", "source", "target"})

var balanceLeaderCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "prophet",
		Subsystem: "scheduler",
		Name:      "balance_leader",
		Help:      "Counter of balance leader scheduler.",
	}, []string{"type", "container"})

var balanceResourceCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "prophet",
		Subsystem: "scheduler",
		Name:      "balance_resource",
		Help:      "Counter of balance resource scheduler.",
	}, []string{"type", "container"})

var hotSchedulerResultCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "prophet",
		Subsystem: "scheduler",
		Name:      "hot_resource",
		Help:      "Counter of hot resource scheduler.",
	}, []string{"type", "container"})

var balanceDirectionCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "prophet",
		Subsystem: "scheduler",
		Name:      "balance_direction",
		Help:      "Counter of direction of balance related schedulers.",
	}, []string{"type", "source", "target"})

var hotDirectionCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "prophet",
		Subsystem: "scheduler",
		Name:      "hot_resource_direction",
		Help:      "Counter of hot resource scheduler.",
	}, []string{"type", "rw", "container", "direction"})

var scatterRangeLeaderCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "prophet",
		Subsystem: "scheduler",
		Name:      "scatter_range_leader",
		Help:      "Counter of scatter range leader scheduler.",
	}, []string{"type", "container"})

var scatterRangeResourceCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "prophet",
		Subsystem: "scheduler",
		Name:      "scatter_range_resource",
		Help:      "Counter of scatter range resource scheduler.",
	}, []string{"type", "container"})

func init() {
	prometheus.MustRegister(schedulerCounter)
	prometheus.MustRegister(schedulerStatus)
	prometheus.MustRegister(hotPeerSummary)
	prometheus.MustRegister(balanceLeaderCounter)
	prometheus.MustRegister(balanceResourceCounter)
	prometheus.MustRegister(hotSchedulerResultCounter)
	prometheus.MustRegister(hotDirectionCounter)
	prometheus.MustRegister(balanceDirectionCounter)
	prometheus.MustRegister(scatterRangeLeaderCounter)
	prometheus.MustRegister(scatterRangeResourceCounter)
	prometheus.MustRegister(opInfluenceStatus)
	prometheus.MustRegister(tolerantResourceStatus)
}
