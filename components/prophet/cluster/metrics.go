// Copyright 2020 PingCAP, Inc.
// Modifications copyright (C) 2021 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	healthStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prophet",
			Subsystem: "cluster",
			Name:      "health_status",
			Help:      "Status of the cluster.",
		}, []string{"name"})

	resourceEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "prophet",
			Subsystem: "cluster",
			Name:      "resource_event",
			Help:      "Counter of the resource event",
		}, []string{"event"})

	schedulerStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prophet",
			Subsystem: "scheduler",
			Name:      "status",
			Help:      "Status of the scheduler.",
		}, []string{"kind", "type"})

	hotSpotStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prophet",
			Subsystem: "hotspot",
			Name:      "status",
			Help:      "Status of the hotspot.",
		}, []string{"address", "container", "type"})

	patrolCheckShardsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "prophet",
			Subsystem: "checker",
			Name:      "patrol_resources_time",
			Help:      "Time spent of patrol checks resource.",
		})

	clusterStateCPUGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "prophet",
			Subsystem: "server",
			Name:      "cluster_state_cpu_usage",
			Help:      "CPU usage to determine the cluster state",
		})
	clusterStateCurrent = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "prophet",
			Subsystem: "server",
			Name:      "cluster_state_current",
			Help:      "Current state of the cluster",
		}, []string{"state"})

	resourceWaitingListGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "prophet",
			Subsystem: "checker",
			Name:      "resource_waiting_list",
			Help:      "Number of resource in waiting list",
		})
)

func init() {
	prometheus.MustRegister(resourceEventCounter)
	prometheus.MustRegister(healthStatusGauge)
	prometheus.MustRegister(schedulerStatusGauge)
	prometheus.MustRegister(hotSpotStatusGauge)
	prometheus.MustRegister(patrolCheckShardsGauge)
	prometheus.MustRegister(clusterStateCPUGauge)
	prometheus.MustRegister(clusterStateCurrent)
	prometheus.MustRegister(resourceWaitingListGauge)
}
