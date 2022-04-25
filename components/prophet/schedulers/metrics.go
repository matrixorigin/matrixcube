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

var opInfluenceStatus = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "prophet",
		Subsystem: "scheduler",
		Name:      "op_influence",
		Help:      "Store status for schedule",
	}, []string{"scheduler", "container", "type"})

var tolerantShardStatus = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "prophet",
		Subsystem: "scheduler",
		Name:      "tolerant_resource",
		Help:      "Store status for schedule",
	}, []string{"scheduler", "source", "target"})

var balanceLeaderCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "prophet",
		Subsystem: "scheduler",
		Name:      "balance_leader",
		Help:      "Counter of balance leader scheduler.",
	}, []string{"type", "container"})

var balanceShardCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "prophet",
		Subsystem: "scheduler",
		Name:      "balance_resource",
		Help:      "Counter of balance resource scheduler.",
	}, []string{"type", "container"})

var balanceDirectionCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "prophet",
		Subsystem: "scheduler",
		Name:      "balance_direction",
		Help:      "Counter of direction of balance related schedulers.",
	}, []string{"type", "source", "target"})

var scatterRangeLeaderCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "prophet",
		Subsystem: "scheduler",
		Name:      "scatter_range_leader",
		Help:      "Counter of scatter range leader scheduler.",
	}, []string{"type", "container"})

var scatterRangeShardCounter = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "prophet",
		Subsystem: "scheduler",
		Name:      "scatter_range_resource",
		Help:      "Counter of scatter range resource scheduler.",
	}, []string{"type", "container"})

func init() {
	prometheus.MustRegister(schedulerCounter)
	prometheus.MustRegister(schedulerStatus)
	prometheus.MustRegister(balanceLeaderCounter)
	prometheus.MustRegister(balanceShardCounter)
	prometheus.MustRegister(balanceDirectionCounter)
	prometheus.MustRegister(scatterRangeLeaderCounter)
	prometheus.MustRegister(scatterRangeShardCounter)
	prometheus.MustRegister(opInfluenceStatus)
	prometheus.MustRegister(tolerantShardStatus)
}
