// Copyright 2020 MatrixOrigin.
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

package metric

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	registry = prometheus.NewRegistry()
)

// MustRegister Delegate the prometheus MustRegister
func MustRegister(cs ...prometheus.Collector) {
	registry.MustRegister(cs...)
}

func init() {
	registry.MustRegister(queueGauge)
	registry.MustRegister(batchGauge)
	registry.MustRegister(storeStorageGauge)
	registry.MustRegister(shardCountGauge)

	registry.MustRegister(raftReadyCounter)
	registry.MustRegister(raftMsgsCounter)
	registry.MustRegister(raftCommandCounter)
	registry.MustRegister(raftAdminCommandCounter)

	registry.MustRegister(raftLogLagHistogram)
	registry.MustRegister(raftLogAppendDurationHistogram)
	registry.MustRegister(raftLogApplyDurationHistogram)
	registry.MustRegister(raftProposalSizeHistogram)
	registry.MustRegister(snapshotSizeHistogram)
	registry.MustRegister(snapshotBuildingDurationHistogram)
	registry.MustRegister(snapshotSendingDurationHistogram)
}
