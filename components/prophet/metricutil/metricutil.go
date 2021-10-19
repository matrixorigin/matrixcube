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

package metricutil

import (
	"os"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"go.uber.org/zap"
)

const zeroDuration = time.Duration(0)

// MetricConfig is the metric configuration.
type MetricConfig struct {
	PushJob      string            `toml:"job" json:"job"`
	PushAddress  string            `toml:"address" json:"address"`
	PushInterval typeutil.Duration `toml:"interval" json:"interval"`
}

// prometheusPushClient pushes metrics to Prometheus Pushgateway.
func prometheusPushClient(job, addr string, interval time.Duration, logger *zap.Logger) {
	pusher := push.New(addr, job).
		Gatherer(prometheus.DefaultGatherer).
		Grouping("instance", instanceName())

	for {
		err := pusher.Push()
		if err != nil {
			logger.Error("fail to push metrics to Prometheus Pushgateway",
				zap.Error(err))
		}

		time.Sleep(interval)
	}
}

// Push metrics in background.
func Push(cfg *MetricConfig, logger *zap.Logger) {
	if cfg.PushInterval.Duration == zeroDuration || len(cfg.PushAddress) == 0 {
		logger.Info("disable Prometheus push client")
		return
	}

	logger.Info("start Prometheus push client")

	interval := cfg.PushInterval.Duration
	go prometheusPushClient(cfg.PushJob, cfg.PushAddress, interval, logger)
}

func instanceName() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}
