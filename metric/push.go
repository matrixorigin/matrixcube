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
	"time"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/prometheus/client_golang/prometheus/push"
	"go.uber.org/zap"
)

// StartPush start push metric
func StartPush(cfg Cfg, logger *zap.Logger) {
	logger = log.Adjust(logger)
	logger.Info("start push job metric",
		zap.String("job", cfg.Job),
		zap.String("pushgateway", cfg.Addr),
		zap.Int("interval", cfg.Interval))

	if cfg.Interval == 0 || cfg.Addr == "" || cfg.Job == "" {
		return
	}

	pusher := push.New(cfg.Addr, cfg.Job).
		Gatherer(registry).
		Grouping("instance", cfg.instance())
	go func() {
		timer := time.NewTicker(time.Second * time.Duration(cfg.Interval))
		defer timer.Stop()

		for range timer.C {
			if err := pusher.Push(); err != nil {
				logger.Error("fail to push metric",
					zap.String("pushgateway", cfg.Addr),
					zap.Error(err))
			}
		}
	}()
}
