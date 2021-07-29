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

	"github.com/fagongzi/log"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
	logger = log.NewLoggerWithPrefix("[matrixcube-metric]")
)

// StartPush start push metric
func StartPush(cfg Cfg) {
	logger.Infof("start push job %s metric to prometheus pushgateway %s, interval %d seconds",
		cfg.Job,
		cfg.Addr,
		cfg.Interval)

	if cfg.Interval == 0 || cfg.Addr == "" || cfg.Job == "" {
		return
	}

	pusher := push.New(cfg.Addr, cfg.Job).
		Gatherer(registry).
		Grouping("instance", cfg.instance())
	go func() {
		timer := time.NewTicker(time.Second * time.Duration(cfg.Interval))
		defer timer.Stop()

		for {
			select {
			case <-timer.C:
				err := pusher.Push()
				if err != nil {
					logger.Errorf("push to %s failed with %+v",
						cfg.Addr,
						err)
				}
			}
		}
	}()
}
