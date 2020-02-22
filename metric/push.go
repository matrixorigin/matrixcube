package metric

import (
	"time"

	"github.com/fagongzi/log"
	"github.com/prometheus/client_golang/prometheus/push"
)

var (
	logger = log.NewLoggerWithPrefix("[beehive-metric]")
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
