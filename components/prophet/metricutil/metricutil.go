package metricutil

import (
	"os"
	"time"
	"unicode"

	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
)

const zeroDuration = time.Duration(0)

// MetricConfig is the metric configuration.
type MetricConfig struct {
	PushJob      string            `toml:"job" json:"job"`
	PushAddress  string            `toml:"address" json:"address"`
	PushInterval typeutil.Duration `toml:"interval" json:"interval"`
}

func runesHasLowerNeighborAt(runes []rune, idx int) bool {
	if idx > 0 && unicode.IsLower(runes[idx-1]) {
		return true
	}
	if idx+1 < len(runes) && unicode.IsLower(runes[idx+1]) {
		return true
	}
	return false
}

func camelCaseToSnakeCase(str string) string {
	runes := []rune(str)
	length := len(runes)

	var ret []rune
	for i := 0; i < length; i++ {
		if i > 0 && unicode.IsUpper(runes[i]) && runesHasLowerNeighborAt(runes, i) {
			ret = append(ret, '_')
		}
		ret = append(ret, unicode.ToLower(runes[i]))
	}

	return string(ret)
}

// prometheusPushClient pushes metrics to Prometheus Pushgateway.
func prometheusPushClient(job, addr string, interval time.Duration) {
	pusher := push.New(addr, job).
		Gatherer(prometheus.DefaultGatherer).
		Grouping("instance", instanceName())

	for {
		err := pusher.Push()
		if err != nil {
			util.GetLogger().Errorf("could not push metrics to Prometheus Pushgateway, error %+v",
				err)
		}

		time.Sleep(interval)
	}
}

// Push metrics in background.
func Push(cfg *MetricConfig) {
	if cfg.PushInterval.Duration == zeroDuration || len(cfg.PushAddress) == 0 {
		util.GetLogger().Info("disable Prometheus push client")
		return
	}

	util.GetLogger().Info("start Prometheus push client")

	interval := cfg.PushInterval.Duration
	go prometheusPushClient(cfg.PushJob, cfg.PushAddress, interval)
}

func instanceName() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return hostname
}
