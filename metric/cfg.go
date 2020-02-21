package metric

import "os"

// Cfg metric cfg
type Cfg struct {
	Addr     string `toml:"addr"`
	Interval int    `toml:"interval"`
	Job      string `toml:"job"`
	Instance string `toml:"instance"`
}

func (c Cfg) instance() string {
	if c.Instance != "" {
		return c.Instance
	}

	name, err := os.Hostname()
	if err != nil {
		return "unknown"
	}

	return name
}
