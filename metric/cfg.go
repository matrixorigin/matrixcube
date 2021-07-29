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
	"os"
)

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
