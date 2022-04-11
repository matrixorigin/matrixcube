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

package prophet

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixcube/components/log"
	"go.uber.org/zap"
)

// StartMonitor calls systimeErrHandler if system time jump backward.
func StartMonitor(ctx context.Context, now func() time.Time, systimeErrHandler func(), logger *zap.Logger) {
	logger = log.Adjust(logger)
	logger.Info("start system time monitor")
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	last := now()
	for {
		select {
		case <-ticker.C:
			if t := now(); t.Before(last) {
				logger.Error("system time jump backward",
					zap.Time("last", last))
				systimeErrHandler()
			} else {
				last = t
			}
		case <-ctx.Done():
			return
		}
	}
}
