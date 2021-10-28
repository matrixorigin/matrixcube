// Copyright 2021 MatrixOrigin.
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

package pebble

import (
	cpebble "github.com/cockroachdb/pebble"
	"go.uber.org/zap"
)

func hasEventListener(l cpebble.EventListener) bool {
	return l.BackgroundError != nil ||
		l.CompactionBegin != nil ||
		l.CompactionEnd != nil ||
		l.DiskSlow != nil ||
		l.FlushBegin != nil ||
		l.FlushEnd != nil ||
		l.ManifestCreated != nil ||
		l.ManifestDeleted != nil ||
		l.TableCreated != nil ||
		l.TableDeleted != nil ||
		l.TableIngested != nil ||
		l.TableStatsLoaded != nil ||
		l.WALCreated != nil ||
		l.WALDeleted != nil ||
		l.WriteStallBegin != nil ||
		l.WriteStallEnd != nil
}

func getEventListener(logger *zap.Logger) cpebble.EventListener {
	return cpebble.EventListener{
		CompactionBegin: func(info cpebble.CompactionInfo) {
			logger.Info(info.String())
		},
		CompactionEnd: func(info cpebble.CompactionInfo) {
			logger.Info(info.String())
		},
		DiskSlow: func(info cpebble.DiskSlowInfo) {
			logger.Info(info.String())
		},
		FlushBegin: func(info cpebble.FlushInfo) {
			logger.Info(info.String())
		},
		FlushEnd: func(info cpebble.FlushInfo) {
			logger.Info(info.String())
		},
		WriteStallBegin: func(info cpebble.WriteStallBeginInfo) {
			logger.Info(info.String())
		},
		WriteStallEnd: func() {
			logger.Info("write stall ended")
		},
	}
}
