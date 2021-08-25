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
	"github.com/fagongzi/log"
)

var (
	// TODO: use zap when it is ready to be integrated
	logger = log.NewLoggerWithPrefix("[pebble]")
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

func getEventListener() cpebble.EventListener {
	return cpebble.EventListener{
		CompactionBegin: func(info cpebble.CompactionInfo) {
			logger.Infof("%s", info)
		},
		CompactionEnd: func(info cpebble.CompactionInfo) {
			logger.Infof("%s", info)
		},
		DiskSlow: func(info cpebble.DiskSlowInfo) {
			logger.Infof("%s", info)
		},
		FlushBegin: func(info cpebble.FlushInfo) {
			logger.Infof("%s", info)
		},
		FlushEnd: func(info cpebble.FlushInfo) {
			logger.Infof("%s", info)
		},
		WriteStallBegin: func(info cpebble.WriteStallBeginInfo) {
			logger.Infof("%s", info)
		},
		WriteStallEnd: func() {
			logger.Infof("write stall ended")
		},
	}
}
