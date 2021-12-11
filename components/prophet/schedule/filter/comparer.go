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

package filter

import (
	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
)

// ContainerComparer compares 2 containers. Often used for ContainerCandidates to
// sort candidate containers.
type ContainerComparer func(a, b *core.CachedContainer) int

// ResourceScoreComparer creates a ContainerComparer to sort container by resource
// score.
func ResourceScoreComparer(groupKey string, opt *config.PersistOptions) ContainerComparer {
	return func(a, b *core.CachedContainer) int {
		sa := a.ResourceScore(groupKey, opt.GetResourceScoreFormulaVersion(), opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0, 0)
		sb := b.ResourceScore(groupKey, opt.GetResourceScoreFormulaVersion(), opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0, 0)
		switch {
		case sa > sb:
			return 1
		case sa < sb:
			return -1
		default:
			return 0
		}
	}
}

// IsolationComparer creates a ContainerComparer to sort container by isolation score.
func IsolationComparer(locationLabels []string, resourceContainers []*core.CachedContainer) ContainerComparer {
	return func(a, b *core.CachedContainer) int {
		sa := core.DistinctScore(locationLabels, resourceContainers, a)
		sb := core.DistinctScore(locationLabels, resourceContainers, b)
		switch {
		case sa > sb:
			return 1
		case sa < sb:
			return -1
		default:
			return 0
		}
	}
}
