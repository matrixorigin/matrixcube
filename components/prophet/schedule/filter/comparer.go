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

// StoreComparer compares 2 containers. Often used for StoreCandidates to
// sort candidate containers.
type StoreComparer func(a, b *core.CachedStore) int

// ShardScoreComparer creates a StoreComparer to sort container by resource
// score.
func ShardScoreComparer(groupKey string, opt *config.PersistOptions) StoreComparer {
	return func(a, b *core.CachedStore) int {
		sa := a.ShardScore(groupKey, opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0, 0)
		sb := b.ShardScore(groupKey, opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0, 0)
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

// IsolationComparer creates a StoreComparer to sort container by isolation score.
func IsolationComparer(locationLabels []string, resourceStores []*core.CachedStore) StoreComparer {
	return func(a, b *core.CachedStore) int {
		sa := core.DistinctScore(locationLabels, resourceStores, a)
		sb := core.DistinctScore(locationLabels, resourceStores, b)
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
