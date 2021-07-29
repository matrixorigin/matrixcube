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

package util

import (
	"math"
	"time"
)

var (
	bootstrapBalanceCount = uint64(10)
	bootstrapBalanceDiff  = float64(2)
)

// MinUint64 min uint64
func MinUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// MaxUint64 max uint64
func MaxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// MinDuration min duration
func MinDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// MinBalanceDiff returns the minimal diff to do balance. The formula is based
// on experience to let the diff increase alone with the count slowly.
func MinBalanceDiff(count uint64) float64 {
	if count < bootstrapBalanceCount {
		return bootstrapBalanceDiff
	}
	return math.Sqrt(float64(count))
}
