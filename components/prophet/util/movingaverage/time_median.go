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

package movingaverage

import (
	"time"
)

// TimeMedian is AvgOverTime + MedianFilter
// Size of MedianFilter should be larger than double size of AvgOverTime to denoisy.
// Delay is aotSize * mfSize * reportInterval/4
// and the min filled period is aotSize * reportInterval, which is not related with mfSize
type TimeMedian struct {
	aot           *AvgOverTime
	mf            *MedianFilter
	aotSize       int
	mfSize        int
	instantaneous float64
}

// NewTimeMedian returns a TimeMedian with given size.
func NewTimeMedian(aotSize, mfSize int, reportInterval time.Duration) *TimeMedian {
	return &TimeMedian{
		aot:     NewAvgOverTime(time.Duration(aotSize) * reportInterval),
		mf:      NewMedianFilter(mfSize),
		aotSize: aotSize,
		mfSize:  mfSize,
	}
}

// Get returns change rate in the median of the several intervals.
func (t *TimeMedian) Get() float64 {
	return t.mf.Get()
}

// Add adds recent change to TimeMedian.
func (t *TimeMedian) Add(delta float64, interval time.Duration) {
	t.instantaneous = delta / interval.Seconds()
	t.aot.Add(delta, interval)
	if t.aot.IsFull() {
		t.mf.Add(t.aot.Get())
	}
}

// Set sets the given average.
func (t *TimeMedian) Set(avg float64) {
	t.mf.Set(avg)
}

// GetFilledPeriod returns filled period.
func (t *TimeMedian) GetFilledPeriod() int { // it is unrelated with mfSize
	return t.aotSize
}

// GetInstantaneous returns instantaneous speed
func (t *TimeMedian) GetInstantaneous() float64 {
	return t.instantaneous
}
