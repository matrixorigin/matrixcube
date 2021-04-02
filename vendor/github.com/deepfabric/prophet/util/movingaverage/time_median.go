package movingaverage

import (
	"time"
)

// TimeMedian is AvgOverTime + MedianFilter
// Size of MedianFilter should be larger than double size of AvgOverTime to denoisy.
// Delay is aotSize * mfSize * reportInterval/4
// and the min filled period is aotSize * reportInterval, which is not related with mfSize
type TimeMedian struct {
	aotInterval   time.Duration
	aot           *AvgOverTime
	mf            *MedianFilter
	aotSize       int
	mfSize        int
	instantaneous float64
}

// NewTimeMedian returns a TimeMedian with given size.
func NewTimeMedian(aotSize, mfSize, reportInterval int) *TimeMedian {
	interval := time.Duration(aotSize*reportInterval) * time.Second
	return &TimeMedian{
		aotInterval: interval,
		aot:         NewAvgOverTime(interval),
		mf:          NewMedianFilter(mfSize),
		aotSize:     aotSize,
		mfSize:      mfSize,
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
