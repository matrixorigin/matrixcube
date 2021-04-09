package movingaverage

import (
	"time"

	"github.com/phf/go-queue/queue"
)

type deltaWithInterval struct {
	delta    float64
	interval time.Duration
}

// AvgOverTime maintains change rate in the last avgInterval.
//
// AvgOverTime takes changes with their own intervals,
// containers recent changes that happened in the last avgInterval,
// then calculates the change rate by (sum of changes) / (sum of intervals).
type AvgOverTime struct {
	que         *queue.Queue      // The element is `deltaWithInterval`, sum of all elements' interval is less than `avgInterval`
	margin      deltaWithInterval // The last element from `PopFront` in `que`
	deltaSum    float64           // Including `margin` and all elements in `que`
	intervalSum time.Duration     // Including `margin` and all elements in `que`
	avgInterval time.Duration
}

// NewAvgOverTime returns an AvgOverTime with given interval.
func NewAvgOverTime(interval time.Duration) *AvgOverTime {
	return &AvgOverTime{
		que: queue.New(),
		margin: deltaWithInterval{
			delta:    0,
			interval: 0,
		},
		deltaSum:    0,
		intervalSum: 0,
		avgInterval: interval,
	}
}

// Get returns change rate in the last interval.
func (aot *AvgOverTime) Get() float64 {
	if aot.intervalSum < aot.avgInterval {
		return 0
	}
	marginDelta := aot.margin.delta * (aot.intervalSum.Seconds() - aot.avgInterval.Seconds()) / aot.margin.interval.Seconds()
	return (aot.deltaSum - marginDelta) / aot.avgInterval.Seconds()
}

// Clear clears the AvgOverTime.
func (aot *AvgOverTime) Clear() {
	aot.que.Init()
	aot.margin = deltaWithInterval{
		delta:    0,
		interval: 0,
	}
	aot.intervalSum = 0
	aot.deltaSum = 0
}

// Add adds recent change to AvgOverTime.
func (aot *AvgOverTime) Add(delta float64, interval time.Duration) {
	if interval == 0 {
		return
	}

	aot.que.PushBack(deltaWithInterval{delta, interval})
	aot.deltaSum += delta
	aot.intervalSum += interval

	for aot.intervalSum-aot.margin.interval >= aot.avgInterval {
		aot.deltaSum -= aot.margin.delta
		aot.intervalSum -= aot.margin.interval
		aot.margin = aot.que.PopFront().(deltaWithInterval)
	}
}

// Set sets AvgOverTime to the given average.
func (aot *AvgOverTime) Set(avg float64) {
	aot.Clear()
	aot.margin.delta = avg * aot.avgInterval.Seconds()
	aot.margin.interval = aot.avgInterval
	aot.deltaSum = aot.margin.delta
	aot.intervalSum = aot.avgInterval
	aot.que.PushBack(deltaWithInterval{delta: aot.deltaSum, interval: aot.intervalSum})
}

// IsFull returns whether AvgOverTime is full
func (aot *AvgOverTime) IsFull() bool {
	return aot.intervalSum >= aot.avgInterval
}
