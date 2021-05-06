package movingaverage

import (
	"math"
)

const defaultHMASize = 10

// HMA works as hull moving average
// There are at most `size` data points for calculating.
// References: https://www.fidelity.com/learning-center/trading-investing/technical-analysis/technical-indicator-guide/hull-moving-average
type HMA struct {
	size uint64
	wma  []*WMA
}

// NewHMA returns a WMA.
func NewHMA(sizes ...float64) *HMA {
	size := defaultHMASize
	if len(sizes) != 0 && sizes[0] > 1 {
		size = int(sizes[0])
	}
	wma := make([]*WMA, 3)
	wma[0] = NewWMA(size / 2)
	wma[1] = NewWMA(size)
	wma[2] = NewWMA(int(math.Sqrt(float64(size))))
	return &HMA{
		wma:  wma,
		size: uint64(size),
	}
}

// Add adds a data point.
func (h *HMA) Add(n float64) {
	h.wma[0].Add(n)
	h.wma[1].Add(n)
	h.wma[2].Add(2*h.wma[0].Get() - h.wma[1].Get())
}

// Get returns the weight average of the data set.
func (h *HMA) Get() float64 {
	return h.wma[2].Get()
}

// Reset cleans the data set.
func (h *HMA) Reset() {
	h.wma[0] = NewWMA(int(h.size / 2))
	h.wma[1] = NewWMA(int(h.size))
	h.wma[2] = NewWMA(int(math.Sqrt(float64(h.size))))
}

// Set = Reset + Add.
func (h *HMA) Set(n float64) {
	h.Reset()
	h.Add(n)
}
