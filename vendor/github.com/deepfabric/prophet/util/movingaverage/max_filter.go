package movingaverage

import (
	"github.com/montanaflynn/stats"
)

// MaxFilter works as a maximum filter with specified window size.
// There are at most `size` data points for calculating.
type MaxFilter struct {
	records []float64
	size    uint64
	count   uint64
}

// NewMaxFilter returns a MaxFilter.
func NewMaxFilter(size int) *MaxFilter {
	return &MaxFilter{
		records: make([]float64, size),
		size:    uint64(size),
	}
}

// Add adds a data point.
func (r *MaxFilter) Add(n float64) {
	r.records[r.count%r.size] = n
	r.count++
}

// Get returns the maximum of the data set.
func (r *MaxFilter) Get() float64 {
	if r.count == 0 {
		return 0
	}
	records := r.records
	if r.count < r.size {
		records = r.records[:r.count]
	}
	max, _ := stats.Max(records)
	return max
}

// Reset cleans the data set.
func (r *MaxFilter) Reset() {
	r.count = 0
}

// Set = Reset + Add.
func (r *MaxFilter) Set(n float64) {
	r.records[0] = n
	r.count = 1
}
