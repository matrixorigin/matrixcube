package movingaverage

// MovingAvg provides moving average.
// Ref: https://en.wikipedia.org/wiki/Moving_average
type MovingAvg interface {
	// Add adds a data point to the data set.
	Add(data float64)
	// Get returns the moving average.
	Get() float64
	// Reset cleans the data set.
	Reset()
	// Set = Reset + Add
	Set(data float64)
}
