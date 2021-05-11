package typeutil

import "time"

// MinUint64 returns the min value between two variables whose type are uint64.
func MinUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// MaxUint64 returns the max value between two variables whose type are uint64.
func MaxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// MinDuration returns the min value between two variables whose type are time.Duration.
func MinDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
