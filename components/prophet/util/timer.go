package util

import (
	"time"

	"github.com/fagongzi/goetty/timewheel"
)

var (
	defaultTW = timewheel.NewTimeoutWheel(timewheel.WithTickInterval(time.Millisecond * 50))
)

// DefaultTimeoutWheel returns default timeout wheel
func DefaultTimeoutWheel() *timewheel.TimeoutWheel {
	return defaultTW
}
