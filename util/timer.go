package util

import (
	"time"

	"github.com/fagongzi/goetty"
)

var (
	defaultTW = goetty.NewTimeoutWheel(goetty.WithTickInterval(time.Millisecond * 50))
)

// DefaultTimeoutWheel returns default timeout wheel
func DefaultTimeoutWheel() *goetty.TimeoutWheel {
	return defaultTW
}
