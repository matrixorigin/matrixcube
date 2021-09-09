package log

import (
	"go.uber.org/zap"
)

var l = zap.L()

// UseLogger set cube global logger
func UseLogger(logger *zap.Logger) {
	l = logger
}

// Logger returns the cube global logger
func Logger() *zap.Logger {
	return l
}
