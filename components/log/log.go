package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	l, _ = zap.NewProduction(zap.AddStacktrace(zapcore.FatalLevel))
)

// UseLogger set cube global logger
func UseLogger(logger *zap.Logger) {
	l = logger
}

// Logger returns the cube global logger
func Logger() *zap.Logger {
	return l
}
