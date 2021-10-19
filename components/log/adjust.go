package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Adjust reutrns zap.L if logger is nil
func Adjust(logger *zap.Logger, options ...zap.Option) *zap.Logger {
	if logger != nil {
		return logger
	}
	return GetDefaultZapLogger(options...)
}

// GetDefaultZapLoggerWithLevel get default zap logger
func GetDefaultZapLoggerWithLevel(level zapcore.Level, options ...zap.Option) *zap.Logger {
	options = append(options, zap.AddStacktrace(zapcore.FatalLevel), zap.AddCaller())
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevel()
	cfg.Level.SetLevel(level)
	cfg.EncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05.000")
	cfg.EncoderConfig.EncodeDuration = zapcore.MillisDurationEncoder
	l, _ := cfg.Build(options...)
	return l
}

// GetDefaultZapLogger get default zap logger
func GetDefaultZapLogger(options ...zap.Option) *zap.Logger {
	return GetDefaultZapLoggerWithLevel(zapcore.InfoLevel, options...)
}
