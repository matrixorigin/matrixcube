package log

import (
	"os"
)

// NewLoggerWithPrefix returns a logger with prefix
func NewLoggerWithPrefix(prefix string) Logger {
	return &prefixLoggerWrapper{
		l:      defaultLog,
		prefix: prefix,
	}
}

type prefixLoggerWrapper struct {
	l      *logger
	prefix string
}

func (l *prefixLoggerWrapper) FatalEnabled() bool {
	return l.l.FatalEnabled()
}

func (l *prefixLoggerWrapper) ErrorEnabled() bool {
	return l.l.ErrorEnabled()
}

func (l *prefixLoggerWrapper) WarnEnabled() bool {
	return l.l.WarnEnabled()
}

func (l *prefixLoggerWrapper) InfoEnabled() bool {
	return l.l.InfoEnabled()
}

func (l *prefixLoggerWrapper) DebugEnabled() bool {
	return l.l.DebugEnabled()
}

func (l *prefixLoggerWrapper) Info(v ...interface{}) {
	l.l.logWithPrefix(infoLevel, l.prefix, v...)
}

func (l *prefixLoggerWrapper) Infof(format string, v ...interface{}) {
	l.l.logfWithPrefix(infoLevel, format, l.prefix, v...)
}

func (l *prefixLoggerWrapper) Debug(v ...interface{}) {
	l.l.logWithPrefix(debugLevel, l.prefix, v...)
}

func (l *prefixLoggerWrapper) Debugf(format string, v ...interface{}) {
	l.l.logfWithPrefix(debugLevel, format, l.prefix, v...)
}

func (l *prefixLoggerWrapper) Warning(v ...interface{}) {
	l.l.logWithPrefix(warnLevel, l.prefix, v...)
}

func (l *prefixLoggerWrapper) Warningf(format string, v ...interface{}) {
	l.l.logfWithPrefix(warnLevel, format, l.prefix, v...)
}

func (l *prefixLoggerWrapper) Error(v ...interface{}) {
	l.l.logWithPrefix(errorLevel, l.prefix, v...)
}

func (l *prefixLoggerWrapper) Errorf(format string, v ...interface{}) {
	l.l.logfWithPrefix(errorLevel, format, l.prefix, v...)
}

func (l *prefixLoggerWrapper) Fatal(v ...interface{}) {
	l.l.logWithPrefix(fatalLevel, l.prefix, v...)
	os.Exit(-1)
}

func (l *prefixLoggerWrapper) Fatalf(format string, v ...interface{}) {
	l.l.logfWithPrefix(fatalLevel, format, l.prefix, v...)
	os.Exit(-1)
}
