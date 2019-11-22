package prophet

import (
	stdLog "log"
)

var (
	log Logger
)

// Logger logger
type Logger interface {
	Info(v ...interface{})
	Infof(format string, v ...interface{})
	Debug(v ...interface{})
	Debugf(format string, v ...interface{})
	Warning(v ...interface{})
	Warningf(format string, v ...interface{})
	Error(v ...interface{})
	Errorf(format string, v ...interface{})
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
}

type emptyLog struct{}

func (l *emptyLog) Info(v ...interface{}) {
	stdLog.Print(v...)
}

func (l *emptyLog) Infof(format string, v ...interface{}) {
	stdLog.Printf(format, v...)
}
func (l *emptyLog) Debug(v ...interface{}) {
	stdLog.Print(v...)
}

func (l *emptyLog) Debugf(format string, v ...interface{}) {
	stdLog.Printf(format, v...)
}

func (l *emptyLog) Warning(v ...interface{}) {
	stdLog.Print(v...)
}

func (l *emptyLog) Warningf(format string, v ...interface{}) {
	stdLog.Printf(format, v...)
}

func (l *emptyLog) Error(v ...interface{}) {
	stdLog.Print(v...)
}

func (l *emptyLog) Errorf(format string, v ...interface{}) {
	stdLog.Printf(format, v...)
}

func (l *emptyLog) Fatal(v ...interface{}) {
	stdLog.Panic(v...)
}

func (l *emptyLog) Fatalf(format string, v ...interface{}) {
	stdLog.Panicf(format, v...)
}

func init() {
	log = &emptyLog{}
}

// SetLogger set the log for prophet
func SetLogger(l Logger) {
	log = l
	log.Infof("logger set")
}
