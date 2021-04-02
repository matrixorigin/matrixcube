package goetty

import (
	"log"
)

// Logger logger
type Logger interface {
	Infof(format string, v ...interface{})
	Debugf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
	Fatalf(format string, v ...interface{})
}

func newStdLog() Logger {
	return &stdLog{}
}

type stdLog struct{}

func (l *stdLog) Debugf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func (l *stdLog) Infof(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func (l *stdLog) Errorf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func (l *stdLog) Fatalf(format string, v ...interface{}) {
	log.Panicf(format, v...)
}
