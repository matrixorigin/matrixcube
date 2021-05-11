package transport

import (
	"time"

	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
)

// Option transport option
type Option func(*options)

type options struct {
	maxBodySize      int
	readTimeout      time.Duration
	writeTimeout     time.Duration
	sendBatch        int64
	raftWorkerCount  uint64
	snapWorkerCount  uint64
	errorHandlerFunc func(*bhraftpb.RaftMessage, error)
}

// WithTimeout set read and write timeout for rpc
func WithTimeout(read, write time.Duration) Option {
	return func(opts *options) {
		opts.readTimeout = read
		opts.writeTimeout = write
	}
}

// WithSendBatch set batch size for sending messages
func WithSendBatch(value int64) Option {
	return func(opts *options) {
		opts.sendBatch = value
	}
}

// WithMaxBodyBytes set max body bytes for decode message
func WithMaxBodyBytes(value int) Option {
	return func(opts *options) {
		opts.maxBodySize = value
	}
}

// WithWorkerCount set worker count for send raft messages
func WithWorkerCount(raft, snap uint64) Option {
	return func(opts *options) {
		opts.raftWorkerCount = raft
		opts.snapWorkerCount = snap
	}
}

// WithErrorHandler set error handler
func WithErrorHandler(value func(*bhraftpb.RaftMessage, error)) Option {
	return func(opts *options) {
		opts.errorHandlerFunc = value
	}
}
