// Copyright 2020 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package transport

import (
	"time"

	"github.com/matrixorigin/matrixcube/pb/meta"
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
	errorHandlerFunc func(meta.RaftMessage, error)
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
func WithErrorHandler(value func(meta.RaftMessage, error)) Option {
	return func(opts *options) {
		opts.errorHandlerFunc = value
	}
}
