// Copyright 2020 PingCAP, Inc.
// Modifications copyright (C) 2021 MatrixOrigin.
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

package testutil

import (
	"testing"
	"time"
)

const (
	waitMaxRetry   = 200
	waitRetrySleep = time.Millisecond * 100
)

// CheckFunc is a condition checker that passed to WaitUntil. Its implementation
// may call c.Fatal() to abort the test, or c.Log() to add more information.
type CheckFunc func(t *testing.T) bool

// WaitOp represents available options when execute WaitUntil
type WaitOp struct {
	SleepInterval time.Duration
}

// WaitOption configures WaitOp
type WaitOption func(op *WaitOp)

// WithSleepInterval specify the sleep duration
func WithSleepInterval(sleep time.Duration) WaitOption {
	return func(op *WaitOp) { op.SleepInterval = sleep }
}

// WaitUntil repeatedly evaluates f() for a period of time, util it returns true.
func WaitUntil(t *testing.T, f CheckFunc, opts ...WaitOption) {
	t.Log("wait start")
	options := &WaitOp{}
	options.SleepInterval = waitRetrySleep
	for _, opt := range opts {
		opt(options)
	}
	for i := 0; i < waitMaxRetry; i++ {
		if f(t) {
			return
		}
		time.Sleep(options.SleepInterval)
	}
	t.Fatalf("wait timeout")
}
