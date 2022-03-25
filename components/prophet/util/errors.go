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

package util

import (
	"errors"
	"fmt"
	"strings"
)

var (
	// ErrNotLeader error not leader
	ErrNotLeader = errors.New("election: not leader")
	// ErrNotBootstrapped not bootstrapped
	ErrNotBootstrapped = errors.New("prophet: not bootstrapped")

	// ErrReq invalid request
	ErrReq = errors.New("invalid req")
	// ErrStaleShard  stale resource
	ErrStaleShard = errors.New("stale resource")
	// ErrTombstoneStore t ombstone container
	ErrTombstoneStore = errors.New("container is tombstone")

	// ErrSchedulerExisted error with scheduler is existed
	ErrSchedulerExisted = errors.New("scheduler is existed")
	// ErrSchedulerNotFound error with scheduler is not found
	ErrSchedulerNotFound = errors.New("scheduler is not found")

	// errors related with long running job
	// ErrJobProcessorNotFound should keep compatibility with PR 1915 of matrixone
	ErrJobProcessorNotFound = errors.New("missing job processor")
	ErrJobProcessorStopped  = errors.New("job processor stopped")
	ErrJobInvalidCommand    = errors.New("invalid job command")
	ErrJobNotFound          = errors.New("job not found")
)

// IsNotLeaderError is not leader error
func IsNotLeaderError(err string) bool {
	return err == ErrNotLeader.Error()
}

// IsJobProcessorNotFoundErr check error via its string content
func IsJobProcessorNotFoundErr(err string) bool {
	return strings.Contains(err, ErrJobProcessorNotFound.Error())
}

func WrappedError(err error, msg string) error {
	return fmt.Errorf("%w: %s", err, msg)
}
