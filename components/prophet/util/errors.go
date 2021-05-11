package util

import (
	"errors"
)

var (
	// ErrNotLeader error not leader
	ErrNotLeader = errors.New("election: not leader")
	// ErrNotBootstrapped not bootstrapped
	ErrNotBootstrapped = errors.New("prophet: not bootstrapped")

	// ErrReq invalid request
	ErrReq = errors.New("invalid req")
	// ErrStaleResource  stale resource
	ErrStaleResource = errors.New("stale resource")
	// ErrTombstoneContainer t ombstone container
	ErrTombstoneContainer = errors.New("container is tombstone")

	// ErrSchedulerExisted error with scheduler is existed
	ErrSchedulerExisted = errors.New("scheduler is existed")
	// ErrSchedulerNotFound error with scheduler is not found
	ErrSchedulerNotFound = errors.New("scheduler is not found")
)

// IsNotLeaderError is not leader error
func IsNotLeaderError(err string) bool {
	return err == ErrNotLeader.Error()
}
