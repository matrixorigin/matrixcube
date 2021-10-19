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

package election

import (
	"fmt"

	"github.com/matrixorigin/matrixcube/components/log"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

// ElectorOption elector option
type ElectorOption func(*electorOptions)

type electorOptions struct {
	etcd                 *embed.Etcd
	leaderPath, lockPath string
	leaseSec             int64
	lockIfBecomeLeader   bool
	logger               *zap.Logger
}

func (opts *electorOptions) adjust() {
	if opts.leaderPath == "" || opts.lockPath == "" {
		opts.leaderPath = "/electors/leader"
		opts.lockPath = "/electors/lock"
	}

	if opts.leaseSec <= 0 {
		opts.leaseSec = 5
	}

	opts.logger = log.Adjust(opts.logger).Named("elector")
}

// WithEmbedEtcd with embed etcd
func WithEmbedEtcd(etcd *embed.Etcd) ElectorOption {
	return func(opts *electorOptions) {
		opts.etcd = etcd
	}
}

// WithPrefix set data prefix in embed etcd server
func WithPrefix(value string) ElectorOption {
	return func(opts *electorOptions) {
		opts.leaderPath = fmt.Sprintf("%s/leader", value)
		opts.lockPath = fmt.Sprintf("%s/lock", value)
	}
}

// WithLeaderLeaseSeconds set leader lease in seconds
func WithLeaderLeaseSeconds(value int64) ElectorOption {
	return func(opts *electorOptions) {
		opts.leaseSec = value
	}
}

// WithLockIfBecomeLeader set lock enable flag if become leader,
// If true, will add a distributed lock, and will unlock on become follower,
// ensure that the other nodes can be changed to leaders after the previous
// leader has processed the role changes.
func WithLockIfBecomeLeader(value bool) ElectorOption {
	return func(opts *electorOptions) {
		opts.lockIfBecomeLeader = value
	}
}
