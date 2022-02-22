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
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	leaderTickInterval = 50 * time.Millisecond
	loopInterval       = 100 * time.Millisecond
)

// Elector a leader elector
type Elector interface {
	// CreateLeadship create a leadership
	CreateLeadship(purpose string, nodeName, nodeValue string, allowBecomeLeader bool, becomeLeaderFunc, becomeFollowerFunc func(string) bool) *Leadership
	// Client etcd clientv3
	Client() *clientv3.Client
}

type elector struct {
	sync.RWMutex
	options electorOptions
	client  *clientv3.Client
	lease   clientv3.Lease
}

// NewElector create a elector
func NewElector(client *clientv3.Client, options ...ElectorOption) (Elector, error) {
	e := &elector{
		client: client,
		lease:  clientv3.NewLease(client),
	}

	for _, opt := range options {
		opt(&e.options)
	}

	e.options.adjust()

	return e, nil
}

func (e *elector) CreateLeadship(purpose string, nodeName, nodeValue string, allowBecomeLeader bool, becomeLeaderFunc, becomeFollowerFunc func(string) bool) *Leadership {
	return newLeadership(e, purpose, nodeName, nodeValue, allowBecomeLeader, becomeLeaderFunc, becomeFollowerFunc, e.options.logger)
}

func (e *elector) Client() *clientv3.Client {
	return e.client
}

func getPurposePath(prefix string, purpose string) string {
	return fmt.Sprintf("%s/%s", prefix, purpose)
}

func getPurposeExpectPath(prefix string, purpose string) string {
	return fmt.Sprintf("%s/expect-%s", prefix, purpose)
}
