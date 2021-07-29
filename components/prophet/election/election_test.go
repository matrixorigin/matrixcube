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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/mock"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/clientv3"
)

func waitLeaseTimeout() {
	time.Sleep(time.Second * 3)
}

type electorTester struct {
	sync.Mutex

	group     string
	id        string
	leader    bool
	leadship  *Leadership
	cancel    context.CancelFunc
	ctx       context.Context
	startedC  chan interface{}
	once      sync.Once
	blockTime time.Duration
}

func newElectorTester(group string, id string, elector Elector) *electorTester {
	ctx, cancel := context.WithCancel(context.Background())

	et := &electorTester{
		startedC: make(chan interface{}),
		group:    group,
		id:       id,
		ctx:      ctx,
		cancel:   cancel,
	}

	et.leadship = elector.CreateLeadship(group, id, id, id != "", et.becomeLeader, et.becomeFollower)
	return et
}

func (t *electorTester) start() {
	go t.leadship.ElectionLoop(t.ctx)
	<-t.startedC
}

func (t *electorTester) stop(blockTime time.Duration) {
	t.blockTime = blockTime
	t.cancel()
}

func (t *electorTester) isLeader() bool {
	t.Lock()
	defer t.Unlock()

	return t.leader
}

func (t *electorTester) becomeLeader(newLeader string) bool {
	t.Lock()
	defer t.Unlock()

	t.leader = true
	t.notifyStarted()

	if t.blockTime > 0 {
		<-time.After(t.blockTime)
	}

	return true
}

func (t *electorTester) becomeFollower(newLeader string) bool {
	t.Lock()
	defer t.Unlock()

	t.leader = false
	t.notifyStarted()

	if t.blockTime > 0 {
		<-time.After(t.blockTime)
	}

	return true
}

func (t *electorTester) notifyStarted() {
	t.once.Do(func() {
		if t.startedC != nil {
			close(t.startedC)
			t.startedC = nil
		}
	})
}

func TestElectionLoop(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	elector, err := NewElector(client, WithLeaderLeaseSeconds(1))
	assert.Nil(t, err, "create elector failed")

	value1 := newElectorTester("0", "1", elector)
	defer value1.leadship.Stop()

	value2 := newElectorTester("0", "2", elector)
	defer value2.leadship.Stop()

	value1.start()
	value2.start()

	defer value1.stop(0)
	defer value2.stop(0)

	assert.True(t, value1.isLeader(), "value1 must be leader")
	assert.False(t, value2.isLeader(), "value2 must be follower")

	value3 := newElectorTester("0", "", elector)
	value3.start()
	defer value3.stop(0)

	assert.False(t, value3.isLeader(), "value3 must be follower")

	value1.stop(0)
	waitLeaseTimeout()
	assert.True(t, value2.isLeader(), "value2 must be leader")

	value2.stop(0)
	waitLeaseTimeout()
	assert.False(t, value2.isLeader(), "value2 must be follower")
	assert.False(t, value3.isLeader(), "value3 must be follower")
}

func TestElectionLoopWithDistributedLock(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	elector, err := NewElector(client,
		WithLeaderLeaseSeconds(1),
		WithLockIfBecomeLeader(true))
	assert.Nil(t, err, "create elector failed")

	value1 := newElectorTester("0", "1", elector)
	defer value1.leadship.Stop()

	value2 := newElectorTester("0", "2", elector)
	defer value2.leadship.Stop()

	value1.start()
	value2.start()

	defer value1.stop(0)
	defer value2.stop(0)

	assert.True(t, value1.isLeader(), "value1 must be leader")
	assert.False(t, value2.isLeader(), "value2 must be follower")

	value1.stop(time.Second * 4)
	waitLeaseTimeout()
	assert.False(t, value2.isLeader(), "value2 must be follower before distributed lock released")

	waitLeaseTimeout()
	assert.True(t, value2.isLeader(), "value2 must be leader after distributed lock released")
}

func TestChangeLeaderTo(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	elector, err := NewElector(client,
		WithLeaderLeaseSeconds(1))
	assert.Nil(t, err, "create elector failed")

	value1 := newElectorTester("0", "1", elector)
	defer value1.leadship.Stop()

	value2 := newElectorTester("0", "2", elector)
	defer value2.leadship.Stop()

	value1.start()
	value2.start()

	defer value1.stop(0)
	defer value2.stop(0)

	err = value2.leadship.ChangeLeaderTo("3")
	assert.NotNil(t, err, "only leader node can transfer leader")

	err = value1.leadship.ChangeLeaderTo("3")
	assert.Nil(t, err, "change leader failed")

	waitLeaseTimeout()
	waitLeaseTimeout()
	assert.False(t, value1.isLeader() && value2.isLeader(), "value1 or value2 must have a follower")
	assert.True(t, value1.isLeader() || value2.isLeader(), "value1 or value2 must have a leader")
}

func TestCurrentLeader(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	elector, err := NewElector(client,
		WithLeaderLeaseSeconds(1))
	assert.Nil(t, err, "create elector failed")

	value1 := newElectorTester("0", "1", elector)
	defer value1.leadship.Stop()

	value2 := newElectorTester("0", "2", elector)
	defer value2.leadship.Stop()

	value1.start()
	value2.start()

	defer value1.stop(0)
	defer value2.stop(0)

	data, _, err := value1.leadship.CurrentLeader()
	assert.Nil(t, err, "get current leader failed")
	assert.Equal(t, "1", string(data), "current leader failed")

	value1.leadship.ChangeLeaderTo("2")
	assert.Nil(t, err, "get current leader failed")

	waitLeaseTimeout()
	data, _, err = value1.leadship.CurrentLeader()
	assert.Nil(t, err, "get current leader failed")
	assert.Equalf(t, "2", string(data), "current leader failed, %+v", value2.isLeader())
}

func TestDoIfLeader(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	elector, err := NewElector(client,
		WithLeaderLeaseSeconds(1))
	assert.Nil(t, err, "create elector failed")

	value1 := newElectorTester("0", "1", elector)
	defer value1.leadship.Stop()

	value2 := newElectorTester("0", "2", elector)
	defer value2.leadship.Stop()

	value1.start()
	value2.start()

	defer value1.stop(0)
	defer value2.stop(0)

	ok, err := value1.leadship.DoIfLeader(nil, clientv3.OpPut("/key1", "value1"))
	assert.Nil(t, err, "check do if leader failed")
	assert.True(t, ok, "check do if leader failed")

	ok, err = value2.leadship.DoIfLeader(nil, clientv3.OpPut("/key2", "value2"))
	assert.Nil(t, err, "check do if leader failed")
	assert.False(t, ok, "check do if leader failed")

	ok, err = value1.leadship.DoIfLeader([]clientv3.Cmp{clientv3.Value("value2")}, clientv3.OpPut("/key1", "value2"))
	assert.Nil(t, err, "check do if leader failed")
	assert.False(t, ok, "check do if leader failed")

	ok, err = value1.leadship.DoIfLeader([]clientv3.Cmp{clientv3.Compare(clientv3.Value("/key1"), "=", "value1")}, clientv3.OpPut("/key1", "value2"))
	assert.Nil(t, err, "check do if leader failed")
	assert.True(t, ok, "check do if leader failed")
}
