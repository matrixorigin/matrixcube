// Copyright 2021 MatrixOrigin.
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

package raftstore

import (
	"testing"

	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestHandleShardHeartbeatTask(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := []struct {
		shard     Shard
		replica   Replica
		leader    uint64
		action    action
		hasAction bool
	}{
		{
			replica:   Replica{ID: 1},
			leader:    2,
			hasAction: false,
		},
		{
			replica:   Replica{ID: 1},
			leader:    1,
			hasAction: true,
			action:    action{actionType: heartbeatAction},
		},
	}

	for _, c := range cases {
		func() {
			s, cancel := newTestStore(t)
			defer cancel()
			pr := newTestReplica(c.shard, c.replica, s)
			pr.leaderID = c.leader
			s.addReplica(pr)
			s.handleShardHeartbeatTask()
			if c.hasAction {
				v, err := pr.actions.Peek()
				assert.NoError(t, err)
				assert.Equal(t, c.action, v)
			}
		}()
	}
}

func TestHandleSplitCheckTask(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := []struct {
		shard     Shard
		replica   Replica
		leader    uint64
		stats     *replicaStats
		action    action
		hasAction bool
	}{
		{
			replica:   Replica{ID: 1},
			hasAction: false,
		},
		{
			leader:    1,
			replica:   Replica{ID: 1},
			hasAction: true,
			action:    action{actionType: checkSplitAction},
		},
		{
			leader:    1,
			replica:   Replica{ID: 1},
			stats:     &replicaStats{approximateSize: 1024 * 1024 * 1024},
			hasAction: true,
			action:    action{actionType: checkSplitAction},
		},
	}

	for idx, c := range cases {
		func() {
			s, cancel := newTestStore(t)
			defer cancel()
			pr := newTestReplica(c.shard, c.replica, s)
			pr.stats = c.stats
			pr.leaderID = c.leader
			s.addReplica(pr)
			s.handleSplitCheckTask(0)
			assert.Equal(t, c.hasAction, pr.actions.Len() > 0, "index %d", idx)
			if c.hasAction {
				v, err := pr.actions.Peek()
				act := v.(action)
				act.actionCallback = nil
				assert.NoError(t, err, "index %d", idx)
				assert.Equal(t, c.action, act, "index %d", idx)
			}
		}()
	}
}

func TestHandleCompactLogTask(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cases := []struct {
		shard     Shard
		replica   Replica
		leader    uint64
		action    action
		hasAction bool
	}{
		{
			replica:   Replica{ID: 1},
			hasAction: false,
		},
		{
			leader:    1,
			replica:   Replica{ID: 1},
			hasAction: true,
			action:    action{actionType: checkCompactLogAction},
		},
	}

	for idx, c := range cases {
		func() {
			s, cancel := newTestStore(t)
			defer cancel()
			pr := newTestReplica(c.shard, c.replica, s)
			pr.leaderID = c.leader
			s.addReplica(pr)
			s.handleCompactLogTask()
			assert.Equal(t, c.hasAction, pr.actions.Len() > 0, "index %d", idx)
			if c.hasAction {
				v, err := pr.actions.Peek()
				act := v.(action)
				act.actionCallback = nil
				assert.NoError(t, err, "index %d", idx)
				assert.Equal(t, c.action, act, "index %d", idx)
			}
		}()
	}
}
