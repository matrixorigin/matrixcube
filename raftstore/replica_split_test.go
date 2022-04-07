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

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft/v3"
)

func TestTryCheckSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, cancel := newTestStore(t)
	defer cancel()

	pr := newTestReplica(Shard{ID: 1}, Replica{ID: 2}, s)
	pr.leaderID = 1

	// check not leader
	assert.False(t, pr.tryCheckSplit(action{actionType: checkSplitAction}))

	// check approximateSize
	pr.replicaID = 1
	pr.replica.ID = 1
	pr.stats.approximateSize = 100
	pr.feature.ShardSplitCheckBytes = 200
	assert.False(t, pr.tryCheckSplit(action{actionType: checkSplitAction}))

	pr.feature.ShardSplitCheckBytes = 99
	pr.rn, _ = raft.NewRawNode(getRaftConfig(pr.replicaID, 0, pr.lr, &pr.cfg, log.Adjust(nil)))
	assert.True(t, pr.tryCheckSplit(action{actionType: checkSplitAction, actionCallback: func(v interface{}) {
		assert.Equal(t, pr.getShard(), v)
	}}))
}

func TestDoSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, cancel := newTestStore(t)
	defer cancel()

	pr := newTestReplica(Shard{ID: 1, Epoch: Epoch{Generation: 2}}, Replica{ID: 2}, s)
	pr.leaderID = 1

	act := action{actionType: splitAction}
	act.splitCheckData.keys = 1
	act.splitCheckData.size = 100

	// check not leader
	pr.doSplit(act)
	assert.Equal(t, uint64(0), pr.stats.approximateSize)
	assert.Equal(t, uint64(0), pr.stats.approximateKeys)

	pr.leaderID = 2

	// check stale epoch
	pr.doSplit(act)
	assert.Equal(t, uint64(0), pr.stats.approximateSize)
	assert.Equal(t, uint64(0), pr.stats.approximateKeys)

	// check no split keys, only change memory fields
	act.epoch = pr.getShard().Epoch
	pr.doSplit(act)
	assert.Equal(t, pr.stats.approximateSize, act.splitCheckData.size)
	assert.Equal(t, pr.stats.approximateKeys, act.splitCheckData.keys)

	// check split panic, len(splitIDs) == len(splitKeys)+1
	ch := make(chan bool)
	act.splitCheckData.splitIDs = []rpcpb.SplitID{{NewID: 100, NewReplicaIDs: []uint64{1000}}}
	act.splitCheckData.splitKeys = [][]byte{{0}}
	go func() {
		defer func() {
			if err := recover(); err != nil {
				ch <- true
			} else {
				ch <- false
			}
		}()
		pr.doSplit(act)
	}()
	assert.True(t, <-ch)

	// check split
	act.splitCheckData.splitIDs = []rpcpb.SplitID{{NewID: 100, NewReplicaIDs: []uint64{1000}}, {NewID: 200, NewReplicaIDs: []uint64{2000}}}
	act.splitCheckData.splitKeys = [][]byte{{1}}
	pr.doSplit(act)
	assert.Equal(t, int64(1), pr.requests.Len())
	v, err := pr.requests.Peek()
	assert.NoError(t, err)
	var req rpcpb.BatchSplitRequest
	protoc.MustUnmarshal(&req, v.(reqCtx).req.Cmd)
	assert.Equal(t, rpcpb.CmdBatchSplit, rpcpb.InternalCmd(v.(reqCtx).req.CustomType))
	assert.Equal(t, 2, len(req.Requests))
	assert.Equal(t, pr.getShard().Start, req.Requests[0].Start)
	assert.Equal(t, act.splitCheckData.splitKeys[0], req.Requests[0].End)
	assert.Equal(t, act.splitCheckData.splitIDs[0].NewID, req.Requests[0].NewShardID)
	assert.Equal(t, act.splitCheckData.splitKeys[0], req.Requests[1].Start)
	assert.Equal(t, pr.getShard().End, req.Requests[1].End)
	assert.Equal(t, act.splitCheckData.splitIDs[1].NewID, req.Requests[1].NewShardID)
}
