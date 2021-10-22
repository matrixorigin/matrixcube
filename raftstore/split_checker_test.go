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

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockclient"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/stretchr/testify/assert"
)

func TestSplitCheckerAdd(t *testing.T) {
	sc := newSplitChecker(1, 100, nil, nil)

	sc.add(Shard{})
	assert.Equal(t, 0, len(sc.shardsC))

	sc.mu.running = true
	for i := 0; i < 10; i++ {
		sc.add(Shard{ID: uint64(i)})
		assert.Equal(t, 1, len(sc.shardsC))
	}
}

func TestSplitCheckerStartAndClose(t *testing.T) {
	sc := newSplitChecker(1, 100, nil, nil)

	assert.False(t, sc.mu.running)
	sc.start()
	assert.True(t, sc.mu.running)

	sc.close()
	assert.False(t, sc.mu.running)
}

func TestSplitCheckerDoCheck(t *testing.T) {
	var currentSize uint64
	var currentKeys uint64
	var splitKeys [][]byte
	var err error
	trg := newTestReplicaGetter()
	sc := newSplitChecker(1, 100, trg, func(group uint64) splitCheckFunc {
		return func(start, end []byte, size uint64) (uint64, uint64, [][]byte, error) {
			return currentSize, currentKeys, splitKeys, err
		}
	})

	assert.False(t, sc.doChecker(Shard{}))

	s := NewSingleTestClusterStore(t).GetStore(0).(*store)
	pr := newTestReplica(Shard{ID: 1, Epoch: Epoch{Version: 1}}, Replica{ID: 1}, s)
	trg.replicas[1] = pr
	assert.False(t, sc.doChecker(Shard{}))
	assert.True(t, sc.doChecker(pr.getShard()))
	assert.Equal(t, int64(1), pr.actions.Len())
	act, _ := pr.actions.Peek()
	pr.actions.Get(1, make([]interface{}, 1))
	assert.Equal(t, action{actionType: splitAction, epoch: pr.getShard().Epoch, splitCheckData: splitCheckData{keys: currentKeys, size: currentSize, splitKeys: splitKeys}}, act)

	splitKeys = [][]byte{{0}}
	splitIDs := []rpcpb.SplitID{{NewID: 1, NewReplicaIDs: []uint64{1, 2, 3}}}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	client := mockclient.NewMockClient(ctrl)
	client.EXPECT().AskBatchSplit(gomock.Any(), gomock.Any()).Return(splitIDs, nil)
	pr.prophetClient = client
	assert.True(t, sc.doChecker(pr.getShard()))
	assert.Equal(t, int64(1), pr.actions.Len())
	act, _ = pr.actions.Peek()
	assert.Equal(t, action{actionType: splitAction, epoch: pr.getShard().Epoch, splitCheckData: splitCheckData{keys: currentKeys, size: currentSize, splitKeys: splitKeys, splitIDs: splitIDs}}, act)

}
