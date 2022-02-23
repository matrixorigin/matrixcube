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

package schedule

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/stretchr/testify/assert"
)

type mockSplitShardsHandler struct {
	// resourceID -> startKey, endKey
	resources map[uint64][2][]byte
}

func newMockSplitShardsHandler() *mockSplitShardsHandler {
	return &mockSplitShardsHandler{
		resources: map[uint64][2][]byte{},
	}
}

// SplitShardByKeys mock SplitresourcesHandler
func (m *mockSplitShardsHandler) SplitShardByKeys(resource *core.CachedShard, splitKeys [][]byte) error {
	m.resources[resource.Meta.GetID()] = [2][]byte{
		resource.GetStartKey(),
		resource.GetEndKey(),
	}
	return nil
}

// WatchresourcesByKeyRange mock SplitresourcesHandler
func (m *mockSplitShardsHandler) ScanShardsByKeyRange(group uint64, groupKeys *resourceGroupKeys, results *splitKeyResults) {
	splitKeys := groupKeys.keys
	startKey, endKey := groupKeys.resource.GetStartKey(), groupKeys.resource.GetEndKey()
	for resourceID, keyRange := range m.resources {
		if bytes.Equal(startKey, keyRange[0]) && bytes.Equal(endKey, keyRange[1]) {
			resources := make(map[uint64][]byte)
			for i := 0; i < len(splitKeys); i++ {
				resources[resourceID+uint64(i)+1000] = splitKeys[i]
			}
			results.addShardsID(resources)
		}
	}
	groupKeys.finished = true
}

func TestShardSplitter(t *testing.T) {
	ctx := context.Background()
	opt := config.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(opt)
	handler := newMockSplitShardsHandler()
	tc.AddLeaderShardWithRange(1, "eee", "hhh", 2, 3, 4)
	splitter := NewShardSplitter(tc, handler)
	newresources := map[uint64]struct{}{}
	// assert success
	failureKeys := splitter.splitShardsByKeys(ctx, 0, [][]byte{[]byte("fff"), []byte("ggg")}, newresources)
	assert.Empty(t, failureKeys)
	assert.Equal(t, 2, len(newresources))

	percentage, newresourcesID := splitter.SplitShards(ctx, 0, [][]byte{[]byte("fff"), []byte("ggg")}, 1)
	assert.Equal(t, 100, percentage)
	assert.Equal(t, 2, len(newresourcesID))
	// assert out of range
	newresources = map[uint64]struct{}{}
	failureKeys = splitter.splitShardsByKeys(ctx, 0, [][]byte{[]byte("aaa"), []byte("bbb")}, newresources)
	assert.Equal(t, len(failureKeys), 2)
	assert.Empty(t, len(newresources))

	percentage, newresourcesID = splitter.SplitShards(ctx, 0, [][]byte{[]byte("aaa"), []byte("bbb")}, 1)
	assert.Equal(t, 0, percentage)
	assert.Empty(t, newresourcesID)
}

func TestGroupKeysByShard(t *testing.T) {
	opt := config.NewTestOptions()
	opt.SetPlacementRuleEnabled(false)
	tc := mockcluster.NewCluster(opt)
	handler := newMockSplitShardsHandler()
	tc.AddLeaderShardWithRange(1, "aaa", "ccc", 2, 3, 4)
	tc.AddLeaderShardWithRange(2, "ccc", "eee", 2, 3, 4)
	tc.AddLeaderShardWithRange(3, "fff", "ggg", 2, 3, 4)
	splitter := NewShardSplitter(tc, handler)
	groupKeys := splitter.groupKeysByShard(0, [][]byte{
		[]byte("bbb"),
		[]byte("ddd"),
		[]byte("fff"),
		[]byte("zzz"),
	})
	assert.Equal(t, 3, len(groupKeys))
	for k, v := range groupKeys {
		switch k {
		case uint64(1):
			assert.Equal(t, 1, len(v.keys))
			assert.True(t, reflect.DeepEqual([]byte("bbb"), v.keys[0]))
		case uint64(2):
			assert.Equal(t, 1, len(v.keys))
			assert.True(t, reflect.DeepEqual([]byte("ddd"), v.keys[0]))
		case uint64(3):
			assert.Equal(t, 1, len(v.keys))
			assert.True(t, reflect.DeepEqual([]byte("fff"), v.keys[0]))
		}
	}
}
