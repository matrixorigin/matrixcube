// Copyright 2021 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless assertd by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package raftstore

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestIssue123(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	c := NewSingleTestClusterStore(t,
		WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
			cfg.Customize.CustomInitShardsFactory = func() []Shard { return []Shard{{Start: []byte("a"), End: []byte("b")}} }
		}))

	c.Start()
	defer c.Stop()
	c.WaitShardByCountPerNode(1, testWaitTimeout)

	p, err := c.GetStore(0).CreateResourcePool(metapb.ResourcePool{
		RangePrefix: []byte("b"),
		Capacity:    20,
	})
	assert.NoError(t, err)
	assert.NotNil(t, p)

	c.WaitShardByCountPerNode(21, testWaitTimeout)

	kv := c.CreateTestKVClient(0)
	defer kv.Close()

	for i := 0; i < 20; i++ {
		s, err := p.Alloc(0, []byte(fmt.Sprintf("%d", i)))
		assert.NoError(t, err)
		assert.NoError(t, kv.Set(string(c.GetShardByID(0, s.ShardID).Start), "OK", time.Second))
	}
}

func TestIssue192(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()
	wc := make(chan struct{})
	c := NewSingleTestClusterStore(t,
		WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
			cfg.Customize.CustomInitShardsFactory = func() []Shard { return []Shard{{Start: []byte("a"), End: []byte("b")}} }
			cfg.Test.ShardPoolCreateWaitC = wc
		}))

	c.Start()
	defer c.Stop()
	c.WaitShardByCountPerNode(1, testWaitTimeout)

	p, err := c.GetStore(0).CreateResourcePool(metapb.ResourcePool{Group: 0, Capacity: 1, RangePrefix: []byte("b")})
	assert.NoError(t, err)
	assert.NotNil(t, p)

	_, err = p.Alloc(0, []byte("purpose"))
	assert.Error(t, err)
	assert.False(t, strings.Contains(err.Error(), "timeout"))
	close(wc)
}
