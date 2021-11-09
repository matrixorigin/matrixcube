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
	"testing"

	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/util/leaktest"
)

func TestSingleTestClusterStartAndStop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	c := NewSingleTestClusterStore(t, DiskTestCluster)
	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.CheckShardCount(1)
}

func TestClusterStartAndStop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	c := NewTestClusterStore(t)
	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.CheckShardCount(1)
}

func TestClusterStartWithMoreNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	c := NewTestClusterStore(t,
		WithTestClusterNodeCount(5),
		WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
			cfg.Prophet.Replication.MaxReplicas = 5
		}))
	c.Start()
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.CheckShardCount(1)
}

func TestClusterStartConcurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode.")
		return
	}

	defer leaktest.AfterTest(t)()

	c := NewTestClusterStore(t, DiskTestCluster)
	c.StartWithConcurrent(true)
	defer c.Stop()

	c.WaitShardByCountPerNode(1, testWaitTimeout)
	c.CheckShardCount(1)
}
