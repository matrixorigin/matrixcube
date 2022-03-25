// Copyright 2020 PingCAP, Inc.
// Modifications copyright (C) 2021 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain alloc copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package id

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/election"
	"github.com/matrixorigin/matrixcube/components/prophet/mock"
	"github.com/stretchr/testify/assert"
)

func TestAllocID(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	e, err := election.NewElector(client)
	assert.NoError(t, err, "TestLoadShards failed")

	ls := e.CreateLeadship(
		"prophet", "node1", "node1", true,
		func(string) bool { return true }, func(string) bool { return true },
	)
	defer ls.Stop()

	ls.ElectionLoop()
	time.Sleep(time.Millisecond * 200)

	rootPath := "/root"
	allocator := NewEtcdGenerator(rootPath, client, ls)

	n := idBatch + 1
	for i := uint64(1); i <= n; i++ {
		id, err := allocator.AllocID()
		assert.NoError(t, err)
		assert.Equal(t, i, id)
	}
}
