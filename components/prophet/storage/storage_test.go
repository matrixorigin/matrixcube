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

package storage

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/election"
	"github.com/matrixorigin/matrixcube/components/prophet/id"
	"github.com/matrixorigin/matrixcube/components/prophet/mock"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/stretchr/testify/assert"
)

func TestPutAndGetShard(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	e, err := election.NewElector(client)
	assert.NoError(t, err, "TestPutAndGetShard failed")
	ls := e.CreateLeadship("prophet", "node1", "node1", true, func(string) bool { return true }, func(string) bool { return true })
	defer ls.Stop()

	ls.ElectionLoop()
	time.Sleep(time.Millisecond * 200)

	rootPath := "/root"
	storage := NewStorage(
		rootPath,
		NewEtcdKV("/root", client, ls),
		id.NewEtcdGenerator(rootPath, client, ls),
	)
	id := uint64(1)
	assert.NoError(t, storage.PutShard(metapb.Shard{ID: id}), "TestPutAndGetShard failed")

	v, err := storage.GetShard(id)
	assert.NoError(t, err, "TestPutAndGetShard failed")
	assert.Equal(t, id, v.GetID(), "TestPutAndGetShard failed")
}

func TestPutAndGetStore(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	e, err := election.NewElector(client)
	assert.NoError(t, err, "TestPutAndGetStore failed")
	ls := e.CreateLeadship("prophet", "node1", "node1", true, func(string) bool { return true }, func(string) bool { return true })
	defer ls.Stop()

	ls.ElectionLoop()
	time.Sleep(time.Millisecond * 200)

	rootPath := "/root"
	storage := NewStorage(
		rootPath,
		NewEtcdKV("/root", client, ls),
		id.NewEtcdGenerator(rootPath, client, ls),
	)

	id := uint64(1)
	assert.NoError(t, storage.PutStore(metapb.Store{ID: id}), "TestPutAndGetStore failed")

	v, err := storage.GetStore(id)
	assert.NoError(t, err, "TestPutAndGetStore failed")
	assert.Equal(t, id, v.GetID(), "TestPutAndGetStore failed")
}

func TestLoadShards(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	e, err := election.NewElector(client)
	assert.NoError(t, err, "TestLoadShards failed")
	ls := e.CreateLeadship("prophet", "node1", "node1", true, func(string) bool { return true }, func(string) bool { return true })
	defer ls.Stop()

	ls.ElectionLoop()
	time.Sleep(time.Millisecond * 200)

	rootPath := "/root"
	s := NewStorage(
		rootPath,
		NewEtcdKV("/root", client, ls),
		id.NewEtcdGenerator(rootPath, client, ls),
	)

	var values []metapb.Shard
	cb := func(v metapb.Shard) {
		values = append(values, v)
	}

	err = s.LoadShards(1, cb)
	assert.NoError(t, err, "TestLoadShards failed")
	assert.Empty(t, values, "TestLoadShards failed")

	n := 10
	for i := 0; i < n; i++ {
		assert.NoError(t, s.PutShard(metapb.Shard{ID: uint64(i)}), "TestLoadShards failed")
	}
	err = s.LoadShards(1, cb)
	assert.NoError(t, err, "TestLoadShards failed")
	assert.Equal(t, n, len(values), "TestLoadShards failed")
}

func TestLoadStores(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	e, err := election.NewElector(client)
	assert.NoError(t, err, "TestLoadStores failed")
	ls := e.CreateLeadship("prophet", "node1", "node1", true, func(string) bool { return true }, func(string) bool { return true })
	defer ls.Stop()

	ls.ElectionLoop()
	time.Sleep(time.Millisecond * 200)

	rootPath := "/root"
	s := NewStorage(
		rootPath,
		NewEtcdKV("/root", client, ls),
		id.NewEtcdGenerator(rootPath, client, ls),
	)

	var values []metapb.Store
	cb := func(v metapb.Store, lw, cw float64) {
		values = append(values, v)
	}

	err = s.LoadStores(1, cb)
	assert.NoError(t, err, "TestLoadStores failed")
	assert.Empty(t, values, "TestLoadStores failed")

	n := 10
	for i := 0; i < n; i++ {
		s.PutStore(metapb.Store{ID: uint64(i)})
	}
	err = s.LoadStores(1, cb)
	assert.NoError(t, err, "TestLoadStores failed")
	assert.Equal(t, n, len(values), "TestLoadStores failed")
}

func TestAlreadyBootstrapped(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	e, err := election.NewElector(client)
	assert.Nil(t, err, "TestAlreadyBootstrapped failed")
	ls := e.CreateLeadship("prophet", "node1", "node1", true, func(string) bool { return true }, func(string) bool { return true })
	defer ls.Stop()

	ls.ElectionLoop()
	time.Sleep(time.Millisecond * 200)

	rootPath := "/root"
	s := NewStorage(
		rootPath,
		NewEtcdKV("/root", client, ls),
		id.NewEtcdGenerator(rootPath, client, ls),
	)

	yes, err := s.AlreadyBootstrapped()
	assert.NoError(t, err, "TestAlreadyBootstrapped failed")
	assert.False(t, yes, "TestAlreadyBootstrapped failed")

	var reses []*metapb.Shard
	for i := 0; i < 10; i++ {
		res := &metapb.Shard{ID: uint64(i + 1)}
		reses = append(reses, res)
	}
	yes, err = s.PutBootstrapped(metapb.Store{ID: 1}, reses...)
	assert.NoError(t, err, "TestAlreadyBootstrapped failed")
	assert.True(t, yes, "TestAlreadyBootstrapped failed")
	c := 0
	err = s.LoadShards(8, func(res metapb.Shard) {
		c++
	})
	assert.NoError(t, err, "TestAlreadyBootstrapped failed")
	assert.Equal(t, 10, c, "TestAlreadyBootstrapped failed")

	yes, err = s.AlreadyBootstrapped()
	assert.NoError(t, err, "TestAlreadyBootstrapped failed")
	assert.True(t, yes, "TestAlreadyBootstrapped failed")
}

func TestPutAndDeleteAndLoadJobs(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	e, err := election.NewElector(client)
	assert.NoError(t, err, "TestPutAndDeleteAndLoadJobs failed")
	ls := e.CreateLeadship("prophet", "node1", "node1", true, func(string) bool { return true }, func(string) bool { return true })
	defer ls.Stop()

	ls.ElectionLoop()
	time.Sleep(time.Millisecond * 200)

	rootPath := "/root"
	storage := NewStorage(
		rootPath,
		NewEtcdKV("/root", client, ls),
		id.NewEtcdGenerator(rootPath, client, ls),
	)

	assert.NoError(t, storage.PutJob(metapb.Job{Type: metapb.JobType(1), Content: []byte("job1")}))
	assert.NoError(t, storage.PutJob(metapb.Job{Type: metapb.JobType(2), Content: []byte("job2")}))
	assert.NoError(t, storage.PutJob(metapb.Job{Type: metapb.JobType(3), Content: []byte("job3")}))
	var loadedValues []metapb.Job
	assert.NoError(t, storage.LoadJobs(1, func(job metapb.Job) {
		loadedValues = append(loadedValues, job)
	}))
	assert.Equal(t, 3, len(loadedValues))
	assert.Equal(t, []byte("job1"), loadedValues[0].Content)
	assert.NoError(t, storage.RemoveJob(loadedValues[0].Type))
	assert.Equal(t, []byte("job2"), loadedValues[1].Content)
	assert.NoError(t, storage.RemoveJob(loadedValues[1].Type))
	assert.Equal(t, []byte("job3"), loadedValues[2].Content)
	assert.NoError(t, storage.RemoveJob(loadedValues[2].Type))

	c := 0
	assert.NoError(t, storage.LoadJobs(1, func(job metapb.Job) {
		c++
	}))
	assert.Equal(t, 0, c)
}

func TestScheduleGroupRule(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	e, err := election.NewElector(client)
	assert.NoError(t, err, "TestScheduleGroupRule failed")
	ls := e.CreateLeadship("prophet", "node1", "node1", true, func(string) bool { return true }, func(string) bool { return true })
	defer ls.Stop()

	ls.ElectionLoop()
	time.Sleep(time.Millisecond * 200)

	rootPath := "/root"
	storage := NewStorage(
		rootPath,
		NewEtcdKV("/root", client, ls),
		id.NewEtcdGenerator(rootPath, client, ls),
	)

	// try to add 10 rules
	for id := 10; id < 20; id++ {
		rule := metapb.ScheduleGroupRule{
			ID:           uint64(id),
			GroupID:      uint64(id),
			Name:         "RuleTable",
			GroupByLabel: "LabelTable",
		}
		err = storage.PutScheduleGroupRule(rule)
		assert.NoError(t, err)

		// duplicated add
		err = storage.PutScheduleGroupRule(rule)
		assert.NoError(t, err)
	}

	ruleCache := core.NewScheduleGroupRuleCache()
	err = storage.LoadScheduleGroupRules(256, func(rule metapb.ScheduleGroupRule) {
		ruleCache.AddRule(rule)
	})
	assert.NoError(t, err)
	assert.Equal(t, ruleCache.RuleCount(), 10)
}
