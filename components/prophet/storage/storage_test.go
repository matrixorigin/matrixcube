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

	"github.com/matrixorigin/matrixcube/components/prophet/election"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
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

	storage := NewStorage("/root", NewEtcdKV("/root", client, ls))
	id := uint64(1)
	assert.NoError(t, storage.PutShard(metadata.NewTestShard(id)), "TestPutAndGetShard failed")

	v, err := storage.GetShard(id)
	assert.NoError(t, err, "TestPutAndGetShard failed")
	assert.Equal(t, id, v.ID(), "TestPutAndGetShard failed")
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

	storage := NewStorage("/root", NewEtcdKV("/root", client, ls))
	id := uint64(1)
	assert.NoError(t, storage.PutStore(metadata.NewTestStore(id)), "TestPutAndGetStore failed")

	v, err := storage.GetStore(id)
	assert.NoError(t, err, "TestPutAndGetStore failed")
	assert.Equal(t, id, v.ID(), "TestPutAndGetStore failed")
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

	s := NewStorage("/root", NewEtcdKV("/root", client, ls))

	var values []*metadata.ShardWithRWLock
	cb := func(v *metadata.ShardWithRWLock) {
		values = append(values, v)
	}

	err = s.LoadShards(1, cb)
	assert.NoError(t, err, "TestLoadShards failed")
	assert.Empty(t, values, "TestLoadShards failed")

	n := 10
	for i := 0; i < n; i++ {
		assert.NoError(t, s.PutShard(metadata.NewTestShard(uint64(i))), "TestLoadShards failed")
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

	s := NewStorage("/root", NewEtcdKV("/root", client, ls))

	var values []*metadata.StoreWithRWLock
	cb := func(v *metadata.StoreWithRWLock, lw, cw float64) {
		values = append(values, v)
	}

	err = s.LoadStores(1, cb)
	assert.NoError(t, err, "TestLoadStores failed")
	assert.Empty(t, values, "TestLoadStores failed")

	n := 10
	for i := 0; i < n; i++ {
		s.PutStore(metadata.NewTestStore(uint64(i)))
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

	s := NewStorage("/root", NewEtcdKV("/root", client, ls))
	yes, err := s.AlreadyBootstrapped()
	assert.NoError(t, err, "TestAlreadyBootstrapped failed")
	assert.False(t, yes, "TestAlreadyBootstrapped failed")

	var reses []*metadata.ShardWithRWLock
	for i := 0; i < 10; i++ {
		res := metadata.NewTestShard(uint64(i + 1))
		reses = append(reses, res)
	}
	yes, err = s.PutBootstrapped(metadata.NewTestStore(1), reses...)
	assert.NoError(t, err, "TestAlreadyBootstrapped failed")
	assert.True(t, yes, "TestAlreadyBootstrapped failed")
	c := 0
	err = s.LoadShards(8, func(res *metadata.ShardWithRWLock) {
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
	assert.NoError(t, err, "TestPutAndGetStore failed")
	ls := e.CreateLeadship("prophet", "node1", "node1", true, func(string) bool { return true }, func(string) bool { return true })
	defer ls.Stop()

	ls.ElectionLoop()
	time.Sleep(time.Millisecond * 200)

	storage := NewStorage("/root", NewEtcdKV("/root", client, ls))
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

func TestPutAndDeleteAndLoadCustomData(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	e, err := election.NewElector(client)
	assert.NoError(t, err)
	ls := e.CreateLeadship("prophet", "node1", "node1", true, func(string) bool { return true }, func(string) bool { return true })
	defer ls.Stop()

	ls.ElectionLoop()
	time.Sleep(time.Millisecond * 200)

	storage := NewStorage("/root", NewEtcdKV("/root", client, ls))
	assert.NoError(t, storage.PutCustomData([]byte("k1"), []byte("v1")))
	assert.NoError(t, storage.PutCustomData([]byte("k2"), []byte("v2")))
	assert.NoError(t, storage.PutCustomData([]byte("k3"), []byte("v3")))

	var loadedValues [][]byte
	assert.NoError(t, storage.LoadCustomData(1, func(k, v []byte) error {
		loadedValues = append(loadedValues, k)
		return nil
	}))
	assert.Equal(t, 3, len(loadedValues))
	assert.Equal(t, []byte("k1"), loadedValues[0])
	assert.NoError(t, storage.RemoveCustomData(loadedValues[0]))
	assert.Equal(t, []byte("k2"), loadedValues[1])
	assert.NoError(t, storage.RemoveCustomData(loadedValues[1]))
	assert.Equal(t, []byte("k3"), loadedValues[2])
	assert.NoError(t, storage.RemoveCustomData(loadedValues[2]))

	c := 0
	assert.NoError(t, storage.LoadCustomData(1, func(k, v []byte) error {
		c++
		return nil
	}))
	assert.Equal(t, 0, c)
}

func TestBatchPutCustomData(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	e, err := election.NewElector(client)
	assert.NoError(t, err)
	ls := e.CreateLeadship("prophet", "node1", "node1", true, func(string) bool { return true }, func(string) bool { return true })
	defer ls.Stop()

	ls.ElectionLoop()
	time.Sleep(time.Millisecond * 200)

	keys := [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")}
	data := [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")}
	storage := NewStorage("/root", NewEtcdKV("/root", client, ls))
	assert.NoError(t, storage.BatchPutCustomData(keys, data))

	var loadedValues [][]byte
	assert.NoError(t, storage.LoadCustomData(1, func(k, v []byte) error {
		loadedValues = append(loadedValues, v)
		return nil
	}))
	assert.Equal(t, len(keys), len(loadedValues))
	for i := 0; i < len(keys); i++ {
		assert.Equal(t, data[i], loadedValues[i])
	}
}
