package storage

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/election"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/mock"
	"github.com/stretchr/testify/assert"
)

func TestPutAndGetResource(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	e, err := election.NewElector(client)
	assert.NoError(t, err, "TestPutAndGetResource failed")
	ls := e.CreateLeadship("prophet", "node1", "node1", true, func(string) bool { return true }, func(string) bool { return true })
	defer ls.Stop()

	go ls.ElectionLoop(context.Background())
	time.Sleep(time.Millisecond * 200)

	storage := NewStorage("/root", NewEtcdKV("/root", client, ls), metadata.NewTestAdapter())
	id := uint64(1)
	assert.NoError(t, storage.PutResource(metadata.NewTestResource(id)), "TestPutAndGetResource failed")

	v, err := storage.GetResource(id)
	assert.NoError(t, err, "TestPutAndGetResource failed")
	assert.Equal(t, id, v.ID(), "TestPutAndGetResource failed")
}

func TestPutAndGetContainer(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	e, err := election.NewElector(client)
	assert.NoError(t, err, "TestPutAndGetContainer failed")
	ls := e.CreateLeadship("prophet", "node1", "node1", true, func(string) bool { return true }, func(string) bool { return true })
	defer ls.Stop()

	go ls.ElectionLoop(context.Background())
	time.Sleep(time.Millisecond * 200)

	storage := NewStorage("/root", NewEtcdKV("/root", client, ls), metadata.NewTestAdapter())
	id := uint64(1)
	assert.NoError(t, storage.PutContainer(metadata.NewTestContainer(id)), "TestPutAndGetContainer failed")

	v, err := storage.GetContainer(id)
	assert.NoError(t, err, "TestPutAndGetContainer failed")
	assert.Equal(t, id, v.ID(), "TestPutAndGetContainer failed")
}

func TestLoadResources(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	e, err := election.NewElector(client)
	assert.NoError(t, err, "TestLoadResources failed")
	ls := e.CreateLeadship("prophet", "node1", "node1", true, func(string) bool { return true }, func(string) bool { return true })
	defer ls.Stop()

	go ls.ElectionLoop(context.Background())
	time.Sleep(time.Millisecond * 200)

	s := NewStorage("/root", NewEtcdKV("/root", client, ls), metadata.NewTestAdapter())

	var values []metadata.Resource
	cb := func(v metadata.Resource) {
		values = append(values, v)
	}

	err = s.LoadResources(1, cb)
	assert.NoError(t, err, "TestLoadResources failed")
	assert.Empty(t, values, "TestLoadResources failed")

	n := 10
	for i := 0; i < n; i++ {
		assert.NoError(t, s.PutResource(metadata.NewTestResource(uint64(i))), "TestLoadResources failed")
	}
	err = s.LoadResources(1, cb)
	assert.NoError(t, err, "TestLoadResources failed")
	assert.Equal(t, n, len(values), "TestLoadResources failed")
}

func TestLoadContainers(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	e, err := election.NewElector(client)
	assert.NoError(t, err, "TestLoadContainers failed")
	ls := e.CreateLeadship("prophet", "node1", "node1", true, func(string) bool { return true }, func(string) bool { return true })
	defer ls.Stop()

	go ls.ElectionLoop(context.Background())
	time.Sleep(time.Millisecond * 200)

	s := NewStorage("/root", NewEtcdKV("/root", client, ls), metadata.NewTestAdapter())

	var values []metadata.Container
	cb := func(v metadata.Container, lw, cw float64) {
		values = append(values, v)
	}

	err = s.LoadContainers(1, cb)
	assert.NoError(t, err, "TestLoadContainers failed")
	assert.Empty(t, values, "TestLoadContainers failed")

	n := 10
	for i := 0; i < n; i++ {
		s.PutContainer(metadata.NewTestContainer(uint64(i)))
	}
	err = s.LoadContainers(1, cb)
	assert.NoError(t, err, "TestLoadContainers failed")
	assert.Equal(t, n, len(values), "TestLoadContainers failed")
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

	go ls.ElectionLoop(context.Background())
	time.Sleep(time.Millisecond * 200)

	s := NewStorage("/root", NewEtcdKV("/root", client, ls), metadata.NewTestAdapter())
	yes, err := s.AlreadyBootstrapped()
	assert.NoError(t, err, "TestAlreadyBootstrapped failed")
	assert.False(t, yes, "TestAlreadyBootstrapped failed")

	var reses []metadata.Resource
	for i := 0; i < 10; i++ {
		res := metadata.NewTestResource(uint64(i + 1))
		reses = append(reses, res)
	}
	yes, err = s.PutBootstrapped(metadata.NewTestContainer(1), reses...)
	assert.NoError(t, err, "TestAlreadyBootstrapped failed")
	assert.True(t, yes, "TestAlreadyBootstrapped failed")
	c := 0
	err = s.LoadResources(8, func(res metadata.Resource) {
		c++
	})
	assert.NoError(t, err, "TestAlreadyBootstrapped failed")
	assert.Equal(t, 10, c, "TestAlreadyBootstrapped failed")

	yes, err = s.AlreadyBootstrapped()
	assert.NoError(t, err, "TestAlreadyBootstrapped failed")
	assert.True(t, yes, "TestAlreadyBootstrapped failed")
}

func TestPutAndGetTimestamp(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	e, err := election.NewElector(client)
	assert.NoError(t, err, "TestPutAndGetTimestamp failed")
	ls := e.CreateLeadship("prophet", "node1", "node1", true, func(string) bool { return true }, func(string) bool { return true })
	defer ls.Stop()

	go ls.ElectionLoop(context.Background())
	time.Sleep(time.Millisecond * 200)

	s := NewStorage("/root", NewEtcdKV("/root", client, ls), metadata.NewTestAdapter())

	now := time.Now()
	assert.NoError(t, s.PutTimestamp(now), "TestPutAndGetTimestamp failed")

	v, err := s.GetTimestamp()
	assert.NoError(t, err, "TestPutAndGetTimestamp failed")

	assert.Equal(t, now.Nanosecond(), v.Nanosecond(), "TestPutAndGetTimestamp failed")
}

func TestPutAndDeleteAndLoadJobs(t *testing.T) {
	stopC, port := mock.StartTestSingleEtcd(t)
	defer close(stopC)

	client := mock.NewEtcdClient(t, port)
	defer client.Close()

	e, err := election.NewElector(client)
	assert.NoError(t, err, "TestPutAndGetContainer failed")
	ls := e.CreateLeadship("prophet", "node1", "node1", true, func(string) bool { return true }, func(string) bool { return true })
	defer ls.Stop()

	go ls.ElectionLoop(context.Background())
	time.Sleep(time.Millisecond * 200)

	storage := NewStorage("/root", NewEtcdKV("/root", client, ls), metadata.NewTestAdapter())
	assert.NoError(t, storage.PutJob([]byte("job1"), []byte("job1")))
	assert.NoError(t, storage.PutJob([]byte("job2"), []byte("job2")))
	assert.NoError(t, storage.PutJob([]byte("job3"), []byte("job3")))

	var loadedValues [][]byte
	assert.NoError(t, storage.LoadJobs(1, func(k, v []byte) {
		loadedValues = append(loadedValues, k)
	}))
	assert.Equal(t, 3, len(loadedValues))
	assert.Equal(t, []byte("job1"), loadedValues[0])
	assert.NoError(t, storage.RemoveJob(loadedValues[0]))
	assert.Equal(t, []byte("job2"), loadedValues[1])
	assert.NoError(t, storage.RemoveJob(loadedValues[1]))
	assert.Equal(t, []byte("job3"), loadedValues[2])
	assert.NoError(t, storage.RemoveJob(loadedValues[2]))

	c := 0
	assert.NoError(t, storage.LoadJobs(1, func(k, v []byte) {
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

	go ls.ElectionLoop(context.Background())
	time.Sleep(time.Millisecond * 200)

	storage := NewStorage("/root", NewEtcdKV("/root", client, ls), metadata.NewTestAdapter())
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

	go ls.ElectionLoop(context.Background())
	time.Sleep(time.Millisecond * 200)

	keys := [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")}
	data := [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")}
	storage := NewStorage("/root", NewEtcdKV("/root", client, ls), metadata.NewTestAdapter())
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
