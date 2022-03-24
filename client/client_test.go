// Copyright 2020 MatrixOrigin.
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

package client

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/executor/simple"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestExec(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := raftstore.NewSingleTestClusterStore(t)
	defer c.Stop()

	c.Start()
	s := NewClient(Cfg{Store: c.GetStore(0), storeStarted: true})
	s.Start()
	defer s.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	req := newTestWriteCustomRequest("k", "v")
	f := s.Write(ctx, req.CmdType, req.Cmd, WithRouteKey(req.Key))
	defer f.Close()
	v, err := f.Get()
	assert.NoError(t, err)
	assert.Equal(t, simple.OK, v)
}

func TestExecWithTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := raftstore.NewSingleTestClusterStore(t)
	defer c.Stop()

	c.Start()
	s := NewClient(Cfg{Store: c.GetStore(0), storeStarted: true})
	s.Start()
	defer s.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*10)
	time.Sleep(time.Millisecond * 10)
	defer cancel()

	req := newTestWriteCustomRequest("k", "v")
	f := s.Write(ctx, req.CmdType, req.Cmd, WithRouteKey(req.Key))
	defer f.Close()

	v, err := f.Get()
	assert.Error(t, err)
	assert.Empty(t, v)
}

func TestAddShardLabel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := raftstore.NewSingleTestClusterStore(t)
	defer c.Stop()

	c.Start()
	s := NewClient(Cfg{Store: c.GetStore(0), storeStarted: true})
	s.Start()
	defer s.Stop()

	c.WaitShardByCount(1, time.Minute)

	sid := c.GetShardByIndex(0, 0).ID
	assert.NotEqual(t, 0, sid)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	f := s.AddLabelToShard(ctx, "l1", "v1", sid)
	defer f.Close()

	_, err := f.Get()
	assert.NoError(t, err)

	c.WaitShardByLabel(sid, "l1", "v1", time.Minute)
}

func newTestWriteCustomRequest(k, v string) storage.Request {
	return simple.NewWriteRequest([]byte(k), []byte(v))
}
