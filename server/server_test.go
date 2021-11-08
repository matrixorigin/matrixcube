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

package server

import (
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/storage/executor/simple"
	"github.com/stretchr/testify/assert"
)

func TestExec(t *testing.T) {
	c := raftstore.NewSingleTestClusterStore(t)
	defer c.Stop()

	c.Start()
	s := NewApplication(Cfg{Store: c.GetStore(0), storeStarted: true})
	s.Start()
	defer s.Stop()

	v, err := s.Exec(newTestWriteCustomRequest("k", "v"), time.Minute)
	assert.NoError(t, err)
	assert.Equal(t, simple.OK, v)
}

func TestAsyncExec(t *testing.T) {
	c := raftstore.NewSingleTestClusterStore(t)
	defer c.Stop()

	c.Start()
	s := NewApplication(Cfg{Store: c.GetStore(0), storeStarted: true})
	s.Start()
	defer s.Stop()

	ch := make(chan struct{})
	var arg CustomRequest
	var resp []byte
	var err error
	s.AsyncExec(newTestWriteCustomRequest("k", "v"), func(a CustomRequest, r []byte, e error) {
		arg = a
		resp = r
		err = e
		ch <- struct{}{}
	}, time.Minute)
	<-ch
	assert.NoError(t, err)
	assert.Equal(t, simple.OK, resp)
	assert.Equal(t, newTestWriteCustomRequest("k", "v"), arg)
}

func newTestWriteCustomRequest(k, v string) CustomRequest {
	r := simple.NewWriteRequest([]byte(k), []byte(v))
	return CustomRequest{
		CustomType: r.CmdType,
		Cmd:        r.Cmd,
		Key:        r.Key,
		Write:      true,
	}
}
