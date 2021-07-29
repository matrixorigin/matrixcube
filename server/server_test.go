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
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/fagongzi/goetty/codec"
	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/stretchr/testify/assert"
)

func TestClusterStartAndStop(t *testing.T) {
	c := raftstore.NewTestClusterStore(t, "", nil, func(node int, cfg *config.Config, store raftstore.Store, attrs map[string]interface{}) {
		h := &testHandler{
			store: store,
		}
		store.RegisterWriteFunc(1, h.set)
		store.RegisterReadFunc(2, h.get)
		attrs["app"] = NewApplication(Cfg{
			Addr:    fmt.Sprintf("127.0.0.1:808%d", node),
			Store:   store,
			Handler: h,
		})
	}, func(attrs map[string]interface{}) {
		app := attrs["app"]
		assert.NoError(t, app.(*Application).Start())
	})
	defer c.Stop()

	c.Start()
	c.WaitShardByCount(t, 1, time.Second*10)

	app := c.GetAttr(0, "app").(*Application)

	cmdSet := testRequest{
		Op:    "SET",
		Key:   "hello",
		Value: "world",
	}
	resp, err := app.Exec(&cmdSet, 10*time.Second)
	assert.NoError(t, err)
	assert.Equal(t, "OK", string(resp))

	cmdGet := testRequest{
		Op:  "GET",
		Key: "hello",
	}
	value, err := app.Exec(&cmdGet, 10*time.Second)
	assert.NoError(t, err)
	assert.Equal(t, value, []byte("world"))
	println("QSQ: ", string(value))
}

type testRequest struct {
	Op    string `json:"json:op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type testHandler struct {
	store raftstore.Store
}

func (h *testHandler) BuildRequest(req *raftcmdpb.Request, msg interface{}) error {
	cmd := msg.(*testRequest)

	cmdName := strings.ToUpper(cmd.Op)

	if cmdName != "SET" && cmdName != "GET" {
		return fmt.Errorf("%s not support", cmd)
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	req.Key = []byte(cmd.Key)
	switch cmdName {
	case "SET":
		req.CustemType = 1
		req.Type = raftcmdpb.CMDType_Write
	case "GET":
		req.CustemType = 2
		req.Type = raftcmdpb.CMDType_Read

	}
	req.Key = []byte(cmd.Key)
	req.Cmd = data
	return nil
}

func (h *testHandler) Codec() (codec.Encoder, codec.Decoder) {
	return nil, nil
}

func (h *testHandler) AddReadFunc(cmdType uint64, cb command.ReadCommandFunc) {
	h.store.RegisterReadFunc(cmdType, cb)
}

func (h *testHandler) AddWriteFunc(cmdType uint64, cb command.WriteCommandFunc) {
	h.store.RegisterWriteFunc(cmdType, cb)
}

func (h *testHandler) set(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (uint64, int64, *raftcmdpb.Response) {
	resp := pb.AcquireResponse()

	cmd := testRequest{}
	err := json.Unmarshal(req.Cmd, &cmd)
	if err != nil {
		resp.Value = []byte(err.Error())
		return 0, 0, resp
	}

	err = ctx.WriteBatch().Set(req.Key, []byte(cmd.Value))
	if err != nil {
		resp.Value = []byte(err.Error())
		return 0, 0, resp
	}

	writtenBytes := uint64(len(req.Key) + len(cmd.Value))
	changedBytes := int64(writtenBytes)
	resp.Value = []byte("OK")
	return writtenBytes, changedBytes, resp
}

func (h *testHandler) get(shard bhmetapb.Shard, req *raftcmdpb.Request, ctx command.Context) (*raftcmdpb.Response, uint64) {
	resp := pb.AcquireResponse()

	cmd := testRequest{}
	err := json.Unmarshal(req.Cmd, &cmd)
	if err != nil {
		resp.Value = []byte(err.Error())
		return resp, 0
	}

	value, err := h.store.DataStorageByGroup(0, 0).(storage.KVStorage).Get(req.Key)
	if err != nil {
		resp.Value = []byte(err.Error())
		return resp, 0
	}

	resp.Value = value
	return resp, uint64(len(value))
}
