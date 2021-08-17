package server

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/fagongzi/goetty/codec"
	"github.com/matrixorigin/matrixcube/command"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/stretchr/testify/assert"
)

// TestApplicationCluster test application cluster, it based on raftstore.TestClusterStore.
type TestApplicationCluster struct {
	t                  *testing.T
	RaftCluster        *raftstore.TestRaftCluster
	Applications       []*Application
	applicationFactory func(i int, store raftstore.Store) *Application
}

// NewTestApplicationCluster returns TestApplicationCluster
func NewTestApplicationCluster(t *testing.T, applicationFactory func(i int, store raftstore.Store) *Application, opts ...raftstore.TestClusterOption) *TestApplicationCluster {
	c := &TestApplicationCluster{t: t, applicationFactory: applicationFactory}
	opts = append(opts, raftstore.WithTestClusterNodeStartFunc(func(node int, store raftstore.Store) {
		assert.NoError(t, c.Applications[node].Start())
	}))
	opts = append(opts, raftstore.WithTestClusterLogLevel("info"))
	rc := raftstore.NewTestClusterStore(t, opts...)
	rc.EveryStore(func(i int, s raftstore.Store) {
		c.Applications = append(c.Applications, applicationFactory(i, s))
	})
	c.RaftCluster = rc
	return c
}

// Start start the application cluster
func (c *TestApplicationCluster) Start() {
	c.RaftCluster.Start()
}

// Stop stop the application cluster
func (c *TestApplicationCluster) Stop() {
	c.RaftCluster.Stop()
	for _, app := range c.Applications {
		app.Stop()
	}
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
