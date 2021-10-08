package server

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/fagongzi/goetty/codec"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/stretchr/testify/assert"
)

// TestApplicationCluster test application cluster, it based on raftstore.TestClusterStore.
type TestApplicationCluster struct {
	t                  *testing.T
	RaftCluster        raftstore.TestRaftCluster
	Applications       []*Application
	applicationFactory func(i int, store raftstore.Store) *Application
}

// NewTestApplicationCluster returns TestApplicationCluster
func NewTestApplicationCluster(t *testing.T, applicationFactory func(i int, store raftstore.Store) *Application, opts ...raftstore.TestClusterOption) *TestApplicationCluster {
	c := &TestApplicationCluster{t: t, applicationFactory: applicationFactory}
	opts = append(opts, raftstore.WithTestClusterNodeStartFunc(func(node int, store raftstore.Store) {
		assert.NoError(t, c.Applications[node].Start())
	}))
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

func (h *testHandler) BuildRequest(req *rpc.Request, msg interface{}) error {
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
		req.CustomType = 1
		req.Type = rpc.CmdType_Write
	case "GET":
		req.CustomType = 2
		req.Type = rpc.CmdType_Read

	}
	req.Key = []byte(cmd.Key)
	req.Cmd = data
	return nil
}

func (h *testHandler) Codec() (codec.Encoder, codec.Decoder) {
	return nil, nil
}
