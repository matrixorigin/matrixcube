// Copyright 2021 MatrixOrigin.
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

package raftstore

import (
	"testing"

	"github.com/fagongzi/util/uuid"
	"github.com/matrixorigin/matrixcube/pb/errorpb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/stretchr/testify/require"
)

func TestPendingProposalsCanBeCreated(t *testing.T) {
	p := newPendingProposals()
	require.Empty(t, p.cmds)
	require.Equal(t, cmd{}, p.confChangeCmd)
}

func TestPendingProposalAppend(t *testing.T) {
	p := newPendingProposals()
	p.append(cmd{})
	p.append(cmd{})
	require.Equal(t, 2, len(p.cmds))
}

func TestPendingProposalPop(t *testing.T) {
	p := newPendingProposals()
	cmd1 := cmd{size: 100}
	cmd2 := cmd{size: 200}
	p.append(cmd1)
	p.append(cmd2)
	require.Equal(t, 2, len(p.cmds))
	v1, ok := p.pop()
	require.True(t, ok)
	require.Equal(t, 1, len(p.cmds))
	require.Equal(t, cmd1, v1)
	v2, ok := p.pop()
	require.True(t, ok)
	require.Equal(t, 0, len(p.cmds))
	require.Equal(t, cmd2, v2)
	_, ok = p.pop()
	require.False(t, ok)
}

func TestPendingConfigChangeProposalCanBeSetAndGet(t *testing.T) {
	p := newPendingProposals()
	cmd := cmd{
		req: &raftcmdpb.RaftCMDRequest{
			AdminRequest: &raftcmdpb.AdminRequest{
				CmdType: raftcmdpb.AdminCmdType_ChangePeer,
			},
		},
	}
	p.setConfigChange(cmd)
	v := p.getConfigChange()
	require.Equal(t, cmd, v)
}

func TestPendingProposalWontAcceptRegularCmdAsConfigChanageCmd(t *testing.T) {
	cmd := cmd{
		req: &raftcmdpb.RaftCMDRequest{
			AdminRequest: &raftcmdpb.AdminRequest{
				CmdType: raftcmdpb.AdminCmdType_TransferLeader,
			},
		},
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("failed to trigger panic")
		}
	}()
	p := newPendingProposals()
	p.setConfigChange(cmd)
}

func testPendingProposalClear(t *testing.T,
	clear bool, cb func(resp *raftcmdpb.RaftCMDResponse)) {
	cmd1 := cmd{
		req: &raftcmdpb.RaftCMDRequest{
			Requests: []*raftcmdpb.Request{
				&raftcmdpb.Request{Key: getDataKey0(0, nil, acquireBuf())},
			},
			Header: &raftcmdpb.RaftRequestHeader{
				ID: uuid.NewV4().Bytes(),
			},
		},
		cb: cb,
	}
	cmd2 := cmd{
		req: &raftcmdpb.RaftCMDRequest{
			Requests: []*raftcmdpb.Request{
				&raftcmdpb.Request{Key: getDataKey0(0, nil, acquireBuf())},
			},
			Header: &raftcmdpb.RaftRequestHeader{
				ID: uuid.NewV4().Bytes(),
			},
		},
		cb: cb,
	}
	ConfChangeCmd := cmd{
		req: &raftcmdpb.RaftCMDRequest{
			AdminRequest: &raftcmdpb.AdminRequest{
				CmdType: raftcmdpb.AdminCmdType_ChangePeer,
			},
			Requests: []*raftcmdpb.Request{
				&raftcmdpb.Request{Key: getDataKey0(0, nil, acquireBuf())},
			},
			Header: &raftcmdpb.RaftRequestHeader{
				ID: uuid.NewV4().Bytes(),
			},
		},
		cb: cb,
	}
	p := newPendingProposals()
	p.append(cmd1)
	p.append(cmd2)
	p.setConfigChange(ConfChangeCmd)
	if clear {
		p.clear()
	} else {
		p.destroy()
	}
	require.Empty(t, p.cmds)
	require.Equal(t, emptyCMD, p.confChangeCmd)
}

func TestPendingProposalClear(t *testing.T) {
	check := func(resp *raftcmdpb.RaftCMDResponse) {
		require.Equal(t, 1, len(resp.Responses))
		require.Equal(t, errStaleCMD.Error(), resp.Responses[0].Error.Message)
	}
	testPendingProposalClear(t, true, check)
}

func TestPendingProposalDestroy(t *testing.T) {
	check := func(resp *raftcmdpb.RaftCMDResponse) {
		require.Equal(t, 1, len(resp.Responses))
		require.Equal(t, errShardNotFound.Error(), resp.Responses[0].Error.Message)
	}
	testPendingProposalClear(t, false, check)
}

func TestPendingProposalCanNotifyConfigChangeCmd(t *testing.T) {
	called := false
	cb := func(resp *raftcmdpb.RaftCMDResponse) {
		called = true
		require.Equal(t, 1, len(resp.Responses))
		require.Equal(t, errStaleCMD.Error(), resp.Responses[0].Error.Message)
	}
	ConfChangeCmd := cmd{
		req: &raftcmdpb.RaftCMDRequest{
			AdminRequest: &raftcmdpb.AdminRequest{
				CmdType: raftcmdpb.AdminCmdType_ChangePeer,
			},
			Requests: []*raftcmdpb.Request{
				&raftcmdpb.Request{Key: getDataKey0(0, nil, acquireBuf())},
			},
			Header: &raftcmdpb.RaftRequestHeader{
				ID: uuid.NewV4().Bytes(),
			},
		},
		cb: cb,
	}
	p := newPendingProposals()
	p.setConfigChange(ConfChangeCmd)
	resp := errorStaleCMDResp(ConfChangeCmd.getUUID())
	p.notify(ConfChangeCmd.req.Header.ID, resp, true)
	require.True(t, called)
	require.Equal(t, emptyCMD, p.confChangeCmd)
}

func TestPendingProposalCanNotifyRegularCmd(t *testing.T) {
	called := false
	staleCalled := false
	staleCB := func(resp *raftcmdpb.RaftCMDResponse) {
		staleCalled = true
		require.Equal(t, 1, len(resp.Responses))
		require.Equal(t, errStaleCMD.Error(), resp.Responses[0].Error.Message)
	}
	cb := func(resp *raftcmdpb.RaftCMDResponse) {
		called = true
		require.Equal(t, 1, len(resp.Responses))
		require.Equal(t, errShardNotFound.Error(), resp.Responses[0].Error.Message)
	}
	cmd1 := cmd{
		req: &raftcmdpb.RaftCMDRequest{
			Requests: []*raftcmdpb.Request{
				&raftcmdpb.Request{Key: getDataKey0(0, nil, acquireBuf())},
			},
			Header: &raftcmdpb.RaftRequestHeader{
				ID: uuid.NewV4().Bytes(),
			},
		},
		cb: staleCB,
	}
	cmd2 := cmd{
		req: &raftcmdpb.RaftCMDRequest{
			Requests: []*raftcmdpb.Request{
				&raftcmdpb.Request{Key: getDataKey0(0, nil, acquireBuf())},
			},
			Header: &raftcmdpb.RaftRequestHeader{
				ID: uuid.NewV4().Bytes(),
			},
		},
		cb: cb,
	}
	cmd3 := cmd{}
	p := newPendingProposals()
	p.append(cmd1)
	p.append(cmd2)
	p.append(cmd3)

	err := new(errorpb.ShardNotFound)
	err.ShardID = 100
	resp := errorPbResp(&errorpb.Error{
		Message:       errShardNotFound.Error(),
		ShardNotFound: err,
	}, cmd2.req.Header.ID)
	p.notify(cmd2.req.Header.ID, resp, false)
	require.True(t, called)
	require.True(t, staleCalled)
	require.Equal(t, 1, len(p.cmds))

	v, ok := p.pop()
	require.True(t, ok)
	require.Equal(t, cmd3, v)
}
