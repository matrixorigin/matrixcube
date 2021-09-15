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
