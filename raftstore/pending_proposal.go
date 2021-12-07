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
	"bytes"

	"github.com/matrixorigin/matrixcube/pb/rpc"
)

type pendingProposals struct {
	cmds          []batch
	confChangeCmd batch
}

func newPendingProposals() *pendingProposals {
	return &pendingProposals{}
}

func (p *pendingProposals) close() {
	for _, c := range p.cmds {
		// FIXME: notifying the client shard removed is not accurate.
		// should just inform clients that shard is shutting down
		c.notifyShardRemoved()
	}
	p.confChangeCmd.notifyShardRemoved()
	p.confChangeCmd = emptyCMD
	p.cmds = p.cmds[:0]
}

func (p *pendingProposals) clear() {
	for _, c := range p.cmds {
		c.notifyStaleCmd()
	}
	if !p.confChangeCmd.requestBatch.IsEmpty() {
		p.confChangeCmd.notifyStaleCmd()
	}
	p.confChangeCmd = emptyCMD
	p.cmds = p.cmds[:0]
}

func (p *pendingProposals) pop() (batch, bool) {
	if len(p.cmds) == 0 {
		return emptyCMD, false
	}

	c := p.cmds[0]
	p.cmds[0] = emptyCMD
	p.cmds = p.cmds[1:]
	return c, true
}

func (p *pendingProposals) append(c batch) {
	p.cmds = append(p.cmds, c)
}

func (p *pendingProposals) setConfigChange(c batch) {
	cmdType := c.requestBatch.GetAdminCmdType()
	if cmdType != rpc.AdminCmdType_ConfigChange {
		panic("not a config change request")
	}
	p.confChangeCmd = c
}

func (p *pendingProposals) getConfigChange() batch {
	return p.confChangeCmd
}

func (p *pendingProposals) notify(id []byte,
	resp rpc.ResponseBatch, confChange bool) {
	if confChange {
		c := p.confChangeCmd
		if bytes.Equal(id, c.getRequestID()) {
			buildID(id, &resp)
			c.resp(resp)
			p.confChangeCmd = emptyCMD
		}
		return
	}

	for {
		c, ok := p.pop()
		if !ok || c.requestBatch.IsEmpty() {
			return
		}
		if bytes.Equal(id, c.getRequestID()) {
			buildID(id, &resp)
			c.resp(resp)
			return
		}
		c.notifyStaleCmd()
	}
}
