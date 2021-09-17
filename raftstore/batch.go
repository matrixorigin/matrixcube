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

package raftstore

import (
	"github.com/fagongzi/goetty/buf"
	"github.com/fagongzi/util/uuid"
	"github.com/matrixorigin/matrixcube/components/keys"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
)

// TODO: request type should has its own type
const (
	read = iota
	write
	admin
)

var (
	emptyCMD = cmd{}
	// testMaxProposalRequestCount just for test, how many requests can be aggregated in a batch, 0 is disabled
	testMaxProposalRequestCount = 0
)

type reqCtx struct {
	admin *raftcmdpb.AdminRequest
	req   *raftcmdpb.Request
	cb    func(*raftcmdpb.RaftCMDResponse)
}

func (c reqCtx) getType() int {
	if c.admin != nil {
		return admin
	}

	if c.req.Type == raftcmdpb.CMDType_Write {
		return write
	}

	return read
}

// TODO: rename this struct to proposalBatch
type proposeBatch struct {
	maxSize uint64
	shardID uint64
	peer    metapb.Peer
	buf     *buf.ByteBuf
	cmds    []cmd
}

func newBatch(maxSize uint64, shardID uint64, peer metapb.Peer) *proposeBatch {
	return &proposeBatch{
		maxSize: maxSize,
		shardID: shardID,
		peer:    peer,
		buf:     buf.NewByteBuf(512),
	}
}

func (b *proposeBatch) size() int {
	return len(b.cmds)
}

func (b *proposeBatch) isEmpty() bool {
	return b.size() == 0
}

func (b *proposeBatch) pop() (cmd, bool) {
	if b.isEmpty() {
		return emptyCMD, false
	}

	value := b.cmds[0]
	b.cmds[0] = emptyCMD
	b.cmds = b.cmds[1:]

	metric.SetRaftProposalBatchMetric(int64(len(value.req.Requests)))
	return value, true
}

// TODO: might make sense to move the epoch value into c.req

// push adds the specified req to a proposalBatch. The epoch value should
// reflect client's view of the shard when the request is made.
func (b *proposeBatch) push(group uint64, epoch metapb.ResourceEpoch, c reqCtx) {
	adminReq := c.admin
	req := c.req
	cb := c.cb
	tp := c.getType()

	isAdmin := tp == admin

	// use data key to store
	if !isAdmin {
		req.Key = keys.GetDataKeyWithBuf(group, req.Key, b.buf)
		b.buf.Clear()
	}

	n := req.Size()
	added := false
	if !isAdmin {
		for idx := range b.cmds {
			if b.cmds[idx].tp == tp && !b.cmds[idx].isFull(n, int(b.maxSize)) &&
				b.cmds[idx].canAppend(epoch, req) {
				b.cmds[idx].req.Requests = append(b.cmds[idx].req.Requests, req)
				b.cmds[idx].size += n
				added = true
				break
			}
		}
	}

	if !added {
		raftCMD := pb.AcquireRaftCMDRequest()
		raftCMD.Header = pb.AcquireRaftRequestHeader()
		raftCMD.Header.ShardID = b.shardID
		raftCMD.Header.Peer = b.peer
		raftCMD.Header.ID = uuid.NewV4().Bytes()
		raftCMD.Header.Epoch = epoch

		if isAdmin {
			raftCMD.AdminRequest = adminReq
		} else {
			raftCMD.Header.IgnoreEpochCheck = req.IgnoreEpochCheck
			raftCMD.Requests = append(raftCMD.Requests, req)
		}

		b.cmds = append(b.cmds, newCMD(raftCMD, cb, tp, n))
	}
}
