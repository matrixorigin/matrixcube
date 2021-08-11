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
	"bytes"

	"github.com/fagongzi/goetty/buf"
	"github.com/fagongzi/util/uuid"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"go.etcd.io/etcd/raft"
)

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

type readIndexQueue struct {
	shardID     uint64
	reads       []cmd
	readyToRead int
}

func (q *readIndexQueue) reset() {
	q.reads = q.reads[:0]
	q.readyToRead = 0
}

func (q *readIndexQueue) push(c cmd) {
	q.reads = append(q.reads, c)
}

func (q *readIndexQueue) ready(state raft.ReadState) {
	if !bytes.Equal(state.RequestCtx, q.reads[q.readyToRead].getUUID()) {
		logger.Fatalf("shard %d apply read failed, uuid not match",
			q.shardID)
	}
	q.reads[q.readyToRead].readIndexCommittedIndex = state.Index
	q.readyToRead++
}

func (q *readIndexQueue) pop(index uint64) (cmd, bool) {
	if len(q.reads) == 0 || q.readyToRead <= 0 {
		return emptyCMD, false
	}

	if q.reads[0].readIndexCommittedIndex > index {
		return emptyCMD, false
	}

	value := q.reads[0]
	q.reads[0] = emptyCMD
	q.reads = q.reads[1:]
	q.readyToRead--
	return value, true
}

type reqCtx struct {
	admin *raftcmdpb.AdminRequest
	req   *raftcmdpb.Request
	cb    func(*raftcmdpb.RaftCMDResponse)
}

type proposeBatch struct {
	pr *peerReplica

	buf  *buf.ByteBuf
	cmds []cmd
}

func newBatch(pr *peerReplica) *proposeBatch {
	return &proposeBatch{
		pr:  pr,
		buf: buf.NewByteBuf(512),
	}
}

func (b *proposeBatch) getType(c reqCtx) int {
	if c.admin != nil {
		return admin
	}

	if c.req.Type == raftcmdpb.CMDType_Write {
		return write
	}

	return read
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

func (b *proposeBatch) push(group uint64, c reqCtx) {
	adminReq := c.admin
	req := c.req
	cb := c.cb
	tp := b.getType(c)

	isAdmin := tp == admin

	// use data key to store
	if !isAdmin {
		req.Key = getDataKey0(group, req.Key, b.buf)
		b.buf.Clear()
	}

	n := req.Size()
	added := false
	if !isAdmin {
		for idx := range b.cmds {
			if b.cmds[idx].tp == tp &&
				!b.cmds[idx].isFull(n, int(b.pr.store.cfg.Raft.MaxEntryBytes)) &&
				b.cmds[idx].canAppend(c.req) {
				b.cmds[idx].req.Requests = append(b.cmds[idx].req.Requests, req)
				b.cmds[idx].size += n
				added = true
				break
			}
		}
	}

	if !added {
		shard := b.pr.ps.shard
		raftCMD := pb.AcquireRaftCMDRequest()
		raftCMD.Header = pb.AcquireRaftRequestHeader()
		raftCMD.Header.ShardID = shard.ID
		raftCMD.Header.Peer = b.pr.peer
		raftCMD.Header.ID = uuid.NewV4().Bytes()
		raftCMD.Header.Epoch = shard.Epoch

		if isAdmin {
			raftCMD.AdminRequest = adminReq
		} else {
			raftCMD.Header.IgnoreEpochCheck = req.IgnoreEpochCheck
			raftCMD.Requests = append(raftCMD.Requests, req)
		}

		b.cmds = append(b.cmds, newCMD(raftCMD, cb, tp, n))
	}
}
