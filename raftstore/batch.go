package raftstore

import (
	"sync/atomic"

	"github.com/deepfabric/beehive/metric"
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/uuid"
)

const (
	read = iota
	write
	admin
)

var (
	emptyCMD = cmd{}
)

type readIndexQueue struct {
	shardID  uint64
	reads    []cmd
	readyCnt int32
}

func (q *readIndexQueue) push(c cmd) {
	q.reads = append(q.reads, c)
}

func (q *readIndexQueue) pop() (cmd, bool) {
	if len(q.reads) == 0 {
		return emptyCMD, false
	}

	value := q.reads[0]
	q.reads[0] = emptyCMD
	q.reads = q.reads[1:]
	return value, true
}

func (q *readIndexQueue) incrReadyCnt() int32 {
	q.readyCnt++
	return q.readyCnt
}

func (q *readIndexQueue) decrReadyCnt() int32 {
	q.readyCnt--
	return q.readyCnt
}

func (q *readIndexQueue) resetReadyCnt() {
	q.readyCnt = 0
}

func (q *readIndexQueue) getReadyCnt() int32 {
	return atomic.LoadInt32(&q.readyCnt)
}

func (q *readIndexQueue) size() int {
	return len(q.reads)
}

type reqCtx struct {
	admin *raftcmdpb.AdminRequest
	req   *raftcmdpb.Request
	cb    func(*raftcmdpb.RaftCMDResponse)
}

func (r *reqCtx) reset() {
	r.admin = nil
	r.cb = nil
	r.req = nil
}

type proposeBatch struct {
	pr *peerReplica

	buf  *goetty.ByteBuf
	cmds []cmd
}

func newBatch(pr *peerReplica) *proposeBatch {
	return &proposeBatch{
		pr:  pr,
		buf: goetty.NewByteBuf(512),
	}
}

func (b *proposeBatch) getType(c reqCtx) int {
	if c.admin != nil {
		return admin
	}

	if c.req.Type == raftcmdpb.Write {
		return write
	}

	return read
}

func (b *proposeBatch) size() int {
	return len(b.cmds)
}

func (b *proposeBatch) isEmpty() bool {
	return 0 == b.size()
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
			if b.cmds[idx].tp == tp && !b.cmds[idx].isFull(n, b.pr.store.opts.maxProposalBytes) {
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
		raftCMD.Header.ShardEpoch = shard.Epoch

		if isAdmin {
			raftCMD.AdminRequest = adminReq
		} else {
			raftCMD.Requests = append(raftCMD.Requests, req)
		}

		b.cmds = append(b.cmds, newCMD(raftCMD, cb, tp, n))
	}
}
