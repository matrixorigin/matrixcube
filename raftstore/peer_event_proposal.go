package raftstore

import (
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/coreos/etcd/raft"
	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/beehive/metric"
	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/fagongzi/util/protoc"
)

type requestPolicy int

const (
	readLocal             = requestPolicy(0)
	readIndex             = requestPolicy(1)
	proposeNormal         = requestPolicy(2)
	proposeTransferLeader = requestPolicy(3)
	proposeChange         = requestPolicy(4)
)

func (pr *peerReplica) handleRequest(items []interface{}) {
	for {
		size := pr.requests.Len()
		if size == 0 {
			break
		}

		n, err := pr.requests.Get(readyBatch, items)
		if err != nil {
			return
		}

		for i := int64(0); i < n; i++ {
			req := items[i].(reqCtx)
			if req.req != nil {
				if h, ok := pr.store.localHandlers[req.req.CustemType]; ok {
					rsp, err := h(pr.ps.shard, req.req)
					if err != nil {
						respWithRetry(req.req, req.cb)
					} else {
						resp(req.req, rsp, req.cb)
					}
					continue
				}
			}

			if logger.DebugEnabled() && req.req != nil {
				logger.Debugf("%s push to proposal batch", hex.EncodeToString(req.req.ID))
			}
			pr.batch.push(pr.ps.shard.Group, req)
		}
	}

	for {
		if pr.batch.isEmpty() {
			break
		}

		if c, ok := pr.batch.pop(); ok {
			pr.propose(c)
		}
	}

	size := pr.requests.Len()
	metric.SetRaftRequestQueueMetric(size)

	if size > 0 {
		pr.addEvent()
	}
}

func (pr *peerReplica) propose(c cmd) {
	if !pr.checkProposal(c) {
		return
	}

	// Note:
	// The peer that is being checked is a leader. It might step down to be a follower later. It
	// doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
	// command log entry can't be committed.
	c.term = pr.getCurrentTerm()
	isConfChange := false
	policy, err := pr.getHandlePolicy(c.req)
	if err != nil {
		resp := errorOtherCMDResp(err)
		c.resp(resp)
		return
	}

	doPropose := false
	switch policy {
	case readIndex:
		pr.execReadIndex(c)
	case readLocal:
		pr.doExecReadCmd(c)
	case proposeNormal:
		doPropose = pr.proposeNormal(c)
	case proposeTransferLeader:
		doPropose = pr.proposeTransferLeader(c)
	case proposeChange:
		isConfChange = true
		pr.metrics.admin.confChange++
		doPropose = pr.proposeConfChange(c)
	}

	if !doPropose {
		return
	}

	err = pr.startProposeJob(c, isConfChange)
	if err != nil {
		c.respOtherError(err)
	}
}

func (pr *peerReplica) execReadIndex(c cmd) {
	if !pr.isLeader() {
		target, _ := pr.store.getPeer(pr.getLeaderPeerID())
		c.respNotLeader(pr.shardID, target)
		return
	}

	lastPendingReadCount := pr.pendingReadCount()
	lastReadyReadCount := pr.readyReadCount()

	pr.rn.ReadIndex(c.getUUID())

	pendingReadCount := pr.pendingReadCount()
	readyReadCount := pr.readyReadCount()

	if pendingReadCount == lastPendingReadCount &&
		readyReadCount == lastReadyReadCount {
		target, _ := pr.store.getPeer(pr.getLeaderPeerID())
		c.respNotLeader(pr.shardID, target)
		return
	}

	pr.pendingReads.push(c)
	pr.metrics.propose.readIndex++
}

func (pr *peerReplica) proposeNormal(c cmd) bool {
	if !pr.isLeader() {
		target, _ := pr.store.getPeer(pr.getLeaderPeerID())
		c.respNotLeader(pr.shardID, target)
		return false
	}

	data := protoc.MustMarshal(c.req)
	size := len(data)
	metric.ObserveProposalBytes(int64(size))

	if size > pr.store.opts.maxProposalBytes {
		c.respLargeRaftEntrySize(pr.shardID, uint64(size))
		return false
	}

	idx := pr.nextProposalIndex()
	err := pr.rn.Propose(data)
	if err != nil {
		c.resp(errorOtherCMDResp(err))
		return false
	}
	idx2 := pr.nextProposalIndex()
	if idx == idx2 {
		target, _ := pr.store.getPeer(pr.getLeaderPeerID())
		c.respNotLeader(pr.shardID, target)
		return false
	}

	pr.metrics.propose.normal++
	return true
}

func (pr *peerReplica) proposeConfChange(c cmd) bool {
	err := pr.checkConfChange(c)
	if err != nil {
		c.respOtherError(err)
		return false
	}

	changePeer := c.req.AdminRequest.ChangePeer
	cc := new(etcdraftpb.ConfChange)
	switch changePeer.ChangeType {
	case raftcmdpb.AddNode:
		cc.Type = etcdraftpb.ConfChangeAddNode
	case raftcmdpb.RemoveNode:
		cc.Type = etcdraftpb.ConfChangeRemoveNode
	}
	cc.NodeID = changePeer.Peer.ID
	cc.Context = protoc.MustMarshal(c.req)

	idx := pr.nextProposalIndex()
	err = pr.rn.ProposeConfChange(*cc)
	if err != nil {
		c.respOtherError(err)
		return false
	}
	idx2 := pr.nextProposalIndex()
	if idx == idx2 {
		target, _ := pr.store.getPeer(pr.getLeaderPeerID())
		c.respNotLeader(pr.shardID, target)
		return false
	}

	logger.Infof("shard %d propose %s with peer %+v",
		pr.shardID,
		changePeer.ChangeType.String(),
		changePeer.Peer)

	pr.metrics.propose.confChange++
	return true
}

func (pr *peerReplica) proposeTransferLeader(c cmd) bool {
	req := c.req.AdminRequest.Transfer
	if pr.isTransferLeaderAllowed(req.Peer) {
		pr.doTransferLeader(req.Peer)
	} else {
		logger.Infof("shard %d transfer leader ignored directly, req=<%+v>",
			pr.shardID,
			req)
	}

	// transfer leader command doesn't need to replicate log and apply, so we
	// return immediately. Note that this command may fail, we can view it just as an advice
	c.resp(newAdminRaftCMDResponse(raftcmdpb.TransferLeader, &raftcmdpb.TransferLeaderResponse{}))
	return false
}

func (pr *peerReplica) doTransferLeader(peer metapb.Peer) {
	logger.Infof("shard %d transfer leader to peer %d",
		pr.shardID,
		peer.ID)

	pr.rn.TransferLeader(peer.ID)
	pr.metrics.propose.transferLeader++
}

func (pr *peerReplica) isTransferLeaderAllowed(newLeaderPeer metapb.Peer) bool {
	status := pr.rn.Status()
	if _, ok := status.Progress[newLeaderPeer.ID]; !ok {
		return false
	}

	for _, p := range status.Progress {
		if p.State == raft.ProgressStateSnapshot {
			return false
		}
	}

	lastIndex, _ := pr.ps.LastIndex()
	return lastIndex <= status.Progress[newLeaderPeer.ID].Match+pr.store.opts.maxAllowTransferLogLag
}

func (pr *peerReplica) checkProposal(c cmd) bool {
	// we handle all read, write and admin cmd here
	if c.req.Header == nil || len(c.req.Header.ID) == 0 {
		c.resp(errorOtherCMDResp(errMissingUUIDCMD))
		return false
	}

	err := pr.store.validateStoreID(c.req)
	if err != nil {
		c.respOtherError(err)
		return false
	}

	term := pr.getCurrentTerm()
	pe := pr.store.validateShard(c.req)
	if pe != nil {
		c.resp(errorPbResp(pe, c.req.Header.ID, term))
		return false
	}

	return true
}

func (pr *peerReplica) checkConfChange(c cmd) error {
	// Check whether it's safe to propose the specified conf change request.
	// It's safe iff at least the quorum of the Raft group is still healthy
	// right after that conf change is applied.
	// Define the total number of nodes in current Raft cluster to be `total`.
	// To ensure the above safety, if the cmd is
	// 1. A `AddNode` request
	//    Then at least '(total + 1)/2 + 1' nodes need to be up to date for now.
	// 2. A `RemoveNode` request
	//    Then at least '(total - 1)/2 + 1' other nodes (the node about to be removed is excluded)
	//    need to be up to date for now.

	total := len(pr.rn.Status().Progress)

	if total == 1 {
		// It's always safe if there is only one node in the cluster.
		return nil
	}

	changePeer := c.req.AdminRequest.ChangePeer
	peer := changePeer.Peer

	switch changePeer.ChangeType {
	case raftcmdpb.AddNode:
		if _, ok := pr.rn.Status().Progress[peer.ID]; !ok {
			total++
		}
	case raftcmdpb.RemoveNode:
		if _, ok := pr.rn.Status().Progress[peer.ID]; !ok {
			return nil
		}

		total--
	}

	healthy := pr.countHealthyNode()
	quorumAfterChange := total/2 + 1

	if healthy >= quorumAfterChange {
		return nil
	}

	logger.Infof("shard %d rejects unsafe conf change request, total=<%d> healthy=<%d> quorum after change=<%d>",
		pr.shardID,
		total,
		healthy,
		quorumAfterChange)

	pr.metrics.admin.confChangeReject++
	return fmt.Errorf("unsafe to perform conf change, total=<%d> healthy=<%d> quorum after change=<%d>",
		total,
		healthy,
		quorumAfterChange)
}

func (pr *peerReplica) countHealthyNode() int {
	// Count the number of the healthy nodes.
	// A node is healthy when
	// 1. it's the leader of the Raft group, which has the latest logs
	// 2. it's a follower, and it does not lag behind the leader a lot.
	//    If a snapshot is involved between it and the Raft leader, it's not healthy since
	//    it cannot works as a node in the quorum to receive replicating logs from leader.

	healthy := 0
	for _, p := range pr.rn.Status().Progress {
		if p.Match >= pr.ps.getTruncatedIndex() {
			healthy++
		}
	}

	return healthy
}

func (pr *peerReplica) getHandlePolicy(req *raftcmdpb.RaftCMDRequest) (requestPolicy, error) {
	if req.AdminRequest != nil {
		switch req.AdminRequest.CmdType {
		case raftcmdpb.ChangePeer:
			return proposeChange, nil
		case raftcmdpb.TransferLeader:
			return proposeTransferLeader, nil
		default:
			return proposeNormal, nil
		}
	}

	var isRead, isWrite bool
	for _, r := range req.Requests {
		isRead = r.Type == raftcmdpb.Read
		isWrite = r.Type == raftcmdpb.Write
	}

	if isRead && isWrite {
		return proposeNormal, errors.New("read and write can't be mixed in one batch")
	}

	if isWrite {
		return proposeNormal, nil
	}

	if pr.store.opts.customCanReadLocalFunc != nil &&
		pr.store.opts.customCanReadLocalFunc(pr.ps.shard) {
		return readLocal, nil
	}

	return readIndex, nil
}
