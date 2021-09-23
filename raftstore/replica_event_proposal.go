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
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/raft/v3/tracker"
)

type confChangeKind int

var (
	simpleKind     = confChangeKind(0)
	enterJointKind = confChangeKind(1)
	leaveJointKind = confChangeKind(2)
)

func getConfChangeKind(changeNum int) confChangeKind {
	if changeNum == 0 {
		return leaveJointKind
	}

	if changeNum == 1 {
		return simpleKind
	}

	return enterJointKind
}

type requestPolicy int

const (
	readIndex             = requestPolicy(1)
	proposeNormal         = requestPolicy(2)
	proposeTransferLeader = requestPolicy(3)
	proposeChange         = requestPolicy(4)
)

func (pr *replica) handleRequest(items []interface{}) {
	shard := pr.getShard()
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
			if logger.DebugEnabled() && req.req != nil {
				logger.Debugf("%s push to proposal batch", hex.EncodeToString(req.req.ID))
			}
			// FIXME: still using the current epoch here. should use epoch value
			// returned when routing the request.
			pr.batch.push(pr.getShard().Group, shard.Epoch, req)
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

func (pr *replica) propose(c batch) {
	if !pr.checkProposal(c) {
		return
	}

	// after propose, raft need to send message to peers
	defer pr.addEvent()

	// Note:
	// The peer that is being checked is a leader. It might step down to be a follower later. It
	// doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
	// command log entry can't be committed.
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

func (pr *replica) execReadIndex(c batch) {
	if c.tp != read {
		panic("not a read index request")
	}
	if !pr.isLeader() {
		target, _ := pr.store.getPeer(pr.getLeaderPeerID())
		c.respNotLeader(pr.shardID, target)
		return
	}

	lastPendingReadCount := pr.pendingReadCount()
	lastReadyReadCount := pr.readyReadCount()

	pr.rn.ReadIndex(protoc.MustMarshal(c.req))

	pendingReadCount := pr.pendingReadCount()
	readyReadCount := pr.readyReadCount()

	if pendingReadCount == lastPendingReadCount &&
		readyReadCount == lastReadyReadCount {
		target, _ := pr.store.getPeer(pr.getLeaderPeerID())
		c.respNotLeader(pr.shardID, target)
		return
	}
	pr.metrics.propose.readIndex++
}

func (pr *replica) proposeNormal(c batch) bool {
	if !pr.isLeader() {
		target, _ := pr.store.getPeer(pr.getLeaderPeerID())
		c.respNotLeader(pr.shardID, target)
		return false
	}

	data := protoc.MustMarshal(c.req)
	size := len(data)
	metric.ObserveProposalBytes(int64(size))

	if size > int(pr.store.cfg.Raft.MaxEntryBytes) {
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

func (pr *replica) proposeConfChange(c batch) bool {
	if pr.rn.PendingConfIndex() > pr.appliedIndex {
		logger.Errorf("shard-%d there is a pending conf change, try later",
			pr.shardID)
		c.respOtherError(errors.New("there is a pending conf change, try later"))
		return false
	}

	data := protoc.MustMarshal(c.req)
	admin := c.req.AdminRequest
	err := pr.proposeConfChangeInternal(c, admin, data)
	if err != nil {
		logger.Errorf("shard-%d proposal conf change failed with %+v",
			pr.shardID,
			err)
		return false
	}

	return true
}

func (pr *replica) proposeConfChangeInternal(c batch, admin *rpc.AdminRequest, data []byte) error {
	cc := pr.toConfChangeI(admin, data)
	var changes []rpc.ConfigChangeRequest
	if admin.ConfigChangeV2 != nil {
		changes = admin.ConfigChangeV2.Changes
	} else {
		changes = append(changes, *admin.ConfigChange)
	}

	err := pr.checkConfChange(changes, cc)
	if err != nil {
		return err
	}

	logger.Infof("shard-%d propose conf change peer %+v",
		pr.shardID,
		changes)

	propose_index := pr.nextProposalIndex()
	err = pr.rn.ProposeConfChange(cc)
	if err != nil {
		return err
	}
	if propose_index == pr.nextProposalIndex() {
		// The message is dropped silently, this usually due to leader absence
		// or transferring leader. Both cases can be considered as NotLeader error.
		target, _ := pr.store.getPeer(pr.getLeaderPeerID())
		c.respNotLeader(pr.shardID, target)
		return errNotLeader
	}

	pr.metrics.propose.confChange++
	return nil
}

func (pr *replica) toConfChangeI(admin *rpc.AdminRequest, data []byte) raftpb.ConfChangeI {
	if admin.ConfigChange != nil {
		return &raftpb.ConfChange{
			Type:    raftpb.ConfChangeType(admin.ConfigChange.ChangeType),
			NodeID:  admin.ConfigChange.Peer.ID,
			Context: data,
		}
	} else {
		cc := &raftpb.ConfChangeV2{}
		for _, ch := range admin.ConfigChangeV2.Changes {
			cc.Changes = append(cc.Changes, raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeType(ch.ChangeType),
				NodeID: ch.Peer.ID,
			})
		}

		if len(cc.Changes) <= 1 {
			cc.Transition = raftpb.ConfChangeTransitionAuto
		} else {
			cc.Transition = raftpb.ConfChangeTransitionJointExplicit
		}
		cc.Context = data
		return cc
	}
}

func (pr *replica) proposeTransferLeader(c batch) bool {
	req := c.req.AdminRequest.TransferLeader

	// has pending conf, skip
	if pr.rn.PendingConfIndex() > pr.appliedIndex {
		logger.Infof("shard %d transfer leader ignored by pending conf, req=<%+v>",
			pr.shardID,
			req)
		return false
	}

	if pr.isTransferLeaderAllowed(req.Peer) {
		pr.doTransferLeader(req.Peer)
	} else {
		logger.Infof("shard %d transfer leader ignored directly, req=<%+v>",
			pr.shardID,
			req)
	}

	// transfer leader command doesn't need to replicate log and apply, so we
	// return immediately. Note that this command may fail, we can view it just as an advice
	c.resp(newAdminResponseBatch(rpc.AdminCmdType_TransferLeader, &rpc.TransferLeaderResponse{}))
	return false
}

func (pr *replica) doTransferLeader(peer Peer) {
	logger.Infof("shard %d transfer leader to peer %d",
		pr.shardID,
		peer.ID)

	// Broadcast heartbeat to make sure followers commit the entries immediately.
	// It's only necessary to ping the target peer, but ping all for simplicity.
	pr.rn.Ping()

	pr.rn.TransferLeader(peer.ID)
	pr.metrics.propose.transferLeader++
}

func (pr *replica) isTransferLeaderAllowed(newLeaderPeer Peer) bool {
	status := pr.rn.Status()
	if _, ok := status.Progress[newLeaderPeer.ID]; !ok {
		return false
	}

	for _, p := range status.Progress {
		if p.State == tracker.StateSnapshot {
			return false
		}
	}

	lastIndex, _ := pr.lr.LastIndex()
	return lastIndex <= status.Progress[newLeaderPeer.ID].Match+pr.store.cfg.Raft.RaftLog.MaxAllowTransferLag
}

func (pr *replica) checkProposal(c batch) bool {
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

	pe := pr.store.validateShard(c.req)
	if pe != nil {
		c.resp(errorPbResp(pe, c.req.Header.ID))
		return false
	}

	return true
}

// TODO: set higher election priority of voter/incoming voter than demoting voter
/// Validate the `ConfChange` requests and check whether it's safe to
/// propose these conf change requests.
/// It's safe iff at least the quorum of the Raft group is still healthy
/// right after all conf change is applied.
/// If 'allow_remove_leader' is false then the peer to be removed should
/// not be the leader.
func (pr *replica) checkConfChange(changes []rpc.ConfigChangeRequest, cci raftpb.ConfChangeI) error {
	cc := cci.AsV2()
	afterProgress, err := pr.checkJointState(cc)
	if err != nil {
		return err
	}
	currentProgress := pr.rn.NewChanger().Tracker

	kind := getConfChangeKind(len(cc.Changes))
	// Leaving joint state, skip check
	if kind == leaveJointKind {
		return nil
	}

	// Check whether this request is valid
	check_dup := make(map[uint64]struct{})
	only_learner_change := true
	currentVoter := currentProgress.Config.Voters.IDs()
	for _, cp := range changes {
		if cp.ChangeType == metapb.ChangePeerType_RemoveNode &&
			cp.Peer.Role == metapb.PeerRole_Voter &&
			kind != simpleKind {
			return fmt.Errorf("invalid conf change request %+v, can not remove voter directly", cp)
		}

		if !(cp.ChangeType == metapb.ChangePeerType_RemoveNode ||
			(cp.ChangeType == metapb.ChangePeerType_AddNode && cp.Peer.Role == metapb.PeerRole_Voter) ||
			(cp.ChangeType == metapb.ChangePeerType_AddLearnerNode && cp.Peer.Role == metapb.PeerRole_Learner)) {
			return fmt.Errorf("invalid conf change request %+v", cp)
		}

		if _, ok := check_dup[cp.Peer.ID]; ok {
			return fmt.Errorf("invalid conf change request %+v, , have multiple commands for the same peer %+v",
				cp,
				cp.Peer)
		}
		check_dup[cp.Peer.ID] = struct{}{}

		if cp.Peer.ID == pr.peer.ID &&
			(cp.ChangeType == metapb.ChangePeerType_RemoveNode ||
				(kind == simpleKind && cp.ChangeType == metapb.ChangePeerType_AddLearnerNode)) &&
			!pr.store.cfg.Replication.AllowRemoveLeader {
			return fmt.Errorf("ignore remove leader or demote leader")
		}

		if _, ok := currentVoter[cp.Peer.ID]; ok || cp.ChangeType == metapb.ChangePeerType_AddNode {
			only_learner_change = false
		}
	}

	// Multiple changes that only effect learner will not product `IncommingVoter` or `DemotingVoter`
	// after apply, but raftstore layer and PD rely on these roles to detect joint state
	if kind != simpleKind && only_learner_change {
		return fmt.Errorf("invalid conf change request, multiple changes that only effect learner")
	}

	// It's always safe if there is only one node in the cluster.
	if currentProgress.IsSingleton() {
		return nil
	}

	// Waking it up to replicate logs to candidate.
	return fmt.Errorf("unsafe to perform conf change %+v, before %+v, after %+v",
		changes,
		currentProgress,
		afterProgress)
}

func (pr *replica) checkJointState(cci raftpb.ConfChangeI) (*tracker.ProgressTracker, error) {
	changer := pr.rn.NewChanger()
	var cfg tracker.Config
	var changes *tracker.Changes
	var err error
	cc := cci.AsV2()
	if cc.LeaveJoint() {
		cfg, _, changes, err = changer.LeaveJoint()
	} else if autoLeave, _ := cc.EnterJoint(); autoLeave {
		cfg, _, changes, err = changer.EnterJoint(autoLeave, cc.Changes...)
	} else {
		cfg, _, changes, err = changer.Simple(cc.Changes...)
	}

	if err != nil {
		return nil, err
	}

	trk := &changer.Tracker
	trk.ApplyConf(cfg, changes, pr.rn.LastIndex())
	return trk, nil
}

func (pr *replica) getHandlePolicy(req *rpc.RequestBatch) (requestPolicy, error) {
	if req.AdminRequest != nil {
		switch req.AdminRequest.CmdType {
		case rpc.AdminCmdType_ConfigChange:
			return proposeChange, nil
		case rpc.AdminCmdType_ConfigChangeV2:
			return proposeChange, nil
		case rpc.AdminCmdType_TransferLeader:
			return proposeTransferLeader, nil
		default:
			return proposeNormal, nil
		}
	}

	var isRead, isWrite bool
	for _, r := range req.Requests {
		isRead = r.Type == rpc.CmdType_Read
		isWrite = r.Type == rpc.CmdType_Write
	}

	if isRead && isWrite {
		return proposeNormal, errors.New("read and write can't be mixed in one batch")
	}

	if isWrite {
		return proposeNormal, nil
	}

	return readIndex, nil
}
