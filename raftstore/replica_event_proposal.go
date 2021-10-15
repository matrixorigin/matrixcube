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
	"errors"
	"fmt"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/raft/v3/tracker"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type confChangeKind int

var (
	simpleKind     = confChangeKind(0)
	enterJointKind = confChangeKind(1)
	leaveJointKind = confChangeKind(2)
)

func getConfigChangeKind(changeNum int) confChangeKind {
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
			if ce := pr.logger.Check(zapcore.DebugLevel, "push to proposal batch"); ce != nil {
				ce.Write(log.HexField("id", req.req.ID))
			}
			pr.incomingProposals.push(shard.Group, req)
		}
	}

	for {
		if c, ok := pr.incomingProposals.pop(); ok {
			pr.propose(c)
		} else {
			break
		}
	}

	size := pr.requests.Len()
	metric.SetRaftRequestQueueMetric(size)
	if size > 0 {
		pr.notifyWorker()
	}
}

func (pr *replica) propose(c batch) {
	if !pr.checkProposal(c) {
		return
	}

	// after propose, raft need to send message to peers
	defer pr.notifyWorker()

	// Note:
	// The peer that is being checked is a leader. It might step down to be a follower later. It
	// doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
	// command log entry can't be committed.
	isConfChange := false
	policy, err := pr.getHandlePolicy(c.requestBatch)
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

	if err := pr.doPropose(c, isConfChange); err != nil {
		c.respOtherError(err)
	}
}

func (pr *replica) doPropose(c batch, isConfChange bool) error {
	if isConfChange {
		changeC := pr.pendingProposals.getConfigChange()
		if !changeC.requestBatch.Header.IsEmpty() {
			changeC.notifyStaleCmd()
		}
		pr.pendingProposals.setConfigChange(c)
	} else {
		pr.pendingProposals.append(c)
	}

	return nil
}

func (pr *replica) execReadIndex(c batch) {
	if c.tp != read {
		panic("not a read index request")
	}
	if !pr.isLeader() {
		target, _ := pr.store.getReplicaRecord(pr.getLeaderPeerID())
		c.respNotLeader(pr.shardID, target)
		return
	}

	lastPendingReadCount := pr.pendingReadCount()
	lastReadyReadCount := pr.readyReadCount()

	pr.rn.ReadIndex(protoc.MustMarshal(&c.requestBatch))

	pendingReadCount := pr.pendingReadCount()
	readyReadCount := pr.readyReadCount()

	if pendingReadCount == lastPendingReadCount &&
		readyReadCount == lastReadyReadCount {
		target, _ := pr.store.getReplicaRecord(pr.getLeaderPeerID())
		c.respNotLeader(pr.shardID, target)
		return
	}
	pr.metrics.propose.readIndex++
}

func (pr *replica) proposeNormal(c batch) bool {
	if !pr.isLeader() {
		target, _ := pr.store.getReplicaRecord(pr.getLeaderPeerID())
		c.respNotLeader(pr.shardID, target)
		return false
	}

	data := protoc.MustMarshal(&c.requestBatch)
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
		target, _ := pr.store.getReplicaRecord(pr.getLeaderPeerID())
		c.respNotLeader(pr.shardID, target)
		return false
	}

	pr.metrics.propose.normal++
	return true
}

func (pr *replica) proposeConfChange(c batch) bool {
	if pr.rn.PendingConfIndex() > pr.appliedIndex {
		pr.logger.Error("there is a pending conf change, try later")
		c.respOtherError(errors.New("there is a pending conf change, try later"))
		return false
	}

	data := protoc.MustMarshal(&c.requestBatch)
	admin := c.requestBatch.AdminRequest
	err := pr.proposeConfChangeInternal(c, admin, data)
	if err != nil {
		pr.logger.Error("fail to proposal conf change",
			zap.Error(err))
		return false
	}
	return true
}

func (pr *replica) proposeConfChangeInternal(c batch, admin rpc.AdminRequest, data []byte) error {
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

	pr.logger.Info("propose conf change",
		log.ConfigChangesField("changes", changes))

	propose_index := pr.nextProposalIndex()
	err = pr.rn.ProposeConfChange(cc)
	if err != nil {
		return err
	}
	if propose_index == pr.nextProposalIndex() {
		// The message is dropped silently, this usually due to leader absence
		// or transferring leader. Both cases can be considered as NotLeader error.
		target, _ := pr.store.getReplicaRecord(pr.getLeaderPeerID())
		c.respNotLeader(pr.shardID, target)
		return errNotLeader
	}

	pr.metrics.propose.confChange++
	return nil
}

func (pr *replica) toConfChangeI(admin rpc.AdminRequest, data []byte) raftpb.ConfChangeI {
	if admin.ConfigChange != nil {
		return &raftpb.ConfChange{
			Type:    raftpb.ConfChangeType(admin.ConfigChange.ChangeType),
			NodeID:  admin.ConfigChange.Replica.ID,
			Context: data,
		}
	} else {
		cc := &raftpb.ConfChangeV2{}
		for _, ch := range admin.ConfigChangeV2.Changes {
			cc.Changes = append(cc.Changes, raftpb.ConfChangeSingle{
				Type:   raftpb.ConfChangeType(ch.ChangeType),
				NodeID: ch.Replica.ID,
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
	req := c.requestBatch.AdminRequest.TransferLeader

	// has pending conf, skip
	if pr.rn.PendingConfIndex() > pr.appliedIndex {
		pr.logger.Info("transfer leader ignored by pending conf")
		return false
	}

	if pr.isTransferLeaderAllowed(req.Replica) {
		pr.doTransferLeader(req.Replica)
	} else {
		pr.logger.Info("transfer leader ignored directly")
	}

	// transfer leader command doesn't need to replicate log and apply, so we
	// return immediately. Note that this command may fail, we can view it just as an advice
	c.resp(newAdminResponseBatch(rpc.AdminCmdType_TransferLeader, &rpc.TransferLeaderResponse{}))
	return false
}

func (pr *replica) doTransferLeader(peer Replica) {
	pr.logger.Info("do transfer leader",
		log.ReplicaField("to", peer))

	// Broadcast heartbeat to make sure followers commit the entries immediately.
	// It's only necessary to ping the target peer, but ping all for simplicity.
	pr.rn.Ping()

	pr.rn.TransferLeader(peer.ID)
	pr.metrics.propose.transferLeader++
}

func (pr *replica) isTransferLeaderAllowed(newLeaderPeer Replica) bool {
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
	if len(c.requestBatch.Header.ID) == 0 {
		c.resp(errorOtherCMDResp(errMissingUUIDCMD))
		return false
	}

	err := pr.store.validateStoreID(c.requestBatch)
	if err != nil {
		c.respOtherError(err)
		return false
	}

	if pe, ok := pr.store.validateShard(c.requestBatch); ok {
		c.resp(errorPbResp(c.getRequestID(), pe))
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
	if _, err := pr.checkJointState(cc); err != nil {
		return err
	}
	currentProgress := pr.rn.NewChanger().Tracker

	kind := getConfigChangeKind(len(cc.Changes))
	// Leaving joint state, skip check
	if kind == leaveJointKind {
		return nil
	}

	// Check whether this request is valid
	check_dup := make(map[uint64]struct{})
	only_learner_change := true
	currentVoter := currentProgress.Config.Voters.IDs()
	for _, cp := range changes {
		if cp.ChangeType == metapb.ConfigChangeType_RemoveNode &&
			cp.Replica.Role == metapb.ReplicaRole_Voter &&
			kind != simpleKind {
			return fmt.Errorf("invalid conf change request %+v, can not remove voter directly", cp)
		}

		if !(cp.ChangeType == metapb.ConfigChangeType_RemoveNode ||
			(cp.ChangeType == metapb.ConfigChangeType_AddNode && cp.Replica.Role == metapb.ReplicaRole_Voter) ||
			(cp.ChangeType == metapb.ConfigChangeType_AddLearnerNode && cp.Replica.Role == metapb.ReplicaRole_Learner)) {
			return fmt.Errorf("invalid conf change request %+v", cp)
		}

		if _, ok := check_dup[cp.Replica.ID]; ok {
			return fmt.Errorf("invalid conf change request %+v, , have multiple commands for the same peer %+v",
				cp,
				cp.Replica)
		}
		check_dup[cp.Replica.ID] = struct{}{}

		if cp.Replica.ID == pr.replica.ID &&
			(cp.ChangeType == metapb.ConfigChangeType_RemoveNode ||
				(kind == simpleKind && cp.ChangeType == metapb.ConfigChangeType_AddLearnerNode)) &&
			!pr.store.cfg.Replication.AllowRemoveLeader {
			return fmt.Errorf("ignore remove leader or demote leader")
		}

		if _, ok := currentVoter[cp.Replica.ID]; ok || cp.ChangeType == metapb.ConfigChangeType_AddNode {
			only_learner_change = false
		}
	}

	// Multiple changes that only effect learner will not product `IncommingVoter` or `DemotingVoter`
	// after apply, but raftstore layer and PD rely on these roles to detect joint state
	if kind != simpleKind && only_learner_change {
		return fmt.Errorf("invalid conf change request, multiple changes that only effect learner")
	}

	return nil
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

// TODO: what is a policy?
func (pr *replica) getHandlePolicy(req rpc.RequestBatch) (requestPolicy, error) {
	if req.IsAdmin() {
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
