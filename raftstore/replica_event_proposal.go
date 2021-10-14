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
	"github.com/cockroachdb/errors"
	"github.com/fagongzi/util/protoc"
	"go.etcd.io/etcd/raft/v3/raftpb"
	trackerPkg "go.etcd.io/etcd/raft/v3/tracker"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/rpc"
)

var (
	ErrInvalidConfigChangeRequest = errors.New("invalid config change request")
	ErrRemoveVoter                = errors.New("removing voter")
	ErrRemoveLeader               = errors.New("removing leader")
	ErrPendingConfigChange        = errors.New("pending config change")
	ErrDuplicatedRequest          = errors.New("duplicated config change request")
	ErrLearnerOnlyChange          = errors.New("learner only change")
)

type tracker = trackerPkg.ProgressTracker

type confChangeKind int

const (
	simpleKind confChangeKind = iota
	enterJointKind
	leaveJointKind
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

type requestType int

const (
	readIndex requestType = iota
	proposalNormal
	proposalConfigChange
	requestTransferLeader
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
			// FIXME: still using the current epoch here. should use epoch value
			// returned when routing the request.
			pr.incomingProposals.push(shard.Group, shard.Epoch, req)
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
	defer pr.notifyWorker()

	isConfChange := false
	madeProposal := false
	switch pr.getRequestType(c.requestBatch) {
	case readIndex:
		pr.execReadIndex(c)
	case proposalNormal:
		madeProposal = pr.proposeNormal(c)
	case requestTransferLeader:
		madeProposal = pr.requestTransferLeader(c)
	case proposalConfigChange:
		isConfChange = true
		pr.metrics.admin.confChange++
		madeProposal = pr.proposeConfChange(c)
	}

	if madeProposal {
		pr.updatePendingProposal(c, isConfChange)
	}
}

func (pr *replica) updatePendingProposal(c batch, isConfChange bool) error {
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

func (pr *replica) respNotLeader(c batch) {
	target, _ := pr.store.getReplicaRecord(pr.getLeaderReplicaID())
	c.respNotLeader(pr.shardID, target)
}

func (pr *replica) execReadIndex(c batch) {
	if c.tp != read {
		panic("not a read index request")
	}
	if !pr.isLeader() {
		pr.respNotLeader(c)
		return
	}

	prevPendingReadCount := pr.pendingReadCount()
	prevReadyReadCount := pr.readyReadCount()

	pr.rn.ReadIndex(protoc.MustMarshal(&c.requestBatch))

	pendingReadCount := pr.pendingReadCount()
	readyReadCount := pr.readyReadCount()

	if pendingReadCount == prevPendingReadCount &&
		readyReadCount == prevReadyReadCount {
		pr.respNotLeader(c)
		return
	}
	pr.metrics.propose.readIndex++
}

func (pr *replica) proposeNormal(c batch) bool {
	if !pr.isLeader() {
		pr.respNotLeader(c)
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
	if err := pr.rn.Propose(data); err != nil {
		c.resp(errorOtherCMDResp(err))
		return false
	}
	if idx == pr.nextProposalIndex() {
		pr.respNotLeader(c)
		return false
	}

	pr.metrics.propose.normal++
	return true
}

func (pr *replica) proposeConfChange(c batch) bool {
	if pr.rn.PendingConfIndex() > pr.appliedIndex {
		pr.logger.Error(ErrPendingConfigChange.Error())
		c.respOtherError(ErrPendingConfigChange)
		return false
	}

	data := protoc.MustMarshal(&c.requestBatch)
	admin := c.requestBatch.AdminRequest
	if err := pr.proposeConfChangeInternal(c, admin, data); err != nil {
		pr.logger.Error("fail to proposal conf change",
			zap.Error(err))
		return false
	}
	return true
}

func (pr *replica) proposeConfChangeInternal(c batch,
	admin rpc.AdminRequest, data []byte) error {
	cc := pr.toConfChangeI(admin, data)
	var changes []rpc.ConfigChangeRequest
	if admin.ConfigChangeV2 != nil {
		changes = admin.ConfigChangeV2.Changes
	} else {
		changes = append(changes, *admin.ConfigChange)
	}

	if err := pr.checkConfChange(changes, cc); err != nil {
		return err
	}

	pr.logger.Info("propose conf change",
		log.ConfigChangesField("changes", changes))

	propose_index := pr.nextProposalIndex()
	if err := pr.rn.ProposeConfChange(cc); err != nil {
		return err
	}
	if propose_index == pr.nextProposalIndex() {
		// The message is dropped silently, this usually due to leader absence
		// or transferring leader. Both cases can be considered as NotLeader error.
		target, _ := pr.store.getReplicaRecord(pr.getLeaderReplicaID())
		c.respNotLeader(pr.shardID, target)
		return errNotLeader
	}

	pr.metrics.propose.confChange++
	return nil
}

func (pr *replica) toConfChangeI(admin rpc.AdminRequest,
	data []byte) raftpb.ConfChangeI {
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

func (pr *replica) requestTransferLeader(c batch) bool {
	req := c.requestBatch.AdminRequest.TransferLeader
	// has pending confChange, skip
	if pr.rn.PendingConfIndex() > pr.appliedIndex {
		pr.logger.Info("transfer leader ignored due to pending confChange")
		return false
	}

	if pr.isTransferLeaderAllowed(req.Replica) {
		pr.doTransferLeader(req.Replica)
	} else {
		pr.logger.Info("transfer leader not allowed")
	}
	// we submitted the request to start the leadership transfer, but there is no
	// guarantee that it will successfully complete.
	c.resp(newAdminResponseBatch(rpc.AdminCmdType_TransferLeader,
		&rpc.TransferLeaderResponse{}))
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

func (pr *replica) isTransferLeaderAllowed(newLeader Replica) bool {
	status := pr.rn.Status()
	if _, ok := status.Progress[newLeader.ID]; !ok {
		return false
	}
	for _, p := range status.Progress {
		if p.State == trackerPkg.StateSnapshot {
			return false
		}
	}

	lastIndex, _ := pr.lr.LastIndex()
	maxTransferLag := pr.store.cfg.Raft.RaftLog.MaxAllowTransferLag
	return lastIndex <= status.Progress[newLeader.ID].Match+maxTransferLag
}

func (pr *replica) checkProposal(c batch) bool {
	// we handle all read, write and admin cmd here
	if len(c.requestBatch.Header.ID) == 0 {
		c.resp(errorOtherCMDResp(errMissingUUIDCMD))
		return false
	}
	if err := pr.store.validateStoreID(c.requestBatch); err != nil {
		c.respOtherError(err)
		return false
	}
	if pe, ok := pr.store.validateShard(c.requestBatch); ok {
		c.resp(errorPbResp(c.getRequestID(), pe))
		return false
	}

	return true
}

func isValidConfigChangeRequest(ccr rpc.ConfigChangeRequest) bool {
	// remove voter or learner
	if ccr.ChangeType == metapb.ConfigChangeType_RemoveNode {
		return true
	}
	// add voter or promote learner
	if ccr.ChangeType == metapb.ConfigChangeType_AddNode &&
		ccr.Replica.Role == metapb.ReplicaRole_Voter {
		return true
	}
	// add learner
	if ccr.ChangeType == metapb.ConfigChangeType_AddLearnerNode &&
		ccr.Replica.Role == metapb.ReplicaRole_Learner {
		return true
	}
	return false
}

func isRemovingOrDemotingLeader(kind confChangeKind,
	ccr rpc.ConfigChangeRequest, leaderReplicaID uint64) bool {
	// targetting the leader
	if ccr.Replica.ID != leaderReplicaID {
		return false
	}
	// removing
	if ccr.ChangeType == metapb.ConfigChangeType_RemoveNode {
		return true
	}
	// demoting
	if kind == simpleKind &&
		ccr.ChangeType == metapb.ConfigChangeType_AddLearnerNode {
		return true
	}
	return false
}

func removingVoterDirectlyInJointConsensusCC(kind confChangeKind,
	ccr rpc.ConfigChangeRequest) bool {
	if kind != simpleKind {
		if ccr.ChangeType == metapb.ConfigChangeType_RemoveNode &&
			ccr.Replica.Role == metapb.ReplicaRole_Voter {
			return true
		}
	}
	return false
}

func (pr *replica) checkConfChange(changes []rpc.ConfigChangeRequest,
	cci raftpb.ConfChangeI) error {
	cc := cci.AsV2()
	if _, err := pr.checkJointState(cc); err != nil {
		return err
	}
	kind := getConfigChangeKind(len(cc.Changes))
	if kind == leaveJointKind {
		return nil
	}

	dup := make(map[uint64]struct{})
	learnerOnly := true
	voters := pr.rn.NewChanger().Tracker.Config.Voters.IDs()
	for _, cp := range changes {
		if removingVoterDirectlyInJointConsensusCC(kind, cp) {
			// TODO: error log the cp value here
			return ErrRemoveVoter
		}
		if !isValidConfigChangeRequest(cp) {
			return ErrInvalidConfigChangeRequest
		}
		if _, ok := dup[cp.Replica.ID]; ok {
			return ErrDuplicatedRequest
		}
		dup[cp.Replica.ID] = struct{}{}

		if !pr.store.cfg.Replication.AllowRemoveLeader {
			if isRemovingOrDemotingLeader(kind, cp, pr.replica.ID) {
				return ErrRemoveLeader
			}
		}

		if cp.ChangeType == metapb.ConfigChangeType_AddNode {
			learnerOnly = false
		}
		if _, ok := voters[cp.Replica.ID]; ok {
			learnerOnly = false
		}
	}
	// such config change request will confuse raftstore
	if kind != simpleKind && learnerOnly {
		return ErrLearnerOnlyChange
	}

	return nil
}

func (pr *replica) checkJointState(cci raftpb.ConfChangeI) (*tracker, error) {
	changer := pr.rn.NewChanger()
	var cfg trackerPkg.Config
	var changes *trackerPkg.Changes
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

func (pr *replica) getRequestType(req rpc.RequestBatch) requestType {
	if req.IsAdmin() {
		switch req.AdminRequest.CmdType {
		case rpc.AdminCmdType_ConfigChange:
			return proposalConfigChange
		case rpc.AdminCmdType_ConfigChangeV2:
			return proposalConfigChange
		case rpc.AdminCmdType_TransferLeader:
			return requestTransferLeader
		default:
			return proposalNormal
		}
	}
	var hasRead, hasWrite bool
	for _, r := range req.Requests {
		if !hasRead {
			hasRead = r.Type == rpc.CmdType_Read
		}
		if !hasWrite {
			hasWrite = r.Type == rpc.CmdType_Write
		}
	}
	if hasRead && hasWrite {
		panic("read and write can't be mixed in one batch")
	}
	if hasWrite {
		return proposalNormal
	}
	return readIndex
}
