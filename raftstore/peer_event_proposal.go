package raftstore

import (
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/metric"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/raft/tracker"
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

func (pr *peerReplica) proposeConfChange(c cmd) bool {
	if pr.rn.PendingConfIndex() > pr.ps.getAppliedIndex() {
		logger.Errorf("shard-%d there is a pending conf change, try later",
			pr.shardID)
		c.respOtherError(errors.New("there is a pending conf change, try later"))
		return false
	}

	if pr.ps.appliedIndexTerm != pr.rn.BasicStatus().Term {
		logger.Errorf("shard-%d has not applied to current term, applied_term %d, current_term %d",
			pr.shardID,
			pr.ps.appliedIndexTerm,
			pr.rn.BasicStatus().Term)
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

func (pr *peerReplica) proposeConfChangeInternal(c cmd, admin *raftcmdpb.AdminRequest, data []byte) error {
	cc := pr.toConfChangeI(admin, data)
	var changes []raftcmdpb.ChangePeerRequest
	if admin.ChangePeerV2 != nil {
		changes = admin.ChangePeerV2.Changes
	} else {
		changes = append(changes, *admin.ChangePeer)
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

func (pr *peerReplica) toConfChangeI(admin *raftcmdpb.AdminRequest, data []byte) raftpb.ConfChangeI {
	if admin.ChangePeer != nil {
		return &raftpb.ConfChange{
			Type:    raftpb.ConfChangeType(admin.ChangePeer.ChangeType),
			NodeID:  admin.ChangePeer.Peer.ID,
			Context: data,
		}
	} else {
		cc := &raftpb.ConfChangeV2{}
		for _, ch := range admin.ChangePeerV2.Changes {
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

func (pr *peerReplica) proposeTransferLeader(c cmd) bool {
	req := c.req.AdminRequest.TransferLeader

	// has pending conf, skip
	if pr.rn.PendingConfIndex() > pr.ps.getAppliedIndex() {
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
	c.resp(newAdminRaftCMDResponse(raftcmdpb.AdminCmdType_TransferLeader, &raftcmdpb.TransferLeaderResponse{}))
	return false
}

func (pr *peerReplica) doTransferLeader(peer metapb.Peer) {
	logger.Infof("shard %d transfer leader to peer %d",
		pr.shardID,
		peer.ID)

	// Broadcast heartbeat to make sure followers commit the entries immediately.
	// It's only necessary to ping the target peer, but ping all for simplicity.
	pr.rn.Ping()

	pr.rn.TransferLeader(peer.ID)
	pr.metrics.propose.transferLeader++
}

func (pr *peerReplica) isTransferLeaderAllowed(newLeaderPeer metapb.Peer) bool {
	status := pr.rn.Status()
	if _, ok := status.Progress[newLeaderPeer.ID]; !ok {
		return false
	}

	for _, p := range status.Progress {
		if p.State == tracker.StateSnapshot {
			return false
		}
	}

	lastIndex, _ := pr.ps.LastIndex()
	return lastIndex <= status.Progress[newLeaderPeer.ID].Match+pr.store.cfg.Raft.RaftLog.MaxAllowTransferLag
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

// TODO: set higher election priority of voter/incoming voter than demoting voter
/// Validate the `ConfChange` requests and check whether it's safe to
/// propose these conf change requests.
/// It's safe iff at least the quorum of the Raft group is still healthy
/// right after all conf change is applied.
/// If 'allow_remove_leader' is false then the peer to be removed should
/// not be the leader.
func (pr *peerReplica) checkConfChange(changes []raftcmdpb.ChangePeerRequest, cci raftpb.ConfChangeI) error {
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

	promoted_commit_index := currentProgress.Committed()
	// It's always safe if there is only one node in the cluster.
	if currentProgress.IsSingleton() || promoted_commit_index >= pr.ps.getTruncatedIndex() {
		return nil
	}

	// Waking it up to replicate logs to candidate.
	return fmt.Errorf("unsafe to perform conf change %+v, before %+v, after %+v, truncated index %+v, promoted commit index %+v",
		changes,
		currentProgress,
		afterProgress,
		pr.ps.getTruncatedIndex(),
		promoted_commit_index)
}

func (pr *peerReplica) checkJointState(cci raftpb.ConfChangeI) (*tracker.ProgressTracker, error) {
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

func (pr *peerReplica) getHandlePolicy(req *raftcmdpb.RaftCMDRequest) (requestPolicy, error) {
	if req.AdminRequest != nil {
		switch req.AdminRequest.CmdType {
		case raftcmdpb.AdminCmdType_ChangePeer:
			return proposeChange, nil
		case raftcmdpb.AdminCmdType_ChangePeerV2:
			return proposeChange, nil
		case raftcmdpb.AdminCmdType_TransferLeader:
			return proposeTransferLeader, nil
		default:
			return proposeNormal, nil
		}
	}

	var isRead, isWrite bool
	for _, r := range req.Requests {
		isRead = r.Type == raftcmdpb.CMDType_Read
		isWrite = r.Type == raftcmdpb.CMDType_Write
	}

	if isRead && isWrite {
		return proposeNormal, errors.New("read and write can't be mixed in one batch")
	}

	if isWrite {
		return proposeNormal, nil
	}

	if pr.store.cfg.Customize.CustomCanReadLocalFunc != nil &&
		pr.store.cfg.Customize.CustomCanReadLocalFunc(pr.ps.shard) {
		return readLocal, nil
	}

	return readIndex, nil
}
