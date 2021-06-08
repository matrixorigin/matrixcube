package raftstore

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/fagongzi/util/collection/deque"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/bhraftpb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
)

func (d *applyDelegate) execAdminRequest(ctx *applyContext) (*raftcmdpb.RaftCMDResponse, *execResult, error) {
	cmdType := ctx.req.AdminRequest.CmdType
	switch cmdType {
	case raftcmdpb.AdminCmdType_ChangePeer:
		return d.doExecChangePeer(ctx)
	case raftcmdpb.AdminCmdType_ChangePeerV2:
		return d.doExecChangePeerV2(ctx)
	case raftcmdpb.AdminCmdType_BatchSplit:
		return d.doExecSplit(ctx)
	case raftcmdpb.AdminCmdType_CompactLog:
		return d.doExecCompactRaftLog(ctx)
	}

	return nil, nil, nil
}

func (d *applyDelegate) doExecChangePeer(ctx *applyContext) (*raftcmdpb.RaftCMDResponse, *execResult, error) {
	req := ctx.req.AdminRequest.ChangePeer
	peer := req.Peer
	logger.Infof("shard %d do apply change peer %+v at epoch %+v, peers %+v",
		d.shard.ID,
		req,
		d.shard.Epoch,
		d.shard.Peers)

	res := bhmetapb.Shard{}
	protoc.MustUnmarshal(&res, protoc.MustMarshal(&d.shard))
	res.Epoch.ConfVer++

	p := findPeer(&res, req.Peer.ContainerID)
	switch req.ChangeType {
	case metapb.ChangePeerType_AddNode:
		exists := false
		if p != nil {
			exists = true
			if p.Role != metapb.PeerRole_Learner || p.ID != peer.ID {
				return nil, nil, fmt.Errorf("shard-%d can't add duplicated peer %+v",
					res.ID,
					peer)
			}

			p.Role = metapb.PeerRole_Voter
		}

		if !exists {
			res.Peers = append(res.Peers, peer)
		}

		logger.Infof("shard-%d add peer %+v successfully, peers %+v",
			res.ID,
			peer,
			res.Peers)
	case metapb.ChangePeerType_RemoveNode:
		if p != nil {
			if p.ID != peer.ID || p.ContainerID != peer.ContainerID {
				return nil, nil, fmt.Errorf("shard %+v ignore remove unmatched peer %+v",
					res.ID,
					peer)
			}

			if d.peerID == peer.ID {
				// Remove ourself, we will destroy all shard data later.
				// So we need not to apply following logs.
				d.setPendingRemove()
			}
		} else {
			return nil, nil, fmt.Errorf("shard %+v remove missing peer %+v",
				res.ID,
				peer)
		}

		logger.Infof("shard-%d remove peer %+v successfully, peers %+v",
			res.ID,
			peer,
			res.Peers)
	case metapb.ChangePeerType_AddLearnerNode:
		if p != nil {
			return nil, nil, fmt.Errorf("shard-%d can't add duplicated learner %+v",
				res.ID,
				peer)
		}

		res.Peers = append(res.Peers, peer)
		logger.Infof("shard-%d add learner peer %+v successfully, peers %+v",
			res.ID,
			peer,
			res.Peers)
	}

	state := bhraftpb.PeerState_Normal
	if d.isPendingRemove() {
		state = bhraftpb.PeerState_Tombstone
	}

	d.shard = res
	d.ps.updatePeerState(d.shard, state, ctx.raftWB)

	resp := newAdminRaftCMDResponse(raftcmdpb.AdminCmdType_ChangePeer, &raftcmdpb.ChangePeerResponse{
		Shard: d.shard,
	})
	result := &execResult{
		adminType: raftcmdpb.AdminCmdType_ChangePeer,
		changePeer: &changePeer{
			index:   d.ctx.index,
			changes: []raftcmdpb.ChangePeerRequest{*req},
			shard:   d.shard,
		},
	}

	return resp, result, nil
}

func (d *applyDelegate) doExecChangePeerV2(ctx *applyContext) (*raftcmdpb.RaftCMDResponse, *execResult, error) {
	req := ctx.req.AdminRequest.ChangePeerV2
	changes := req.Changes
	logger.Infof("shard %d do apply change peer v2 %+v at epoch %+v",
		d.shard.ID,
		changes,
		d.shard.Epoch)

	var res bhmetapb.Shard
	var err error
	kind := getConfChangeKind(len(changes))
	if kind == leaveJointKind {
		res, err = d.applyLeaveJoint()
	} else {
		res, err = d.applyConfChangeByKind(kind, changes)
	}

	if err != nil {
		return nil, nil, err
	}

	state := bhraftpb.PeerState_Normal
	if d.isPendingRemove() {
		state = bhraftpb.PeerState_Tombstone
	}

	d.shard = res
	d.ps.updatePeerState(d.shard, state, ctx.raftWB)

	resp := newAdminRaftCMDResponse(raftcmdpb.AdminCmdType_ChangePeer, &raftcmdpb.ChangePeerResponse{
		Shard: d.shard,
	})
	result := &execResult{
		adminType: raftcmdpb.AdminCmdType_ChangePeer,
		changePeer: &changePeer{
			index:   d.ctx.index,
			changes: changes,
			shard:   d.shard,
		},
	}

	return resp, result, nil
}

func (d *applyDelegate) applyConfChangeByKind(kind confChangeKind, changes []raftcmdpb.ChangePeerRequest) (bhmetapb.Shard, error) {
	res := bhmetapb.Shard{}
	protoc.MustUnmarshal(&res, protoc.MustMarshal(&d.shard))

	for _, cp := range changes {
		change_type := cp.ChangeType
		peer := cp.Peer
		store_id := peer.ContainerID

		exist_peer := findPeer(&d.shard, peer.ContainerID)
		if exist_peer != nil {
			r := exist_peer.Role
			if r == metapb.PeerRole_IncomingVoter || r == metapb.PeerRole_DemotingVoter {
				logger.Fatalf("shard-%d can't apply confchange because configuration is still in joint state",
					res.ID)
			}
		}

		if exist_peer == nil && change_type == metapb.ChangePeerType_AddNode {
			if kind == simpleKind {
				peer.Role = metapb.PeerRole_Voter
			} else if kind == enterJointKind {
				peer.Role = metapb.PeerRole_IncomingVoter
			}

			res.Peers = append(res.Peers, peer)
		} else if exist_peer == nil && change_type == metapb.ChangePeerType_AddLearnerNode {
			peer.Role = metapb.PeerRole_Learner
			res.Peers = append(res.Peers, peer)
		} else if exist_peer == nil && change_type == metapb.ChangePeerType_RemoveNode {
			return res, fmt.Errorf("remove missing peer %+v", peer)
		} else if exist_peer != nil &&
			(change_type == metapb.ChangePeerType_AddNode || change_type == metapb.ChangePeerType_AddLearnerNode) {
			// add node
			role := exist_peer.Role
			exist_id := exist_peer.ID
			incoming_id := peer.ID

			// Add peer with different id to the same store
			if exist_id != incoming_id ||
				// The peer is already the requested role
				(role == metapb.PeerRole_Voter && change_type == metapb.ChangePeerType_AddNode) ||
				(role == metapb.PeerRole_Learner && change_type == metapb.ChangePeerType_AddLearnerNode) {
				return res, fmt.Errorf("can't add duplicated peer %+v, duplicated with exist peer %+v",
					peer, exist_peer)
			}

			if role == metapb.PeerRole_Voter && change_type == metapb.ChangePeerType_AddLearnerNode {
				switch kind {
				case simpleKind:
					exist_peer.Role = metapb.PeerRole_Learner
				case enterJointKind:
					exist_peer.Role = metapb.PeerRole_DemotingVoter
				}
			} else if role == metapb.PeerRole_Learner && change_type == metapb.ChangePeerType_AddNode {
				switch kind {
				case simpleKind:
					exist_peer.Role = metapb.PeerRole_Voter
				case enterJointKind:
					exist_peer.Role = metapb.PeerRole_IncomingVoter
				}
			}
		} else if exist_peer != nil && change_type == metapb.ChangePeerType_RemoveNode {
			// Remove node
			if kind == enterJointKind && exist_peer.Role == metapb.PeerRole_Voter {
				return res, fmt.Errorf("can't remove voter peer %+v directly",
					peer)
			}

			p := removePeer(&res, store_id)
			if p != nil {
				if p.ID != peer.ID || p.ContainerID != peer.ContainerID {
					return res, fmt.Errorf("ignore remove unmatched peer %+v", peer)
				}

				if d.peerID == peer.ID {
					// Remove ourself, we will destroy all region data later.
					// So we need not to apply following logs.
					d.setPendingRemove()
				}
			}
		}
	}

	res.Epoch.ConfVer += uint64(len(changes))
	logger.Infof("shard-%d conf change successfully, changes %+v",
		res.ID,
		changes)
	return res, nil
}

func (d *applyDelegate) applyLeaveJoint() (bhmetapb.Shard, error) {
	region := bhmetapb.Shard{}
	protoc.MustUnmarshal(&region, protoc.MustMarshal(&d.shard))

	change_num := uint64(0)
	for idx := range region.Peers {
		if region.Peers[idx].Role == metapb.PeerRole_IncomingVoter {
			region.Peers[idx].Role = metapb.PeerRole_Voter
			continue
		}

		if region.Peers[idx].Role == metapb.PeerRole_DemotingVoter {
			region.Peers[idx].Role = metapb.PeerRole_Learner
			continue
		}

		change_num += 1
	}
	if change_num == 0 {
		logger.Fatalf("shard-%d can't leave a non-joint config %+v",
			d.shard.ID,
			region)
	}
	region.Epoch.ConfVer += change_num
	logger.Infof(
		"shard-%d leave joint state successfully", d.shard.ID)
	return region, nil
}

func (d *applyDelegate) doExecSplit(ctx *applyContext) (*raftcmdpb.RaftCMDResponse, *execResult, error) {
	ctx.metrics.admin.split++
	splitReqs := ctx.req.AdminRequest.Splits

	if len(splitReqs.Requests) == 0 {
		logger.Errorf("shard %d missing splits request",
			d.shard.ID)
		return nil, nil, errors.New("missing splits request")
	}

	newShardsCount := len(splitReqs.Requests)
	derived := bhmetapb.Shard{}
	protoc.MustUnmarshal(&derived, protoc.MustMarshal(&d.shard))
	var shards []bhmetapb.Shard
	keys := deque.New()

	for _, req := range splitReqs.Requests {
		if len(req.SplitKey) == 0 {
			return nil, nil, errors.New("missing split key")
		}

		splitKey := DecodeDataKey(req.SplitKey)
		v := derived.Start
		if e, ok := keys.Back(); ok {
			v = e.Value.([]byte)
		}
		if bytes.Compare(splitKey, v) <= 0 {
			return nil, nil, fmt.Errorf("invalid split key %+v", splitKey)
		}

		if len(req.NewPeerIDs) != len(derived.Peers) {
			return nil, nil, fmt.Errorf("invalid new peer id count, need %d, but got %d",
				len(derived.Peers),
				len(req.NewPeerIDs))
		}

		keys.PushBack(splitKey)
	}

	err := checkKeyInShard(keys.MustBack().Value.([]byte), &d.shard)
	if err != nil {
		logger.Errorf("shard %d split key not in shard, errors:\n %+v",
			d.shard.ID,
			err)
		return nil, nil, nil
	}

	derived.Epoch.Version += uint64(newShardsCount)
	keys.PushBack(derived.End)
	derived.End = keys.MustFront().Value.([]byte)

	for _, req := range splitReqs.Requests {
		newShard := bhmetapb.Shard{}
		newShard.ID = req.NewShardID
		newShard.Group = derived.Group
		newShard.Unique = derived.Unique
		newShard.RuleGroups = derived.RuleGroups
		newShard.DisableSplit = derived.DisableSplit
		newShard.Epoch = derived.Epoch
		newShard.Start = keys.PopFront().Value.([]byte)
		newShard.End = keys.MustFront().Value.([]byte)
		for idx, p := range derived.Peers {
			newShard.Peers = append(newShard.Peers, metapb.Peer{
				ID:          req.NewPeerIDs[idx],
				ContainerID: p.ContainerID,
			})
		}

		shards = append(shards, newShard)
		ctx.metrics.admin.splitSucceed++
	}

	if d.store.cfg.Customize.CustomSplitCompletedFuncFactory != nil {
		if fn := d.store.cfg.Customize.CustomSplitCompletedFuncFactory(derived.Group); fn != nil {
			fn(&derived, shards)
		}
	}

	d.ps.updatePeerState(derived, bhraftpb.PeerState_Normal, ctx.raftWB)
	for _, region := range shards {
		d.ps.updatePeerState(region, bhraftpb.PeerState_Normal, ctx.raftWB)
		d.ps.writeInitialState(region.ID, ctx.raftWB)
	}

	if d.store.cfg.Storage.DataMoveFunc != nil {
		err := d.store.cfg.Storage.DataMoveFunc(derived, shards)
		if err != nil {
			logger.Fatalf("shard %d commit apply splits result, move data failed with %+v",
				d.shard.ID,
				err)
		}
	}

	d.shard = derived
	rsp := newAdminRaftCMDResponse(raftcmdpb.AdminCmdType_BatchSplit, &raftcmdpb.BatchSplitResponse{
		Shards: shards,
	})

	result := &execResult{
		adminType: raftcmdpb.AdminCmdType_BatchSplit,
		splitResult: &splitResult{
			derived: derived,
			shards:  shards,
		},
	}

	return rsp, result, nil
}

func (d *applyDelegate) doExecCompactRaftLog(ctx *applyContext) (*raftcmdpb.RaftCMDResponse, *execResult, error) {
	ctx.metrics.admin.compact++

	req := ctx.req.AdminRequest.CompactLog
	compactIndex := req.CompactIndex
	firstIndex := ctx.applyState.TruncatedState.Index + 1

	if compactIndex <= firstIndex {
		return nil, nil, nil
	}

	compactTerm := req.CompactTerm
	if compactTerm == 0 {
		return nil, nil, errors.New("command format is outdated, please upgrade leader")
	}

	err := compactRaftLog(d.shard.ID, &ctx.applyState, compactIndex, compactTerm)
	if err != nil {
		return nil, nil, err
	}

	rsp := newAdminRaftCMDResponse(raftcmdpb.AdminCmdType_CompactLog, &raftcmdpb.CompactLogRequest{})
	result := &execResult{
		adminType: raftcmdpb.AdminCmdType_CompactLog,
		raftGCResult: &raftGCResult{
			state:      ctx.applyState.TruncatedState,
			firstIndex: firstIndex,
		},
	}

	ctx.metrics.admin.compactSucceed++
	return rsp, result, nil
}

func (d *applyDelegate) execWriteRequest(ctx *applyContext) (uint64, int64, *raftcmdpb.RaftCMDResponse) {
	writeBytes := uint64(0)
	diffBytes := int64(0)
	resp := pb.AcquireRaftCMDResponse()

	ctx.batchSize = len(ctx.req.Requests)
	for idx, req := range ctx.req.Requests {
		if logger.DebugEnabled() {
			logger.Debugf("%s exec", hex.EncodeToString(req.ID))
		}
		ctx.offset = idx
		if h, ok := d.store.writeHandlers[req.CustemType]; ok {
			written, diff, rsp := h(d.shard, req, ctx)
			if rsp.Stale {
				rsp.Error.Message = errStaleCMD.Error()
				rsp.Error.StaleCommand = infoStaleCMD
				rsp.OriginRequest = req
				rsp.OriginRequest.Key = DecodeDataKey(req.Key)
			}

			resp.Responses = append(resp.Responses, rsp)
			writeBytes += written
			diffBytes += diff
		} else {
			logger.Fatalf("%s missing handle func", hex.EncodeToString(req.ID))
		}
		ctx.metrics.writtenKeys++
	}
	return writeBytes, diffBytes, resp
}
