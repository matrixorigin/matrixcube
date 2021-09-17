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
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/fagongzi/util/collection/deque"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/keys"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb"
	"github.com/matrixorigin/matrixcube/pb/bhmetapb"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
	"github.com/matrixorigin/matrixcube/storage"
)

func (d *stateMachine) execAdminRequest(ctx *applyContext) (*raftcmdpb.RaftCMDResponse, error) {
	cmdType := ctx.req.AdminRequest.CmdType
	switch cmdType {
	case raftcmdpb.AdminCmdType_ChangePeer:
		return d.doExecChangePeer(ctx)
	case raftcmdpb.AdminCmdType_ChangePeerV2:
		return d.doExecChangePeerV2(ctx)
	case raftcmdpb.AdminCmdType_BatchSplit:
		return d.doExecSplit(ctx)
	}

	return nil, nil
}

func (d *stateMachine) doExecChangePeer(ctx *applyContext) (*raftcmdpb.RaftCMDResponse, error) {
	req := ctx.req.AdminRequest.ChangePeer
	peer := req.Peer
	current := d.getShard()
	logger.Infof("shard %d do apply change peer %+v at epoch %+v, peers %+v",
		d.shardID,
		req,
		current.Epoch,
		current.Peers)

	res := bhmetapb.Shard{}
	protoc.MustUnmarshal(&res, protoc.MustMarshal(&current))
	res.Epoch.ConfVer++

	p := findPeer(&res, req.Peer.ContainerID)
	switch req.ChangeType {
	case metapb.ChangePeerType_AddNode:
		exists := false
		if p != nil {
			exists = true
			if p.Role != metapb.PeerRole_Learner || p.ID != peer.ID {
				return nil, fmt.Errorf("shard-%d can't add duplicated peer %+v",
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
				return nil, fmt.Errorf("shard %+v ignore remove unmatched peer %+v",
					res.ID,
					peer)
			}

			if d.peerID == peer.ID {
				// Remove ourself, we will destroy all shard data later.
				// So we need not to apply following logs.
				d.setPendingRemove()
			}
		} else {
			return nil, fmt.Errorf("shard %+v remove missing peer %+v",
				res.ID,
				peer)
		}

		logger.Infof("shard-%d remove peer %+v successfully, peers %+v",
			res.ID,
			peer,
			res.Peers)
	case metapb.ChangePeerType_AddLearnerNode:
		if p != nil {
			return nil, fmt.Errorf("shard-%d can't add duplicated learner %+v",
				res.ID,
				peer)
		}

		res.Peers = append(res.Peers, peer)
		logger.Infof("shard-%d add learner peer %+v successfully, peers %+v",
			res.ID,
			peer,
			res.Peers)
	}

	state := bhmetapb.PeerState_Normal
	if d.isPendingRemove() {
		state = bhmetapb.PeerState_Tombstone
	}

	d.updateShard(res)
	err := d.saveShardMetedata(ctx.entry.Index, res, state)
	if err != nil {
		logger.Fatalf("shard %d save metadata at index %d failed with %+v",
			res.ID,
			ctx.entry.Index,
			err)
	}

	resp := newAdminRaftCMDResponse(raftcmdpb.AdminCmdType_ChangePeer, &raftcmdpb.ChangePeerResponse{
		Shard: res,
	})
	ctx.adminResult = &adminExecResult{
		adminType: raftcmdpb.AdminCmdType_ChangePeer,
		changePeerResult: &changePeerResult{
			index:   ctx.entry.Index,
			changes: []raftcmdpb.ChangePeerRequest{*req},
			shard:   res,
		},
	}
	return resp, nil
}

func (d *stateMachine) doExecChangePeerV2(ctx *applyContext) (*raftcmdpb.RaftCMDResponse, error) {
	req := ctx.req.AdminRequest.ChangePeerV2
	changes := req.Changes
	current := d.getShard()
	logger.Infof("shard %d do apply change peer v2 %+v at epoch %+v",
		d.shardID,
		changes,
		current.Epoch)

	var res bhmetapb.Shard
	var err error
	kind := getConfChangeKind(len(changes))
	if kind == leaveJointKind {
		res, err = d.applyLeaveJoint()
	} else {
		res, err = d.applyConfChangeByKind(kind, changes)
	}

	if err != nil {
		return nil, err
	}

	state := bhmetapb.PeerState_Normal
	if d.isPendingRemove() {
		state = bhmetapb.PeerState_Tombstone
	}

	d.updateShard(res)
	err = d.saveShardMetedata(ctx.entry.Index, res, state)
	if err != nil {
		logger.Fatalf("shard %d save metadata at index %d failed with %+v",
			res.ID,
			ctx.entry.Index,
			err)
	}

	resp := newAdminRaftCMDResponse(raftcmdpb.AdminCmdType_ChangePeer, &raftcmdpb.ChangePeerResponse{
		Shard: res,
	})
	ctx.adminResult = &adminExecResult{
		adminType: raftcmdpb.AdminCmdType_ChangePeer,
		changePeerResult: &changePeerResult{
			index:   ctx.entry.Index,
			changes: changes,
			shard:   res,
		},
	}
	return resp, nil
}

func (d *stateMachine) applyConfChangeByKind(kind confChangeKind, changes []raftcmdpb.ChangePeerRequest) (bhmetapb.Shard, error) {
	res := bhmetapb.Shard{}
	current := d.getShard()
	protoc.MustUnmarshal(&res, protoc.MustMarshal(&current))

	for _, cp := range changes {
		change_type := cp.ChangeType
		peer := cp.Peer
		store_id := peer.ContainerID

		exist_peer := findPeer(&current, peer.ContainerID)
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

func (d *stateMachine) applyLeaveJoint() (bhmetapb.Shard, error) {
	region := bhmetapb.Shard{}
	current := d.getShard()
	protoc.MustUnmarshal(&region, protoc.MustMarshal(&current))

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
			d.shardID,
			region)
	}
	region.Epoch.ConfVer += change_num
	logger.Infof("shard-%d leave joint state successfully", d.shardID)
	return region, nil
}

func (d *stateMachine) doExecSplit(ctx *applyContext) (*raftcmdpb.RaftCMDResponse, error) {
	ctx.metrics.admin.split++
	splitReqs := ctx.req.AdminRequest.Splits

	if len(splitReqs.Requests) == 0 {
		logger.Errorf("shard %d missing splits request", d.shardID)
		return nil, errors.New("missing splits request")
	}

	newShardsCount := len(splitReqs.Requests)
	derived := bhmetapb.Shard{}
	current := d.getShard()
	protoc.MustUnmarshal(&derived, protoc.MustMarshal(&current))
	var shards []bhmetapb.Shard
	rangeKeys := deque.New()

	for _, req := range splitReqs.Requests {
		if len(req.SplitKey) == 0 {
			return nil, errors.New("missing split key")
		}

		splitKey := keys.DecodeDataKey(req.SplitKey)
		v := derived.Start
		if e, ok := rangeKeys.Back(); ok {
			v = e.Value.([]byte)
		}
		if bytes.Compare(splitKey, v) <= 0 {
			return nil, fmt.Errorf("invalid split key %+v", splitKey)
		}

		if len(req.NewPeerIDs) != len(derived.Peers) {
			return nil, fmt.Errorf("invalid new peer id count, need %d, but got %d",
				len(derived.Peers),
				len(req.NewPeerIDs))
		}

		rangeKeys.PushBack(splitKey)
	}

	err := checkKeyInShard(rangeKeys.MustBack().Value.([]byte), &current)
	if err != nil {
		logger.Errorf("shard %d split key failed with %+v",
			d.shardID,
			err)
		return nil, nil
	}

	derived.Epoch.Version += uint64(newShardsCount)
	rangeKeys.PushBack(derived.End)
	derived.End = rangeKeys.MustFront().Value.([]byte)

	sort.Slice(derived.Peers, func(i, j int) bool {
		return derived.Peers[i].ID < derived.Peers[j].ID
	})
	for _, req := range splitReqs.Requests {
		newShard := bhmetapb.Shard{}
		newShard.ID = req.NewShardID
		newShard.Group = derived.Group
		newShard.Unique = derived.Unique
		newShard.RuleGroups = derived.RuleGroups
		newShard.DisableSplit = derived.DisableSplit
		newShard.Epoch = derived.Epoch
		newShard.Start = rangeKeys.PopFront().Value.([]byte)
		newShard.End = rangeKeys.MustFront().Value.([]byte)
		for idx, p := range derived.Peers {
			newShard.Peers = append(newShard.Peers, metapb.Peer{
				ID:          req.NewPeerIDs[idx],
				ContainerID: p.ContainerID,
			})
		}

		shards = append(shards, newShard)
		ctx.metrics.admin.splitSucceed++
	}

	// TODO(fagongzi): split with sync
	// e := d.dataStorage.Sync(d.shardID)
	// if e != nil {
	// 	logger.Fatalf("%s sync failed with %+v", d.pr.id(), e)
	// }

	// if d.store.cfg.Customize.CustomSplitCompletedFuncFactory != nil {
	// 	if fn := d.store.cfg.Customize.CustomSplitCompletedFuncFactory(derived.Group); fn != nil {
	// 		fn(&derived, shards)
	// 	}
	// }

	// d.updateShard(derived)
	// d.saveShardMetedata(d.shardID, d.getShard(), bhraftpb.PeerState_Normal)

	// d.store.updatePeerState(derived, bhraftpb.PeerState_Normal, ctx.raftWB)
	// for _, shard := range shards {
	// 	d.store.updatePeerState(shard, bhraftpb.PeerState_Normal, ctx.raftWB)
	// 	d.store.writeInitialState(shard.ID, ctx.raftWB)
	// }

	// rsp := newAdminRaftCMDResponse(raftcmdpb.AdminCmdType_BatchSplit, &raftcmdpb.BatchSplitResponse{
	// 	Shards: shards,
	// })

	// result := &adminExecResult{
	// 	adminType: raftcmdpb.AdminCmdType_BatchSplit,
	// 	splitResult: &splitResult{
	// 		derived: derived,
	// 		shards:  shards,
	// 	},
	// }
	return nil, nil
}

func (d *stateMachine) execWriteRequest(ctx *applyContext) *raftcmdpb.RaftCMDResponse {
	ctx.writeCtx.reset(d.getShard())
	ctx.writeCtx.appendRequest(ctx.req)
	for _, req := range ctx.req.Requests {
		logger2.Debug("start to execute write", log.HexField("id", req.ID))
	}
	if err := d.dataStorage.GetCommandExecutor().ExecuteWrite(ctx.writeCtx); err != nil {
		logger.Fatalf("%s exec read cmd failed with %+v",
			d.pr.id(),
			err)
	}
	for _, req := range ctx.req.Requests {
		logger2.Debug("execute write completed", log.HexField("id", req.ID))
	}

	resp := pb.AcquireRaftCMDResponse()
	for _, v := range ctx.writeCtx.responses {
		ctx.metrics.writtenKeys++
		r := pb.AcquireResponse()
		r.Value = v
		resp.Responses = append(resp.Responses, r)
	}
	d.updateWriteMetrics(ctx)
	return resp
}

func (d *stateMachine) updateWriteMetrics(ctx *applyContext) {
	ctx.metrics.writtenBytes += ctx.writeCtx.writtenBytes
	if ctx.writeCtx.diffBytes < 0 {
		v := uint64(math.Abs(float64(ctx.writeCtx.diffBytes)))
		if v >= ctx.metrics.sizeDiffHint {
			ctx.metrics.sizeDiffHint = 0
		} else {
			ctx.metrics.sizeDiffHint -= v
		}
	} else {
		ctx.metrics.sizeDiffHint += uint64(ctx.writeCtx.diffBytes)
	}
}

func (d *stateMachine) saveShardMetedata(index uint64, shard bhmetapb.Shard, state bhmetapb.PeerState) error {
	return d.dataStorage.SaveShardMetadata(storage.ShardMetadata{
		ShardID:  shard.ID,
		LogIndex: index,
		Metadata: protoc.MustMarshal(&bhmetapb.ShardLocalState{
			State: state,
			Shard: shard,
		}),
	})
}
