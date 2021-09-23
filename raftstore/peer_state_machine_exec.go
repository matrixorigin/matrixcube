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
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/storage"
	"go.uber.org/zap"
)

func (d *stateMachine) execAdminRequest(ctx *applyContext) (*rpc.ResponseBatch, error) {
	cmdType := ctx.req.AdminRequest.CmdType
	switch cmdType {
	case rpc.AdminCmdType_ConfigChange:
		return d.doExecChangePeer(ctx)
	case rpc.AdminCmdType_ConfigChangeV2:
		return d.doExecChangePeerV2(ctx)
	case rpc.AdminCmdType_BatchSplit:
		return d.doExecSplit(ctx)
	}

	return nil, nil
}

func (d *stateMachine) doExecChangePeer(ctx *applyContext) (*rpc.ResponseBatch, error) {
	req := ctx.req.AdminRequest.ConfigChange
	peer := req.Peer
	current := d.getShard()

	logger2.Info("begin to apply change peer",
		d.pr.field,
		zap.Uint64("index", ctx.entry.Index),
		log.ShardField("current", current),
		log.ConfigChangeField("request", req))

	res := meta.Shard{}
	protoc.MustUnmarshal(&res, protoc.MustMarshal(&current))
	res.Epoch.ConfVer++

	p := findPeer(&res, req.Peer.ContainerID)
	switch req.ChangeType {
	case metapb.ChangePeerType_AddNode:
		exists := false
		if p != nil {
			exists = true
			if p.Role != metapb.PeerRole_Learner || p.ID != peer.ID {
				return nil, fmt.Errorf("shard %d can't add duplicated peer %+v",
					res.ID,
					peer)
			}
			p.Role = metapb.PeerRole_Voter
		}

		if !exists {
			res.Peers = append(res.Peers, peer)
		}
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
	case metapb.ChangePeerType_AddLearnerNode:
		if p != nil {
			return nil, fmt.Errorf("shard-%d can't add duplicated learner %+v",
				res.ID,
				peer)
		}

		res.Peers = append(res.Peers, peer)
	}

	state := meta.PeerState_Normal
	if d.isPendingRemove() {
		state = meta.PeerState_Tombstone
	}

	d.updateShard(res)
	err := d.saveShardMetedata(ctx.entry.Index, res, state)
	if err != nil {
		logger2.Fatal("fail to save metadata",
			d.pr.field,
			zap.Error(err))
	}

	logger2.Info("apply change peer complete",
		d.pr.field,
		log.ShardField("metadata", res),
		zap.String("state", state.String()))

	resp := newAdminResponseBatch(rpc.AdminCmdType_ConfigChange, &rpc.ConfigChangeResponse{
		Shard: res,
	})
	ctx.adminResult = &adminExecResult{
		adminType: rpc.AdminCmdType_ConfigChange,
		changePeerResult: &changePeerResult{
			index:   ctx.entry.Index,
			changes: []rpc.ConfigChangeRequest{*req},
			shard:   res,
		},
	}
	return resp, nil
}

func (d *stateMachine) doExecChangePeerV2(ctx *applyContext) (*rpc.ResponseBatch, error) {
	req := ctx.req.AdminRequest.ConfigChangeV2
	changes := req.Changes
	current := d.getShard()

	logger2.Info("begin to apply change peer v2",
		d.pr.field,
		zap.Uint64("index", ctx.entry.Index),
		log.ShardField("current", current),
		log.ConfigChangesField("requests", changes))

	var res meta.Shard
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

	state := meta.PeerState_Normal
	if d.isPendingRemove() {
		state = meta.PeerState_Tombstone
	}

	d.updateShard(res)
	err = d.saveShardMetedata(ctx.entry.Index, res, state)
	if err != nil {
		logger2.Fatal("fail to save metadata",
			d.pr.field,
			zap.Error(err))
	}

	logger2.Info("apply change peer v2 complete",
		d.pr.field,
		log.ShardField("metadata", res),
		zap.String("state", state.String()))

	resp := newAdminResponseBatch(rpc.AdminCmdType_ConfigChange, &rpc.ConfigChangeResponse{
		Shard: res,
	})
	ctx.adminResult = &adminExecResult{
		adminType: rpc.AdminCmdType_ConfigChange,
		changePeerResult: &changePeerResult{
			index:   ctx.entry.Index,
			changes: changes,
			shard:   res,
		},
	}
	return resp, nil
}

func (d *stateMachine) applyConfChangeByKind(kind confChangeKind, changes []rpc.ConfigChangeRequest) (meta.Shard, error) {
	res := meta.Shard{}
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
	return res, nil
}

func (d *stateMachine) applyLeaveJoint() (meta.Shard, error) {
	shard := meta.Shard{}
	current := d.getShard()
	protoc.MustUnmarshal(&shard, protoc.MustMarshal(&current))

	change_num := uint64(0)
	for idx := range shard.Peers {
		if shard.Peers[idx].Role == metapb.PeerRole_IncomingVoter {
			shard.Peers[idx].Role = metapb.PeerRole_Voter
			continue
		}

		if shard.Peers[idx].Role == metapb.PeerRole_DemotingVoter {
			shard.Peers[idx].Role = metapb.PeerRole_Learner
			continue
		}

		change_num += 1
	}
	if change_num == 0 {
		logger2.Fatal("can't leave a non-joint config",
			d.pr.field,
			log.ShardField("shard", shard))
	}
	shard.Epoch.ConfVer += change_num
	return shard, nil
}

func (d *stateMachine) doExecSplit(ctx *applyContext) (*rpc.ResponseBatch, error) {
	ctx.metrics.admin.split++
	splitReqs := ctx.req.AdminRequest.Splits

	if len(splitReqs.Requests) == 0 {
		logger.Errorf("shard %d missing splits request", d.shardID)
		return nil, errors.New("missing splits request")
	}

	newShardsCount := len(splitReqs.Requests)
	derived := meta.Shard{}
	current := d.getShard()
	protoc.MustUnmarshal(&derived, protoc.MustMarshal(&current))
	var shards []meta.Shard
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
		newShard := meta.Shard{}
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

	// rsp := newAdminResponseBatch(rpc.AdminCmdType_BatchSplit, &rpc.BatchSplitResponse{
	// 	Shards: shards,
	// })

	// result := &adminExecResult{
	// 	adminType: rpc.AdminCmdType_BatchSplit,
	// 	splitResult: &splitResult{
	// 		derived: derived,
	// 		shards:  shards,
	// 	},
	// }
	return nil, nil
}

func (d *stateMachine) execWriteRequest(ctx *applyContext) *rpc.ResponseBatch {
	ctx.writeCtx.reset(d.getShard())
	ctx.writeCtx.appendRequest(ctx.req)
	for _, req := range ctx.req.Requests {
		logger2.Debug("start to execute write",
			d.pr.field,
			log.HexField("id", req.ID))
	}
	if err := d.dataStorage.GetCommandExecutor().ExecuteWrite(ctx.writeCtx); err != nil {
		logger.Fatal("fail to exec read cmd",
			d.pr.field,
			zap.Error(err))
	}
	for _, req := range ctx.req.Requests {
		logger2.Debug("execute write completed",
			d.pr.field,
			log.HexField("id", req.ID))
	}

	resp := pb.AcquireResponseBatch()
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

func (d *stateMachine) saveShardMetedata(index uint64, shard meta.Shard, state meta.PeerState) error {
	return d.dataStorage.SaveShardMetadata(storage.ShardMetadata{
		ShardID:  shard.ID,
		LogIndex: index,
		Metadata: protoc.MustMarshal(&meta.ShardLocalState{
			State: state,
			Shard: shard,
		}),
	})
}
