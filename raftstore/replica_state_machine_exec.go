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
	"fmt"
	"math"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/fagongzi/util/collection/deque"
	"github.com/fagongzi/util/protoc"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/storage"
)

var (
	ErrNotLearnerReplica = errors.New("not learner")
	ErrReplicaNotFound   = errors.New("replica not found")
	ErrReplicaDuplicated = errors.New("replica duplicated")
)

func (d *stateMachine) execAdminRequest(ctx *applyContext) (rpc.ResponseBatch, error) {
	cmdType := ctx.req.AdminRequest.CmdType
	switch cmdType {
	case rpc.AdminCmdType_ConfigChange:
		return d.doExecConfigChange(ctx)
	case rpc.AdminCmdType_ConfigChangeV2:
		panic("ConfigChangeV2 requested")
	case rpc.AdminCmdType_BatchSplit:
		return d.doExecSplit(ctx)
	}

	return rpc.ResponseBatch{}, nil
}

func (d *stateMachine) doExecConfigChange(ctx *applyContext) (rpc.ResponseBatch, error) {
	req := ctx.req.AdminRequest.ConfigChange
	replica := req.Replica
	current := d.getShard()

	d.logger.Info("begin to apply change replica",
		zap.Uint64("index", ctx.index),
		log.ShardField("current", current),
		log.ConfigChangeField("request", req))

	res := Shard{}
	protoc.MustUnmarshal(&res, protoc.MustMarshal(&current))
	res.Epoch.ConfVer++
	p := findReplica(res, replica.ContainerID)
	switch req.ChangeType {
	case metapb.ConfigChangeType_AddNode:
		exists := false
		if p != nil {
			exists = true
			if p.ID == replica.ID {
				if p.Role != metapb.ReplicaRole_Learner {
					err := errors.Wrapf(ErrReplicaDuplicated,
						"shardID %d, replicaID %d, role %v", res.ID, p.ID, p.Role)
					return rpc.ResponseBatch{}, err
				}
			} else {
				err := errors.Wrapf(ErrReplicaDuplicated,
					"shardID %d, replicaID %d found on container %d", res.ID, p.ID, replica.ContainerID)
				return rpc.ResponseBatch{}, err
			}
			p.Role = metapb.ReplicaRole_Voter
			d.logger.Info("learner promoted to voter",
				log.ReplicaField("replica", *p),
				log.StoreIDField(replica.ContainerID))
		}
		if !exists {
			replica.Role = metapb.ReplicaRole_Voter
			res.Replicas = append(res.Replicas, replica)
		}
	case metapb.ConfigChangeType_RemoveNode:
		if p != nil {
			if p.ID != replica.ID {
				err := errors.Wrapf(ErrReplicaNotFound,
					"shardID %d, replicaID %d found on container %d", res.ID, p.ID, replica.ContainerID)
				return rpc.ResponseBatch{}, err
			} else {
				removeReplica(&res, replica.ContainerID)
			}

			if d.replicaID == replica.ID {
				// Remove ourself, will destroy all shard data later.
				d.setRemoved()
				d.logger.Info("replica remoted itself",
					log.ReplicaField("replica", *p),
					log.StoreIDField(replica.ContainerID))
			}
		} else {
			err := errors.Wrapf(ErrReplicaNotFound,
				"shardID %d, replicaID %d found on container %d", res.ID, p.ID, replica.ContainerID)
			return rpc.ResponseBatch{}, err
		}
	case metapb.ConfigChangeType_AddLearnerNode:
		if p != nil {
			err := errors.Wrapf(ErrReplicaDuplicated,
				"shardID %d, replicaID %d role %v already exist on store %d",
				res.ID, p.ID, p.Role, replica.ContainerID)
			return rpc.ResponseBatch{}, err
		}
		replica.Role = metapb.ReplicaRole_Learner
		res.Replicas = append(res.Replicas, replica)
	}
	state := meta.ReplicaState_Normal
	if d.isRemoved() {
		state = meta.ReplicaState_Tombstone
	}
	d.updateShard(res)
	if err := d.saveShardMetedata(ctx.index, res, state); err != nil {
		d.logger.Fatal("fail to save metadata",
			zap.Error(err))
	}

	d.logger.Info("apply change replica completed",
		log.ShardField("metadata", res),
		zap.String("state", state.String()))

	resp := newAdminResponseBatch(rpc.AdminCmdType_ConfigChange, &rpc.ConfigChangeResponse{
		Shard: res,
	})
	ctx.adminResult = &adminResult{
		adminType: rpc.AdminCmdType_ConfigChange,
		configChangeResult: &configChangeResult{
			index:   ctx.index,
			changes: []rpc.ConfigChangeRequest{*req},
			shard:   res,
		},
	}
	return resp, nil
}

func (d *stateMachine) doExecSplit(ctx *applyContext) (rpc.ResponseBatch, error) {
	ctx.metrics.admin.split++
	splitReqs := ctx.req.AdminRequest.Splits

	if len(splitReqs.Requests) == 0 {
		d.logger.Error("missing splits request")
		return rpc.ResponseBatch{}, errors.New("missing splits request")
	}

	newShardsCount := len(splitReqs.Requests)
	derived := Shard{}
	current := d.getShard()
	protoc.MustUnmarshal(&derived, protoc.MustMarshal(&current))
	var shards []Shard
	rangeKeys := deque.New()

	for _, req := range splitReqs.Requests {
		if len(req.SplitKey) == 0 {
			return rpc.ResponseBatch{}, errors.New("missing split key")
		}

		splitKey := req.SplitKey
		v := derived.Start
		if e, ok := rangeKeys.Back(); ok {
			v = e.Value.([]byte)
		}
		if bytes.Compare(splitKey, v) <= 0 {
			return rpc.ResponseBatch{}, fmt.Errorf("invalid split key %+v", splitKey)
		}

		if len(req.NewReplicaIDs) != len(derived.Replicas) {
			return rpc.ResponseBatch{}, fmt.Errorf("invalid new replica id count, need %d, but got %d",
				len(derived.Replicas),
				len(req.NewReplicaIDs))
		}

		rangeKeys.PushBack(splitKey)
	}

	err := checkKeyInShard(rangeKeys.MustBack().Value.([]byte), current)
	if err != nil {
		d.logger.Error("fail to split key",
			zap.String("err", err.Message))
		return rpc.ResponseBatch{}, nil
	}

	derived.Epoch.Version += uint64(newShardsCount)
	rangeKeys.PushBack(derived.End)
	derived.End = rangeKeys.MustFront().Value.([]byte)

	sort.Slice(derived.Replicas, func(i, j int) bool {
		return derived.Replicas[i].ID < derived.Replicas[j].ID
	})
	for _, req := range splitReqs.Requests {
		newShard := Shard{}
		newShard.ID = req.NewShardID
		newShard.Group = derived.Group
		newShard.Unique = derived.Unique
		newShard.RuleGroups = derived.RuleGroups
		newShard.DisableSplit = derived.DisableSplit
		newShard.Epoch = derived.Epoch
		newShard.Start = rangeKeys.PopFront().Value.([]byte)
		newShard.End = rangeKeys.MustFront().Value.([]byte)
		for idx, p := range derived.Replicas {
			newShard.Replicas = append(newShard.Replicas, Replica{
				ID:          req.NewReplicaIDs[idx],
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
	// d.saveShardMetedata(d.shardID, d.getShard(), bhraftpb.ReplicaState_Normal)

	// d.store.updateReplicaState(derived, bhraftpb.ReplicaState_Normal, ctx.raftWB)
	// for _, shard := range shards {
	// 	d.store.updateReplicaState(shard, bhraftpb.ReplicaState_Normal, ctx.raftWB)
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
	return rpc.ResponseBatch{}, nil
}

func (d *stateMachine) execWriteRequest(ctx *applyContext) rpc.ResponseBatch {
	d.writeCtx.initialize(d.getShard(), ctx.index, ctx.req)
	for _, req := range ctx.req.Requests {
		if ce := d.logger.Check(zap.DebugLevel, "begin to execute write"); ce != nil {
			ce.Write(log.HexField("id", req.ID),
				log.ShardIDField(d.shardID),
				log.ReplicaIDField(d.replicaID),
				log.IndexField(ctx.index))
		}
	}
	if err := d.dataStorage.Write(d.writeCtx); err != nil {
		d.logger.Fatal("fail to exec read cmd",
			zap.Error(err))
	}
	for _, req := range ctx.req.Requests {
		if ce := d.logger.Check(zap.DebugLevel, "write completed"); ce != nil {
			ce.Write(log.HexField("id", req.ID),
				log.ShardIDField(d.shardID),
				log.ReplicaIDField(d.replicaID),
				log.IndexField(ctx.index))
		}
	}

	resp := rpc.ResponseBatch{}
	for _, v := range d.writeCtx.responses {
		ctx.metrics.writtenKeys++
		r := rpc.Response{Value: v}
		resp.Responses = append(resp.Responses, r)
	}
	d.updateWriteMetrics()
	return resp
}

func (d *stateMachine) updateWriteMetrics() {
	d.applyCtx.metrics.writtenBytes += d.writeCtx.writtenBytes
	if d.writeCtx.diffBytes < 0 {
		v := uint64(math.Abs(float64(d.writeCtx.diffBytes)))
		if v >= d.applyCtx.metrics.sizeDiffHint {
			d.applyCtx.metrics.sizeDiffHint = 0
		} else {
			d.applyCtx.metrics.sizeDiffHint -= v
		}
	} else {
		d.applyCtx.metrics.sizeDiffHint += uint64(d.writeCtx.diffBytes)
	}
}

func (d *stateMachine) saveShardMetedata(index uint64,
	shard Shard, state meta.ReplicaState) error {
	return d.dataStorage.SaveShardMetadata([]storage.ShardMetadata{{
		ShardID:  shard.ID,
		LogIndex: index,
		Metadata: protoc.MustMarshal(&meta.ShardLocalState{
			State: state,
			Shard: shard,
		}),
	}})
}
