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
	"math"

	"github.com/cockroachdb/errors"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/meta"
	"github.com/matrixorigin/matrixcube/pb/rpc"
	"github.com/matrixorigin/matrixcube/storage"
	"go.uber.org/zap"
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
	case rpc.AdminCmdType_UpdateMetadata:
		return d.doUpdateMetadata(ctx)
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

			if d.replica.ID == replica.ID {
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
	if err := d.saveShardMetedata(ctx.index, ctx.term, res, state); err != nil {
		d.logger.Fatal("failed to save metadata",
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
		d.logger.Fatal("missing splits request")
	}

	current := d.getShard().Clone()
	if !bytes.Equal(splitReqs.Requests[0].Start, current.Start) ||
		!bytes.Equal(splitReqs.Requests[len(splitReqs.Requests)-1].End, current.End) {
		d.logger.Fatal("invalid splits keys",
			log.HexField("actual-start", splitReqs.Requests[0].Start),
			log.HexField("shard-start", current.Start),
			log.HexField("actual-end", splitReqs.Requests[len(splitReqs.Requests)-1].End),
			log.HexField("shard-end", current.End))
	}

	newShardsCount := len(splitReqs.Requests)
	var newShards []Shard
	var metadata []meta.ShardMetadata
	current.Epoch.Version += uint64(newShardsCount)
	expectStart := current.Start
	last := len(splitReqs.Requests) - 1
	for idx, req := range splitReqs.Requests {
		if checkKeyInShard(req.Start, current) != nil ||
			(idx != last && checkKeyInShard(req.End, current) != nil) {
			d.logger.Fatal("invalid split reuqest range",
				log.HexField("split-start", req.Start),
				log.HexField("split-end", req.End),
				log.HexField("expect-start", current.Start),
				log.HexField("expect-end", current.End))
		}

		if !bytes.Equal(req.Start, expectStart) {
			d.logger.Fatal("invalid split reuqest start key",
				log.HexField("split-start", req.Start),
				log.HexField("expect-start", expectStart))
		}
		expectStart = req.End

		newShard := Shard{}
		newShard.ID = req.NewShardID
		newShard.Group = current.Group
		newShard.Unique = current.Unique
		newShard.RuleGroups = current.RuleGroups
		newShard.DisableSplit = current.DisableSplit
		newShard.Epoch = current.Epoch
		newShard.Start = req.Start
		newShard.End = req.End
		newShard.Replicas = req.NewReplicas
		newShards = append(newShards, newShard)
		ctx.metrics.admin.splitSucceed++
	}

	d.shardCreatorFactory().
		withReason("splited").
		withLogdbContext(d.wc).
		withSaveMetadata(false).
		create(newShards)

	old := meta.ShardMetadata{
		ShardID:  current.ID,
		LogIndex: ctx.index,
		LogTerm:  ctx.term,
		Metadata: meta.ShardLocalState{
			State: meta.ReplicaState_Tombstone,
			Shard: current,
		},
	}
	err := d.dataStorage.Split(old, metadata, splitReqs.Context)
	if err != nil {
		if err == storage.ErrAborted {
			return rpc.ResponseBatch{}, nil
		}
		d.logger.Fatal("failed to split on data storage",
			zap.Error(err))
	}

	d.setSplited()
	resp := newAdminResponseBatch(rpc.AdminCmdType_BatchSplit, &rpc.BatchSplitResponse{
		Shards: newShards,
	})
	ctx.adminResult = &adminResult{
		adminType: rpc.AdminCmdType_BatchSplit,
		splitResult: &splitResult{
			newShards: newShards,
		},
	}
	return resp, nil
}

func (d *stateMachine) doUpdateMetadata(ctx *applyContext) (rpc.ResponseBatch, error) {
	ctx.metrics.admin.updateMetadata++
	updateReq := ctx.req.AdminRequest.UpdateMetadata

	current := d.getShard()
	if isEpochStale(current.Epoch, updateReq.Metadata.Shard.Epoch) {
		d.logger.Fatal("failed to update metadata",
			log.EpochField("current", current.Epoch),
			log.ShardField("new-shard", updateReq.Metadata.Shard))
	}

	err := d.dataStorage.SaveShardMetadata([]meta.ShardMetadata{
		{
			ShardID:  d.shardID,
			LogIndex: ctx.index,
			LogTerm:  ctx.term,
			Metadata: updateReq.Metadata,
		},
	})
	if err != nil {
		d.logger.Fatal("failed to update metadata",
			log.EpochField("current", current.Epoch),
			log.ShardField("new-shard", updateReq.Metadata.Shard),
			zap.Error(err))
	}

	d.updateShard(updateReq.Metadata.Shard)

	d.logger.Info("shard metadata updated",
		zap.String("replica-state", updateReq.Metadata.State.String()),
		log.ShardField("new-shard", updateReq.Metadata.Shard),
	)

	resp := newAdminResponseBatch(rpc.AdminCmdType_UpdateMetadata, &rpc.UpdateMetadataResponse{})
	ctx.adminResult = &adminResult{
		adminType: rpc.AdminCmdType_UpdateMetadata,
	}
	return resp, nil
}

func (d *stateMachine) execWriteRequest(ctx *applyContext) rpc.ResponseBatch {
	d.writeCtx.initialize(d.getShard(), ctx.index, ctx.term, ctx.req)
	for _, req := range ctx.req.Requests {
		if ce := d.logger.Check(zap.DebugLevel, "begin to execute write"); ce != nil {
			ce.Write(log.HexField("id", req.ID),
				log.ShardIDField(d.shardID),
				log.ReplicaIDField(d.replica.ID),
				log.IndexField(ctx.index))
		}
	}
	if err := d.dataStorage.Write(d.writeCtx); err != nil {
		d.logger.Fatal("failed to exec read cmd",
			zap.Error(err))
	}
	for _, req := range ctx.req.Requests {
		if ce := d.logger.Check(zap.DebugLevel, "write completed"); ce != nil {
			ce.Write(log.HexField("id", req.ID),
				log.ShardIDField(d.shardID),
				log.ReplicaIDField(d.replica.ID),
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
		if v >= d.applyCtx.metrics.approximateDiffHint {
			d.applyCtx.metrics.approximateDiffHint = 0
		} else {
			d.applyCtx.metrics.approximateDiffHint -= v
		}
	} else {
		d.applyCtx.metrics.approximateDiffHint += uint64(d.writeCtx.diffBytes)
	}
}

func (d *stateMachine) saveShardMetedata(index uint64, term uint64,
	shard Shard, state meta.ReplicaState) error {
	return d.dataStorage.SaveShardMetadata([]meta.ShardMetadata{{
		ShardID:  shard.ID,
		LogIndex: index,
		LogTerm:  term,
		Metadata: meta.ShardLocalState{
			State: state,
			Shard: shard,
		},
	}})
}
