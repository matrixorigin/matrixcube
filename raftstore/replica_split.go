// Copyright 2021 MatrixOrigin.
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
	"go.etcd.io/etcd/raft/v3/tracker"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/keys"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/pb/rpc"
)

func (pr *replica) tryCheckSplit() {
	if !pr.isLeader() {
		return
	}

	for id, p := range pr.rn.Status().Progress {
		// If a peer is apply snapshot, skip split, avoid sent snapshot again in future.
		if p.State == tracker.StateSnapshot {
			pr.logger.Info("check split skipped",
				log.ReplicaIDField(id),
				log.ReasonField("applying snapshot"))
			return
		}
	}

	if err := pr.doCheckSplit(); err != nil {
		pr.logger.Fatal("fail to add split check job",
			zap.Error(err))
	}
	pr.sizeDiffHint = 0
}

func (pr *replica) doSplit(splitKeys [][]byte, splitIDs []rpcpb.SplitID, epoch metapb.ResourceEpoch) {
	if !pr.isLeader() {
		return
	}

	current := pr.getShard()
	if current.Epoch.Version != epoch.Version {
		pr.logger.Info("epoch changed, need re-check later",
			log.EpochField("current-epoch", current.Epoch),
			log.EpochField("check-epoch", epoch))
		return
	}

	req := rpc.AdminRequest{
		CmdType: rpc.AdminCmdType_BatchSplit,
		Splits:  &rpc.BatchSplitRequest{},
	}
	for idx := range splitIDs {
		req.Splits.Requests = append(req.Splits.Requests, rpc.SplitRequest{
			SplitKey:      splitKeys[idx],
			NewShardID:    splitIDs[idx].NewID,
			NewReplicaIDs: splitIDs[idx].NewReplicaIDs,
		})
	}
	pr.addAdminRequest(req)
}

func (pr *replica) doCheckSplit() error {
	shard := pr.getShard()
	epoch := shard.Epoch
	startKey := keys.EncodeStartKey(shard, nil)
	endKey := keys.EncodeEndKey(shard, nil)

	pr.logger.Info("start split check job",
		log.HexField("from", startKey),
		log.HexField("end", endKey))

	var size uint64
	var keys uint64
	var splitKeys [][]byte
	var err error

	useDefault := true
	if pr.store.cfg.Customize.CustomSplitCheckFuncFactory != nil {
		if fn := pr.store.cfg.Customize.CustomSplitCheckFuncFactory(shard.Group); fn != nil {
			size, keys, splitKeys, err = fn(shard)
			useDefault = false
		}
	}

	if useDefault {
		size, keys, splitKeys, err = pr.store.DataStorageByGroup(shard.Group).SplitCheck(startKey, endKey, uint64(pr.store.cfg.Replication.ShardCapacityBytes))
	}

	pr.logger.Debug("split check result",
		zap.Uint64("size", size),
		zap.Uint64("capacity", uint64(pr.store.cfg.Replication.ShardCapacityBytes)),
		zap.Uint64("keys", keys),
		zap.ByteStrings("split-keys", splitKeys))

	if err != nil {
		pr.logger.Error("fail to scan split key",
			zap.Error(err))
		return err
	}

	pr.approximateSize = size
	pr.approximateKeys = keys
	if len(splitKeys) == 0 {
		pr.sizeDiffHint = size
		return nil
	}

	pr.logger.Info("try to split",
		zap.Uint64("keys", keys),
		zap.ByteStrings("split-keys", splitKeys))

	current := pr.getShard()
	if current.Epoch.Version != epoch.Version {
		pr.logger.Info("epoch changed, need re-check later",
			log.EpochField("current-epoch", current.Epoch),
			log.EpochField("check-epoch", epoch))
		return nil
	}

	newIDs, err := pr.store.pd.GetClient().AskBatchSplit(NewResourceAdapterWithShard(current), uint32(len(splitKeys)))
	if err != nil {
		pr.logger.Error("fail to ask batch split",
			zap.Error(err))
		return err
	}

	pr.addAction(action{actionType: doSplitAction, splitKeys: splitKeys, splitIDs: newIDs, epoch: epoch})
	return nil
}
