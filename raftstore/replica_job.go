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
	"github.com/matrixorigin/matrixcube/components/keys"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"go.uber.org/zap"
)

// remove replica from current node.
// 1. In raft rpc thread:        after receiving messages from other nodes, it is found that the current replica is stale.
// 2. In raft event loop thread: after conf change, it is found that the current replica is removed.
// 3.
func (pr *replica) startApplyDestroy(tombstoneInCluster bool, why string) {
	pr.logger.Info("begin to destory",
		zap.Bool("tombstone-in-cluster", tombstoneInCluster),
		log.ReasonField(why))

	pr.store.removeDroppedVoteMsg(pr.shardID)
	pr.stopEventLoop()

	if err := pr.doApplyDestory(tombstoneInCluster); err != nil {
		pr.logger.Fatal("fail to destory",
			zap.Error(err))
	}
}

func (pr *replica) startSplitCheckJob() error {
	shard := pr.getShard()
	epoch := shard.Epoch
	startKey := keys.EncStartKey(&shard)
	endKey := keys.EncEndKey(&shard)

	pr.logger.Info("start split check job",
		log.HexField("from", startKey),
		log.HexField("end", endKey))
	return pr.doSplitCheck(epoch, startKey, endKey)
}

func (pr *replica) doPropose(c batch, isConfChange bool) error {
	if isConfChange {
		changeC := pr.pendingProposals.getConfigChange()
		if changeC.req != nil && changeC.req.Header != nil {
			changeC.notifyStaleCmd()
		}
		pr.pendingProposals.setConfigChange(c)
	} else {
		pr.pendingProposals.append(c)
	}

	return nil
}

func (pr *replica) doSplitCheck(epoch metapb.ResourceEpoch, startKey, endKey []byte) error {
	if !pr.isLeader() {
		return nil
	}

	var size uint64
	var keys uint64
	var splitKeys [][]byte
	var err error

	useDefault := true
	shard := pr.getShard()
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
