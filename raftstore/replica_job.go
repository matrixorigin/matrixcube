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
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

func (pr *replica) startApplyCommittedEntriesJob(commitedEntries []raftpb.Entry) error {
	err := pr.store.addApplyJob(pr.applyWorker, "doApplyCommittedEntries", func() error {
		return pr.doApplyCommittedEntries(commitedEntries)
	}, nil)
	return err
}

// remove replica from current node.
// 1. In raft rpc thread:        after receiving messages from other nodes, it is found that the current replica is stale.
// 2. In raft event loop thread: after conf change, it is found that the current replica is removed.
// 3.
func (pr *replica) startApplyDestroy(tombstoneInCluster bool, why string) {
	logger2.Info("begin to destory",
		pr.field,
		zap.Bool("tombstone-in-cluster", tombstoneInCluster),
		log.ReasonField(why))

	pr.store.removeDroppedVoteMsg(pr.shardID)
	pr.stopEventLoop()

	err := pr.store.addApplyJob(pr.applyWorker, "destory", func() error {
		return pr.doApplyDestory(tombstoneInCluster)
	}, nil)
	if err != nil {
		logger.Fatal("fail to destory",
			pr.field,
			zap.Error(err))
	}
}

func (pr *replica) startProposeJob(c batch, isConfChange bool) error {
	err := pr.store.addApplyJob(pr.applyWorker, "doPropose", func() error {
		return pr.doPropose(c, isConfChange)
	}, nil)

	return err
}

func (pr *replica) startSplitCheckJob() error {
	shard := pr.getShard()
	epoch := shard.Epoch
	startKey := keys.EncStartKey(&shard)
	endKey := keys.EncEndKey(&shard)

	logger.Infof("shard %d start split check job from %+v to %+v",
		pr.shardID,
		startKey,
		endKey)
	err := pr.store.addSplitJob(func() error {
		return pr.doSplitCheck(epoch, startKey, endKey)
	})

	return err
}

func (pr *replica) doPropose(c batch, isConfChange bool) error {
	if isConfChange {
		changeC := pr.pendings.getConfigChange()
		if changeC.req != nil && changeC.req.Header != nil {
			changeC.notifyStaleCmd()
		}
		pr.pendings.setConfigChange(c)
	} else {
		pr.pendings.append(c)
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

	logger.Debugf("shard %d split check result, total size %d(%d), total keys %d, split keys %+v",
		pr.shardID,
		size,
		uint64(pr.store.cfg.Replication.ShardCapacityBytes),
		keys,
		splitKeys)

	if err != nil {
		logger.Errorf("shard %d scan split key failed with %+v",
			pr.shardID,
			err)
		return err
	}

	pr.approximateSize = size
	pr.approximateKeys = keys
	if len(splitKeys) == 0 {
		pr.sizeDiffHint = size
		return nil
	}

	logger.Infof("shard %d try to split, size %d bytes splitKeys %+v",
		pr.shardID,
		size,
		splitKeys)

	current := pr.getShard()
	if current.Epoch.Version != epoch.Version {
		logger.Infof("shard %d epoch changed, need re-check later, current=<%+v> split=<%+v>",
			pr.shardID,
			current.Epoch,
			epoch)
		return nil
	}

	newIDs, err := pr.store.pd.GetClient().AskBatchSplit(NewResourceAdapterWithShard(current), uint32(len(splitKeys)))
	if err != nil {
		logger.Errorf("shard %d ask batch split failed with %+v",
			pr.shardID,
			err)
		return err
	}

	pr.addAction(action{actionType: doSplitAction, splitKeys: splitKeys, splitIDs: newIDs, epoch: epoch})
	return nil
}
