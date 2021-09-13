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
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func (pr *peerReplica) startApplyCommittedEntriesJob(shardID uint64,
	commitedEntries []raftpb.Entry) error {
	err := pr.store.addApplyJob(pr.applyWorker, "doApplyCommittedEntries", func() error {
		return pr.doApplyCommittedEntries(shardID, commitedEntries)
	}, nil)
	return err
}

func (pr *peerReplica) startCompactRaftLogJob(shardID, startIndex, endIndex uint64) error {
	err := pr.store.addApplyJob(pr.applyWorker, "doCompactRaftLog", func() error {
		return pr.doCompactRaftLog(shardID, startIndex, endIndex)
	}, nil)

	return err
}

func (s *store) startDestroyJob(shardID uint64, peer metapb.Peer, why string) error {
	pr := s.getPR(shardID, false)
	if pr != nil {
		err := s.addApplyJob(pr.applyWorker, "doDestroy", func() error {
			s.doDestroy(shardID, false, why)
			return nil
		}, nil)
		return err
	}

	return nil
}

func (pr *peerReplica) startProposeJob(c cmd, isConfChange bool) error {
	err := pr.store.addApplyJob(pr.applyWorker, "doPropose", func() error {
		return pr.doPropose(c, isConfChange)
	}, nil)

	return err
}

func (pr *peerReplica) startSplitCheckJob() error {
	shard := pr.shard
	epoch := shard.Epoch
	startKey := keys.EncStartKey(&shard)
	endKey := keys.EncEndKey(&shard)

	logger.Infof("shard %d start split check job from %+v to %+v",
		pr.shard.ID,
		startKey,
		endKey)
	err := pr.store.addSplitJob(func() error {
		return pr.doSplitCheck(epoch, startKey, endKey)
	})

	return err
}

func (pr *peerReplica) doPropose(c cmd, isConfChange bool) error {
	value, ok := pr.store.delegates.Load(pr.shardID)
	if !ok {
		c.respShardNotFound(pr.shardID)
		return nil
	}

	delegate := value.(*applyDelegate)
	if delegate.shard.ID != pr.shardID {
		logger.Fatal("BUG: delegate id not match")
	}

	if isConfChange {
		changeC := delegate.pendingChangePeerCMD
		if changeC.req != nil && changeC.req.Header != nil {
			delegate.notifyStaleCMD(changeC)
		}
		delegate.pendingChangePeerCMD = c
	} else {
		delegate.appendPendingCmd(c)
	}

	return nil
}

func (pr *peerReplica) doSplitCheck(epoch metapb.ResourceEpoch, startKey, endKey []byte) error {
	if !pr.isLeader() {
		return nil
	}

	var size uint64
	var keys uint64
	var splitKeys [][]byte
	var err error

	useDefault := true
	if pr.store.cfg.Customize.CustomSplitCheckFuncFactory != nil {
		if fn := pr.store.cfg.Customize.CustomSplitCheckFuncFactory(pr.shard.Group); fn != nil {
			size, keys, splitKeys, err = fn(pr.shard)
			useDefault = false
		}
	}

	if useDefault {
		size, keys, splitKeys, err = pr.store.DataStorageByGroup(pr.shard.Group, pr.shard.ID).SplitCheck(startKey, endKey, uint64(pr.store.cfg.Replication.ShardCapacityBytes))
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

	current := pr.shard
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
