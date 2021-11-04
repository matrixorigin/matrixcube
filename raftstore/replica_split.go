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
	trackerPkg "go.etcd.io/etcd/raft/v3/tracker"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/rpc"
)

func (pr *replica) tryCheckSplit(act action) bool {
	if !pr.isLeader() {
		return false
	}

	if !pr.needDoCheckSplit() {
		return false
	}

	// If a replica is applying snapshot, skip split, avoid sent snapshot again in future.
	if ok, id := pr.hasReplicaInSnapshotState(); ok {
		pr.logger.Debug("check split skipped",
			log.ReplicaIDField(id),
			log.ReasonField("applying snapshot"))
		return false
	}

	// We need to do a real split check, a task that may involve a lot of disk IO to find a suitable
	// split point, so it is not suitable to be executed in the current thread, we use a separate goroutine
	// to run this check in callback.
	if act.actionCallback == nil {
		pr.logger.Fatal("fail to start split check task",
			log.ReasonField("missing callback"))
	}

	act.actionCallback(pr.getShard())
	return true
}

func (pr *replica) hasReplicaInSnapshotState() (bool, uint64) {
	for id, p := range pr.rn.Status().Progress {
		// If a peer is apply snapshot, skip split, avoid sent snapshot again in future.
		if p.State == trackerPkg.StateSnapshot {
			return true, id
		}
	}
	return false, 0
}

func (pr *replica) needDoCheckSplit() bool {
	return pr.approximateSize >= uint64(pr.cfg.Replication.ShardSplitCheckBytes)
}

func (pr *replica) doSplit(act action) {
	if !pr.isLeader() {
		return
	}

	epoch := act.epoch
	current := pr.getShard()
	if current.Epoch.Version != epoch.Version {
		pr.logger.Info("epoch changed, need re-check later",
			log.EpochField("current-epoch", current.Epoch),
			log.EpochField("check-epoch", epoch))
		return
	}

	if act.splitCheckData.size > 0 {
		pr.approximateSize = act.splitCheckData.size
	}
	if act.splitCheckData.keys > 0 {
		pr.approximateKeys = act.splitCheckData.keys
	}
	if len(act.splitCheckData.splitKeys) == 0 {
		return
	}

	if len(act.splitCheckData.splitIDs) == 0 {
		pr.logger.Fatal("missing splitIDs")
	}

	if len(act.splitCheckData.splitIDs) != len(act.splitCheckData.splitKeys)+1 {
		pr.logger.Fatal("invalid splitIDs len",
			zap.Int("expect", len(act.splitCheckData.splitKeys)+1),
			zap.Int("but", len(act.splitCheckData.splitIDs)))
	}

	req := rpc.AdminRequest{
		CmdType: rpc.AdminCmdType_BatchSplit,
		Splits: &rpc.BatchSplitRequest{
			Context: act.splitCheckData.ctx,
		},
	}

	start := current.Start
	lastIdx := len(act.splitCheckData.splitIDs) - 1
	for idx := range act.splitCheckData.splitIDs {
		var end []byte
		if idx == lastIdx {
			end = current.End
		} else {
			end = act.splitCheckData.splitKeys[idx]
		}

		var replicas []Replica
		for idIdx, r := range current.Replicas {
			replicas = append(replicas, Replica{
				ID:          act.splitCheckData.splitIDs[idx].NewReplicaIDs[idIdx],
				ContainerID: r.ContainerID,
			})
		}

		req.Splits.Requests = append(req.Splits.Requests, rpc.SplitRequest{
			Start:       start,
			End:         end,
			NewShardID:  act.splitCheckData.splitIDs[idx].NewID,
			NewReplicas: replicas,
		})
		start = end
	}
	pr.addAdminRequest(req)
}
