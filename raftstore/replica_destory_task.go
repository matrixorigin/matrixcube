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
	"context"
	"time"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	trackerPkg "go.etcd.io/etcd/raft/v3/tracker"
	"go.uber.org/zap"
)

var (
	checkerInterval = time.Second * 5

	destroyShardTaskField = zap.String("task", "destroy-shard-task")
)

type actionHandleFunc func(action)

type destroyingStorage interface {
	CreateDestroying(id uint64, index uint64, removeData bool, replicas []uint64) (metapb.ResourceState, error)
	ReportDestroyed(id uint64, replicaID uint64) (metapb.ResourceState, error)
	GetDestroying(id uint64) (*metapb.DestroyingStatus, error)
}

// startDestoryReplicaTaskAfterSplitted starts a task used to destory old replicas. We cannot directly delete the current replica,
// because once deleted, it may result in some Replica not receiving Split's Log.
// So we start a timer that periodically detects the replication of the Log, and once all the Replica are replicated,
// set the condition on `Prophet` that the Shard can be deleted (AppliedIndex >= SplitLogIndex).
func (pr *replica) startDestoryReplicaTaskAfterSplitted() {
	pr.startDestoryReplicaTask(pr.appliedIndex, false, "splitted")
}

// startDestoryReplicaTask starts a task to destory all replicas of the shard after the specified Log applied.
func (pr *replica) startDestoryReplicaTask(targetIndex uint64, removeData bool, reason string) {
	task := func(ctx context.Context) {
		pr.destoryReplicaAfterLogApplied(ctx, targetIndex, pr.addAction, pr.store.pd.GetClient(), removeData, reason)
	}

	if err := pr.readStopper.RunNamedTask(context.Background(), reason, task); err != nil {
		pr.logger.Error("failed to start async task for destory replica")
	}
}

// destoryReplicaAfterLogApplied this task will be executed on all Replicas of a Shard. The task has 4 steps:
// 1. Periodically check all replicas for CommitIndex >= TargetIndex
// 2. Put the check result into storage
// 3. Periodically check all replicas for AppliedIndex >= TargetIndex
// 4. Perform destruction of replica
func (pr *replica) destoryReplicaAfterLogApplied(ctx context.Context,
	targetIndex uint64,
	actionHandler actionHandleFunc,
	client destroyingStorage,
	removeData bool,
	reason string) {

	pr.logger.Info("start",
		destroyShardTaskField,
		zap.Uint64("target", targetIndex),
		zap.Bool("remove-data", removeData),
		log.ReasonField(reason))

	// we need check the destory record exist if restart.
	doCheckDestoryExists := true
	doCheckLog := false
	doCheckApply := false

	checkTimer := time.NewTimer(checkerInterval)
	defer checkTimer.Stop()

	doSaveDestoryC := make(chan []uint64)
	defer close(doSaveDestoryC)

	doRealDestoryC := make(chan struct{})
	defer close(doRealDestoryC)

	watiToRetry := func() {
		time.Sleep(time.Second)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-checkTimer.C:
			if doCheckDestoryExists {
				status, err := client.GetDestroying(pr.shardID)
				if err != nil {
					pr.logger.Error("failed to get shard destory metadata, retry later",
						destroyShardTaskField,
						zap.Error(err))
					watiToRetry()
					break
				}

				if status == nil {
					// no destorying record exists means we need check log committed info, start step 1
					doCheckLog = true
					pr.logger.Debug("begin to check log committed",
						destroyShardTaskField)
				} else {
					// means all replica committed at targetIndex, start step 3
					doCheckApply = true
					doCheckDestoryExists = false
					pr.logger.Debug("begin to check log applied",
						destroyShardTaskField)
				}
			}

			// step 1
			if doCheckLog {
				actionHandler(action{
					actionType:  checkLogCommittedAction,
					targetIndex: targetIndex,
					actionCallback: func(replicas interface{}) {
						doSaveDestoryC <- replicas.([]uint64)
						pr.logger.Debug("log committed on all replicas",
							destroyShardTaskField)
					},
				})
			}

			// start 3
			if doCheckApply {
				actionHandler(action{
					actionType:  checkLogAppliedAction,
					targetIndex: targetIndex,
					actionCallback: func(i interface{}) {
						doRealDestoryC <- struct{}{}
						pr.logger.Debug("log applied",
							destroyShardTaskField)
					},
				})
			}

			checkTimer.Reset(checkerInterval)
		case replicas := <-doSaveDestoryC: // step 2
			// leader path, only leader node can reach here.
			doCheckLog = false

			// Reaching here means that all Replicas of the Shard have completed the log replication of
			// the TargetIndex and have been Committed.
			// Only if the Destoryed information corresponding to the Shard is set in storage, all replicas
			// can be safely deleted.
			_, err := client.CreateDestroying(pr.shardID, targetIndex, removeData, replicas)
			if err != nil {
				pr.logger.Error("failed to save replica destory metadata, retry later",
					destroyShardTaskField,
					zap.Error(err))
				watiToRetry()
				doSaveDestoryC <- replicas // retry
				break
			}

			doCheckApply = true
			doCheckDestoryExists = false
			pr.logger.Debug("destory metadata saved",
				destroyShardTaskField)
		case <-doRealDestoryC: // step 4
			if _, err := client.ReportDestroyed(pr.shardID, pr.replica.ID); err != nil {
				pr.logger.Error("failed to report destroying status, retry later",
					destroyShardTaskField,
					zap.Error(err))
				watiToRetry()
				doRealDestoryC <- struct{}{} // retry
				break
			}

			pr.store.destroyReplica(pr.shardID, true, removeData, reason)
			pr.logger.Debug("completed",
				destroyShardTaskField)
			return
		}
	}
}

func (pr *replica) doCheckLogCommitted(act action) {
	if !pr.isLeader() {
		pr.logger.Debug("skip check log committed",
			destroyShardTaskField,
			log.ReasonField("not leader"))
		return
	}

	status := pr.rn.Status()
	if len(status.Progress) == 0 {
		pr.logger.Debug("skip check log committed",
			destroyShardTaskField,
			log.ReasonField("no progress"))
		return
	}

	var replicas []uint64
	for id, p := range status.Progress {
		if p.State == trackerPkg.StateSnapshot {
			pr.logger.Debug("check log committed failed",
				destroyShardTaskField,
				log.ReasonField("replica in snapshot"))
			return
		}

		if pr.committedIndexes[id] < act.targetIndex {
			pr.logger.Debug("check log committed failed",
				destroyShardTaskField,
				log.ReasonField("committed too small"),
				zap.Uint64("lag-replica-id", id),
				zap.Uint64("current-committed", pr.committedIndexes[id]),
				zap.Uint64("target-committed", act.targetIndex))
			return
		}

		replicas = append(replicas, id)
	}

	// All replica committedIndex >= targetIndex.
	// The shard state in the state machine is Destroying, and Prophet does not add new replica,
	// the only thing it can do is remove the offline replica.
	// So eventually it must be executed here, means that all replicas commit the target raft log.
	act.actionCallback(replicas)
}

func (pr *replica) doCheckLogApplied(act action) {
	if pr.appliedIndex >= act.targetIndex {
		act.actionCallback(nil)
	}
}
