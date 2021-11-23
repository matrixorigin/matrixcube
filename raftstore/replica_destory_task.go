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
	defaultCheckInterval  = time.Second * 2
	destroyShardTaskField = zap.String("task", "destroy-shard-task")
)

// actionHandleFunc used to testing
type actionHandleFunc func(action)

// destroyingStorage is the storage used to store metadata for shard destruction information.
// The default implementation is to store it on the `Prophet` via the `Prophet`'s client.
// This interface used to testing.
type destroyingStorage interface {
	CreateDestroying(shardID uint64, index uint64, removeData bool, replicas []uint64) (metapb.ResourceState, error)
	ReportDestroyed(shardID uint64, replicaID uint64) (metapb.ResourceState, error)
	GetDestroying(shardID uint64) (*metapb.DestroyingStatus, error)
}

// startDestoryReplicaTaskAfterSplitted starts a task used to destory old replicas. We cannot directly delete the current replica,
// because once deleted, it may result in some Replica not receiving Split's Log.
// So we start a timer that periodically detects the replication of the Log, and once all the Replica are replicated,
// set the condition on `Prophet` that the Shard can be deleted (AppliedIndex >= SplitLogIndex).
func (pr *replica) startDestoryReplicaTaskAfterSplitted(splitRequestLogIndex uint64) {
	pr.startDestoryReplicaTask(splitRequestLogIndex, false, "splitted")
}

// startDestoryReplicaTask starts a task to destory all replicas of the shard after the specified Log applied.
func (pr *replica) startDestoryReplicaTask(targetIndex uint64, removeData bool, reason string) {
	task := pr.destoryTaskFactory.new(pr, targetIndex, removeData, reason)
	if err := pr.readStopper.RunNamedTask(context.Background(), reason, task.run); err != nil {
		pr.logger.Error("failed to start async task for destory replica",
			zap.Error(err))
	}
}

// destroyReplicaTaskFactory used to create destroyReplicaTask.
type destroyReplicaTaskFactory interface {
	new(pr *replica, targetIndex uint64, removeData bool, reason string) destroyReplicaTask
}

type defaultDestroyReplicaTaskFactory struct {
	actionHandler actionHandleFunc
	storage       destroyingStorage
	checkInterval time.Duration
}

func newDefaultDestroyReplicaTaskFactory(actionHandler actionHandleFunc, storage destroyingStorage, checkInterval time.Duration) destroyReplicaTaskFactory {
	return &defaultDestroyReplicaTaskFactory{
		actionHandler: actionHandler,
		storage:       storage,
		checkInterval: checkInterval,
	}
}

func (f *defaultDestroyReplicaTaskFactory) new(pr *replica, targetIndex uint64, removeData bool, reason string) destroyReplicaTask {
	return newDestroyReplicaTask(pr, targetIndex, f.actionHandler, f.storage, removeData, reason, f.checkInterval)
}

// destroyReplicaTask used to testing.
type destroyReplicaTask interface {
	run(ctx context.Context)
}

type defaultDestroyReplicaTask struct {
	pr            *replica
	targetIndex   uint64
	actionHandler actionHandleFunc
	client        destroyingStorage
	removeData    bool
	checkInterval time.Duration
	reason        string
}

func newDestroyReplicaTask(pr *replica, targetIndex uint64,
	actionHandler actionHandleFunc,
	client destroyingStorage,
	removeData bool,
	reason string,
	checkInterval time.Duration) destroyReplicaTask {
	return &defaultDestroyReplicaTask{
		pr:            pr,
		targetIndex:   targetIndex,
		actionHandler: actionHandler,
		removeData:    removeData,
		client:        client,
		reason:        reason,
		checkInterval: checkInterval,
	}
}

// destoryReplicaAfterLogApplied this task will be executed on all Replicas of a Shard. The task has 4 steps:
// 1. Periodically check all replicas for CommitIndex >= TargetIndex
// 2. Put the check result into storage
// 3. Periodically check all replicas for AppliedIndex >= TargetIndex
// 4. Perform destruction of replica
func (t *defaultDestroyReplicaTask) run(ctx context.Context) {
	pr := t.pr
	targetIndex := t.targetIndex
	actionHandler := t.actionHandler
	removeData := t.removeData
	client := t.client
	reason := t.reason
	checkInterval := t.checkInterval

	pr.logger.Info("start",
		destroyShardTaskField,
		zap.Uint64("target", targetIndex),
		zap.Bool("remove-data", removeData),
		log.ReasonField(reason))

	// we need check the destory record exist if restart.
	doCheckDestroyExists := true
	doCheckLog := false
	doCheckApply := false

	checkTimer := time.NewTimer(checkInterval)
	defer checkTimer.Stop()

	doSaveDestoryC := make(chan []uint64, 1)
	defer close(doSaveDestoryC)

	doRealDestoryC := make(chan struct{}, 1)
	defer close(doRealDestoryC)

	watiToRetry := func() {
		time.Sleep(time.Second)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-checkTimer.C:
			if doCheckDestroyExists {
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
					doCheckDestroyExists = false
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

			checkTimer.Reset(checkInterval)
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
				select {
				case doSaveDestoryC <- replicas: // retry
				default:
				}
				break
			}

			doCheckApply = true
			doCheckDestroyExists = false
			pr.logger.Debug("destory metadata saved",
				destroyShardTaskField)
		case <-doRealDestoryC: // step 4
			if _, err := client.ReportDestroyed(pr.shardID, pr.replicaID); err != nil {
				pr.logger.Error("failed to report destroying status, retry later",
					destroyShardTaskField,
					zap.Error(err))
				watiToRetry()
				select {
				case doRealDestoryC <- struct{}{}: // retry
				default:
				}
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
