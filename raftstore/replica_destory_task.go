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
	"fmt"
	"time"

	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet"
	trackerPkg "go.etcd.io/etcd/raft/v3/tracker"
	"go.uber.org/zap"
)

var (
	checkerInterval = time.Second * 5

	taskField = zap.String("task", "destory-replica-task")
)

type actionHandleFunc func(action)

type destoryMetadataStorage interface {
	put(shardID, logIndex uint64) error
	get(shardID uint64) (uint64, error)
}

type prophetBasedStorage struct {
	pd prophet.Prophet
}

func newProphetBasedStorage(pd prophet.Prophet) destoryMetadataStorage {
	return &prophetBasedStorage{
		pd: pd,
	}
}

func (s *prophetBasedStorage) put(shardID, logIndex uint64) error {
	return s.pd.GetStorage().KV().SaveWithoutLeader(s.getKey(shardID), format.Uint64ToString(logIndex))
}

func (s *prophetBasedStorage) get(shardID uint64) (uint64, error) {
	v, err := s.pd.GetStorage().KV().Load(s.getKey(shardID))
	if err != nil {
		return 0, err
	}
	if len(v) == 0 {
		return 0, nil
	}
	return format.MustParseStringUint64(v), nil
}

func (s *prophetBasedStorage) getKey(id uint64) string {
	return fmt.Sprintf("/cube/destory/%d", id)
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
		pr.destoryReplicaAfterLogApplied(ctx, targetIndex, pr.addAction, pr.store.destoryMetadataStorage, removeData, reason)
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
	storage destoryMetadataStorage,
	removeData bool,
	reason string) {

	pr.logger.Info("begin to destory all replicas",
		taskField,
		zap.Uint64("index", targetIndex),
		zap.Bool("remove-data", removeData),
		log.ReasonField(reason))

	// we need check the destory record exist if restart.
	doCheckDestoryExists := true
	doCheckLog := false
	doCheckApply := false

	checkTimer := time.NewTimer(checkerInterval)
	defer checkTimer.Stop()

	doSaveDestoryC := make(chan struct{})
	defer close(doSaveDestoryC)

	doRealDestoryC := make(chan struct{})
	defer close(doRealDestoryC)

	for {
		select {
		case <-ctx.Done():
			return
		case <-checkTimer.C:
			if doCheckDestoryExists {
				v, err := storage.get(pr.shardID)
				if err != nil {
					pr.logger.Error("failed to get replica destory metadata, retry later",
						zap.Error(err))
					break
				}

				if v == 0 {
					// no destory record exists means we need check log committed info, start step 1
					doCheckLog = true
					pr.logger.Debug("start to check log committed for all replicas", taskField)
				} else {
					// means all replica committed at targetIndex, start step 3
					doCheckApply = true
					doCheckDestoryExists = false
					pr.logger.Debug("start to check log applied for current replica", taskField)
				}
			}

			// step 1
			if doCheckLog {
				actionHandler(action{
					actionType:  checkLogCommittedAction,
					targetIndex: targetIndex,
					actionCallback: func(i interface{}) {
						doSaveDestoryC <- struct{}{}
						pr.logger.Debug("target log committed for all replicas", taskField)
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
						pr.logger.Debug("target log applied for current replica", taskField)
					},
				})
			}

			checkTimer.Reset(checkerInterval)
		case <-doSaveDestoryC: // step 2
			// leader path, only leader node can reach here.
			doCheckLog = false

			// Reaching here means that all Replicas of the Shard have completed the log replication of
			// the TargetIndex and have been Committed.
			// Only if the Destoryed information corresponding to the Shard is set in storage, all replicas
			// can be safely deleted.
			err := storage.put(pr.shardID, targetIndex)
			if err != nil {
				pr.logger.Error("failed to save replica destory metadata, retry later",
					taskField,
					zap.Error(err))
				doSaveDestoryC <- struct{}{} // retry
				break
			}

			doCheckApply = true
			doCheckDestoryExists = false
			pr.logger.Debug("destory metadata saved, start to check target log applied for current replica",
				taskField)
		case <-doRealDestoryC: // step 4
			// current shard not used, keep data.
			pr.store.destroyReplica(pr.shardID, true, removeData, reason)
			return
		}
	}
}

func (pr *replica) doCheckLogCommitted(act action) {
	if !pr.isLeader() {
		pr.logger.Debug("skip check log committed",
			taskField,
			log.ReasonField("not leader"))
		return
	}

	status := pr.rn.Status()
	if len(status.Progress) == 0 {
		pr.logger.Debug("skip check log committed",
			taskField,
			log.ReasonField("no progress"))
		return
	}

	for id, p := range status.Progress {
		if p.State == trackerPkg.StateSnapshot {
			pr.logger.Debug("check log committed failed",
				taskField,
				log.ReasonField("replica in snapshot"))
			return
		}

		if pr.committedIndexes[id] < act.targetIndex {
			pr.logger.Debug("check log committed failed",
				taskField,
				log.ReasonField("committed too small"),
				log.ReplicaIDField(id),
				zap.Uint64("current-committed", pr.committedIndexes[id]),
				zap.Uint64("target-committed", act.targetIndex))
			return
		}
	}

	// All replica committedIndex >= targetIndex.
	// The shard state in the state machine is Destroying, and Prophet does not add new replica,
	// the only thing it can do is remove the offline replica.
	// So eventually it must be executed here, means that all replicas commit the target raft log.
	act.actionCallback(nil)
}

func (pr *replica) doCheckLogApplied(act action) {
	if pr.appliedIndex >= act.targetIndex {
		act.actionCallback(nil)
	}
}
