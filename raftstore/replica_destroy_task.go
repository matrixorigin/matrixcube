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

// startDestroyReplicaTaskAfterSplitted starts a task used to destroy old replicas. We cannot directly delete the current replica,
// because once deleted, it may result in some Replica not receiving Split's Log.
// So we start a timer that periodically detects the replication of the Log, and once all the Replica are replicated,
// set the condition on `Prophet` that the Shard can be deleted (AppliedIndex >= SplitLogIndex).
func (pr *replica) startDestroyReplicaTaskAfterSplitted(splitRequestLogIndex uint64) {
	pr.startDestroyReplicaTask(splitRequestLogIndex, false, "splitted")
}

// startDestroyReplicaTask starts a task to destroy all replicas of the shard after the specified Log applied.
func (pr *replica) startDestroyReplicaTask(targetIndex uint64, removeData bool, reason string) {
	pr.store.addUnavailableShard(pr.shardID)

	pr.destroyTaskMu.Lock()
	defer pr.destroyTaskMu.Unlock()

	if pr.destroyTaskMu.hasTask {
		pr.logger.Error("async task for destroy replica already started",
			log.ReasonField(reason),
			zap.String("last-reason", pr.destroyTaskMu.reason),
			destroyShardTaskField)
		return
	}

	pr.destroyTaskMu.hasTask = true
	pr.destroyTaskMu.reason = reason
	task := pr.destroyTaskFactory.new(pr, targetIndex, removeData, reason)
	if err := pr.readStopper.RunNamedTask(context.Background(), reason, task.run); err != nil {
		pr.logger.Error("failed to start async task for destroy replica",
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
	ds            destroyingStorage
	removeData    bool
	reason        string

	doCheckDestroyExists bool
	doCheckLog           bool
	doCheckApply         bool
	checkInterval        time.Duration
	checkTimer           *time.Timer
	doSaveDestroyC       chan []uint64
	doRealDestroyC       chan struct{}
}

func newDestroyReplicaTask(pr *replica, targetIndex uint64,
	actionHandler actionHandleFunc,
	ds destroyingStorage,
	removeData bool,
	reason string,
	checkInterval time.Duration) destroyReplicaTask {
	return &defaultDestroyReplicaTask{
		pr:                   pr,
		targetIndex:          targetIndex,
		actionHandler:        actionHandler,
		removeData:           removeData,
		ds:                   ds,
		reason:               reason,
		doCheckDestroyExists: true,
		doCheckLog:           false,
		doCheckApply:         false,
		doSaveDestroyC:       make(chan []uint64, 1),
		doRealDestroyC:       make(chan struct{}, 1),
		checkInterval:        checkInterval,
		checkTimer:           time.NewTimer(checkInterval),
	}
}

// destroyReplicaAfterLogApplied this task will be executed on all Replicas of a Shard. The task has 4 steps:
// 1. Periodically check all replicas for CommitIndex >= TargetIndex
// 2. Put the check result into storage
// 3. Periodically check all replicas for AppliedIndex >= TargetIndex
// 4. Perform destruction of replica
func (t *defaultDestroyReplicaTask) run(ctx context.Context) {
	t.pr.logger.Info("start",
		destroyShardTaskField,
		zap.Uint64("target", t.targetIndex),
		zap.Bool("remove-data", t.removeData),
		log.ReasonField(t.reason))

	defer t.close()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.checkTimer.C:
			// we need check the destroy record exist if restart.
			t.maybeCheckDestroyExists()
			// step 1
			t.maybeCheckLog()
			// start 3
			t.maybeCheckTargetLogApplied()

			t.checkTimer.Reset(t.checkInterval)
		case replicas := <-t.doSaveDestroyC: // step 2
			// leader path, only leader node can reach here.
			// Reaching here means that all Replicas of the Shard have completed the log replication of
			// the TargetIndex and have been Committed.
			// Only if the Destroyed information corresponding to the Shard is set in storage, all replicas
			// can be safely deleted.
			t.createDestroying(replicas)
		case <-t.doRealDestroyC: // step 4
			if t.performDestroy() {
				return
			}
		}
	}
}

func (t *defaultDestroyReplicaTask) close() {
	t.checkTimer.Stop()
	close(t.doSaveDestroyC)
	close(t.doRealDestroyC)
}

func (t *defaultDestroyReplicaTask) maybeCheckDestroyExists() {
	if t.doCheckDestroyExists {
		status, err := t.ds.GetDestroying(t.pr.shardID)
		if err != nil {
			t.pr.logger.Error("failed to get shard destroy metadata, retry later",
				destroyShardTaskField,
				zap.Error(err))
			t.watiToRetry()
			return
		}

		if status == nil {
			// no destroying record exists means we need check log committed info, start step 1
			t.doCheckLog = true
			t.pr.logger.Debug("begin to check log committed",
				destroyShardTaskField)
		} else {
			// update read index
			t.targetIndex = status.Index
			t.removeData = status.RemoveData

			// means all replica committed at targetIndex, start step 3
			t.doCheckApply = true
			t.doCheckDestroyExists = false
			t.pr.logger.Debug("begin to check log applied",
				zap.Uint64("target-index", t.targetIndex),
				zap.Bool("remove-data", t.removeData),
				destroyShardTaskField)
		}
	}
}

func (t *defaultDestroyReplicaTask) maybeCheckLog() {
	if t.doCheckLog {
		t.actionHandler(action{
			actionType:  checkLogCommittedAction,
			targetIndex: t.targetIndex,
			actionCallback: func(replicas interface{}) {
				t.doSaveDestroyC <- replicas.([]uint64)
				t.pr.logger.Debug("log committed on all replicas",
					destroyShardTaskField)
			},
		})
	}
}

func (t *defaultDestroyReplicaTask) createDestroying(replicas []uint64) {
	t.doCheckLog = false

	_, err := t.ds.CreateDestroying(t.pr.shardID, t.targetIndex, t.removeData, replicas)
	if err != nil {
		t.pr.logger.Error("failed to save replica destroy metadata, retry later",
			destroyShardTaskField,
			zap.Error(err))
		t.watiToRetry()
		select {
		case t.doSaveDestroyC <- replicas: // retry
		default:
		}
		return
	}

	t.doCheckApply = true
	t.doCheckDestroyExists = false
	t.pr.logger.Debug("destroy metadata saved",
		destroyShardTaskField)
}

func (t *defaultDestroyReplicaTask) performDestroy() bool {
	if _, err := t.ds.ReportDestroyed(t.pr.shardID, t.pr.replicaID); err != nil {
		t.pr.logger.Error("failed to report destroying status, retry later",
			destroyShardTaskField,
			zap.Error(err))
		t.watiToRetry()
		select {
		case t.doRealDestroyC <- struct{}{}: // retry
		default:
		}
		return false
	}

	t.pr.store.destroyReplica(t.pr.shardID, true, t.removeData, t.reason)
	t.pr.logger.Debug("completed",
		destroyShardTaskField)
	return true
}

func (t *defaultDestroyReplicaTask) maybeCheckTargetLogApplied() {
	if t.doCheckApply {
		t.actionHandler(action{
			actionType:  checkLogAppliedAction,
			targetIndex: t.targetIndex,
			actionCallback: func(i interface{}) {
				t.doRealDestroyC <- struct{}{}
				t.pr.logger.Debug("log applied",
					destroyShardTaskField)
			},
		})
	}
}

func (t *defaultDestroyReplicaTask) watiToRetry() {
	time.Sleep(time.Second)
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
