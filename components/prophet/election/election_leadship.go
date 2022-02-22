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

package election

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/option"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/util/stop"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

var (
	mainLoopFiled  = zap.String("step", "main-loop")
	watchField     = zap.String("step", "watch-leader")
	campaignField  = zap.String("step", "campaign")
	keepaliveField = zap.String("step", "keepalive")
)

// Leadership is used to manage the leadership campaigning.
type Leadership struct {
	elector *elector
	tag     string
	// purpose is used to show what this election for
	purpose string
	// The lease which is used to get this leadership
	lease atomic.Value // stored as *lease
	// leaderKey and leaderValue are key-value pair in etcd
	leaderKey string
	nodeName  string
	nodeValue string

	ctx                                  context.Context
	isProphet                            bool
	becomeLeaderFunc, becomeFollowerFunc func(string) bool
	stopper                              *stop.Stopper
	logger                               *zap.Logger
}

func newLeadership(
	elector *elector,
	purpose, nodeName, nodeValue string,
	isProphet bool,
	becomeLeaderFunc, becomeFollowerFunc func(string) bool,
	logger *zap.Logger,
) *Leadership {
	tag := fmt.Sprintf("[%s/%s]", purpose, nodeName)
	return &Leadership{
		purpose:            purpose,
		elector:            elector,
		leaderKey:          getPurposePath(elector.options.leaderPath, purpose),
		nodeValue:          nodeValue,
		nodeName:           nodeName,
		isProphet:          isProphet,
		becomeLeaderFunc:   becomeLeaderFunc,
		becomeFollowerFunc: becomeFollowerFunc,
		tag:                tag,
		logger:             log.Adjust(logger).With(zap.String("tag", tag), zap.String("purpose", purpose)),
		stopper:            stop.NewStopper("leadership"),
	}
}

// GetLease gets the lease of leadership, only if leadership is valid,
// i.e the owner is a true leader, the lease is not nil.
func (ls *Leadership) GetLease() *LeaderLease {
	l := ls.lease.Load()
	if l == nil {
		return nil
	}
	return l.(*LeaderLease)
}

// Check returns whether the leadership is still available
func (ls *Leadership) Check() bool {
	lease := ls.GetLease()
	return ls != nil && lease != nil && !lease.IsExpired()
}

func (ls *Leadership) setLease(lease *LeaderLease) {
	ls.lease.Store(lease)
}

// ChangeLeaderTo change leader to new leader
func (ls *Leadership) ChangeLeaderTo(newLeader string) error {
	err := ls.addExpectLeader(newLeader)
	if err != nil {
		return err
	}

	return ls.GetLease().Close(ls.elector.client.Ctx())
}

// Stop stop the current leadship
func (ls *Leadership) Stop() error {
	ls.logger.Info("begin to stop")
	var err error
	lease := ls.GetLease()
	if lease != nil {
		err = lease.Close(ls.elector.client.Ctx())
	}
	ls.logger.Info("begin to stop stopper")
	ls.stopper.Stop()
	return err
}

// CurrentLeader returns the current leader
func (ls *Leadership) CurrentLeader() (string, int64, error) {
	value, rev, err := util.GetEtcdValue(ls.elector.client, ls.leaderKey)
	if err != nil {
		return "", 0, err
	}

	if len(value) == 0 {
		return "", 0, nil
	}

	return string(value), rev, nil
}

// DoIfLeader do if i'm leader
func (ls *Leadership) DoIfLeader(conditions []clientv3.Cmp, ops ...clientv3.Op) (bool, error) {
	if len(ops) == 0 {
		return true, nil
	}

	resp, err := util.LeaderTxn(ls.elector.client, ls.leaderKey, ls.nodeValue, conditions...).
		Then(ops...).
		Commit()
	if err != nil {
		return false, err
	}

	if !resp.Succeeded {
		return false, nil
	}

	return true, nil
}

// ElectionLoop start lead election
func (ls *Leadership) ElectionLoop() {
	ls.stopper.RunTask(context.Background(), ls.doElectionLoop)
}

func (ls *Leadership) doElectionLoop(ctx context.Context) {
	ls.ctx = ctx
	for {
		ls.logger.Info("ready to next loop", mainLoopFiled)

		select {
		case <-ctx.Done():
			ls.logger.Info("loop exit due to context done", mainLoopFiled)
			return
		default:
		}

		ls.logger.Info("ready to load current leader", mainLoopFiled)
		currentLeader, rev, err := ls.CurrentLeader()
		if err != nil {
			ls.logger.Error("fail to load current leader, retry later",
				mainLoopFiled,
				zap.Error(err),
			)
			time.Sleep(loopInterval)
			continue
		}

		ls.logger.Info("current leader loaded", mainLoopFiled,
			zap.String("leader", currentLeader),
		)

		if len(currentLeader) > 0 {
			if ls.nodeValue != "" && currentLeader == ls.nodeValue {
				// oh, we are already leader, we may meet something wrong
				// in previous campaignLeader. we can resign and campaign again.
				ls.logger.Warn("matched, resign and campaign again")

				if err := ls.resign(); err != nil {
					ls.logger.Warn("fail to resign leader", mainLoopFiled,
						zap.Error(err),
					)
					time.Sleep(loopInterval)
					continue
				}
			} else {
				ls.logger.Info("start to watch current leader", mainLoopFiled,
					zap.String("leader", currentLeader),
				)

				ls.becomeFollowerFunc(currentLeader)
				ls.watch(rev)
				ls.becomeFollowerFunc("")

				ls.logger.Info("current leader out", mainLoopFiled,
					zap.String("leader", currentLeader),
				)
			}
		}

		if ls.isProphet {
			ls.logger.Info("start checkExpectLeader", mainLoopFiled)
			if err := ls.checkExpectLeader(); err != nil {
				ls.logger.Error("fail to check expect leader", mainLoopFiled,
					zap.Error(err),
				)
				time.Sleep(200 * time.Millisecond)
				continue
			}

			ls.logger.Info("end checkExpectLeader, and start campaign", mainLoopFiled)
			if err := ls.campaign(); err != nil {
				ls.logger.Error("fail to campaign leader", mainLoopFiled,
					zap.Error(err),
				)
				time.Sleep(time.Second * time.Duration(ls.elector.options.leaseSec))
				continue
			}

			ls.logger.Info("end campaign", mainLoopFiled)
		}

		time.Sleep(loopInterval)
	}
}

func (ls *Leadership) campaign() error {
	ls.logger.Info("start campaign",
		campaignField)

	l := newLease(ls.tag, ls.purpose, ls.elector.client, ls.logger)
	ls.setLease(l)

	err := l.grant(ls.ctx, ls.elector.options.leaseSec)
	if err != nil {
		return err
	}

	ls.logger.Info("grant lease ok",
		campaignField)

	// The leader key must not exist, so the CreateRevision is 0.
	resp, err := util.Txn(ls.elector.client).
		If(clientv3.Compare(clientv3.CreateRevision(ls.leaderKey), "=", 0)).
		Then(clientv3.OpPut(ls.leaderKey,
			ls.nodeValue,
			clientv3.WithLease(l.getLeaseID()))).
		Commit()
	if err != nil {
		ls.GetLease().Close(ls.elector.client.Ctx())
		return err
	}
	if !resp.Succeeded {
		ls.GetLease().Close(ls.elector.client.Ctx())
		return errors.New("campaign leader failed, other server may campaign ok")
	}

	ls.logger.Info("start keep lease alive",
		campaignField)

	// Start keepalive
	ctx, cancel := context.WithCancel(ls.ctx)
	defer cancel()
	ls.stopper.RunTask(ctx, l.keepAlive)

	var lock *concurrency.Mutex
	if ls.elector.options.lockIfBecomeLeader {
		session, err := concurrency.NewSession(ls.elector.client,
			concurrency.WithLease(l.getLeaseID()))
		if err != nil {
			ls.logger.Error("fail to create etcd concurrency lock session",
				keepaliveField,
				zap.Error(err))
			return err
		}
		defer session.Close()

		// distributed lock to make sure last leader complete async tasks
		lock = concurrency.NewMutex(session, getPurposePath(ls.elector.options.lockPath, ls.purpose))
		for {
			ctx, cancel := context.WithTimeout(ls.ctx, time.Second*5)
			err = lock.Lock(ctx)
			if err != nil {
				cancel()
				ls.logger.Error("fail to get lock, retry later",
					keepaliveField,
					zap.Error(err))
				continue
			}

			cancel()
			break
		}

		ls.logger.Info("get lock",
			keepaliveField)
	}

	if !ls.becomeLeaderFunc(ls.nodeValue) {
		ls.logger.Info("become leader func return false",
			keepaliveField)
		return nil
	}

	defer func() {
		ls.becomeFollowerFunc("")
		if lock != nil {
			lock.Unlock(ls.ctx)
			ls.logger.Info("lock released",
				keepaliveField)
		}
	}()

	currentEtcdLeader := uint64(0)
	if ls.elector.options.etcd != nil {
		currentEtcdLeader = ls.elector.options.etcd.Server.Lead()
	}

	leaderTicker := time.NewTicker(leaderTickInterval)
	defer leaderTicker.Stop()

	for {
		select {
		case <-leaderTicker.C:
			if l.IsExpired() {
				ls.logger.Info("exit due to lease expired",
					keepaliveField)
				return nil
			}

			if ls.elector.options.etcd != nil {
				if ls.elector.options.etcd.Server.Lead() != currentEtcdLeader {
					ls.logger.Info("exit due to etcd leader changed",
						keepaliveField)
					return nil
				}
			}
		case <-ls.elector.client.Ctx().Done():
			ls.logger.Info("exit due to client context done",
				keepaliveField)
			return errors.New("etcd client closed")
		case <-ls.ctx.Done():
			ls.logger.Info("exit due to context done",
				keepaliveField)
			return nil
		}
	}
}

func (ls *Leadership) resign() error {
	resp, err := util.LeaderTxn(ls.elector.client, ls.leaderKey, ls.nodeValue).
		Then(clientv3.OpDelete(ls.leaderKey)).
		Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errors.New("resign leader failed, we are not leader already")
	}

	return nil
}

func (ls *Leadership) watch(revision int64) {
	watcher := clientv3.NewWatcher(ls.elector.client)
	defer watcher.Close()

	// The revision is the revision of last modification on this key.
	// If the revision is compacted, will meet required revision has been compacted error.
	// In this case, use the compact revision to re-watch the key.
	for {
		rch := watcher.Watch(ls.ctx, ls.leaderKey, clientv3.WithRev(revision))
		for wresp := range rch {
			// meet compacted error, use the compact revision.
			if wresp.CompactRevision != 0 {
				ls.logger.Warn("required revision has been compacted, use the compact revision",
					watchField,
					zap.Int64("revision", revision),
					zap.Int64("compact-revision", wresp.CompactRevision))
				revision = wresp.CompactRevision
				break
			}

			if wresp.Canceled {
				ls.logger.Info("exit due to watcher canceled",
					watchField)
				return
			}

			for _, ev := range wresp.Events {
				if ev.Type == mvccpb.DELETE {
					ls.logger.Info("leader deleted",
						watchField)
					return
				} else if ev.Type == mvccpb.PUT {
					if ev.PrevKv != nil {
						ls.logger.Info("leader changed",
							watchField,
							zap.String("from", string(ev.PrevKv.Value)),
							zap.String("to", string(ev.Kv.Value)))
					} else {
						ls.logger.Info("leader updated",
							watchField,
							zap.String("value", string(ev.Kv.Value)))
					}
				}
			}
		}

		select {
		case <-ls.ctx.Done():
			ls.logger.Info("exit due to context done",
				watchField)
			return
		default:
		}
	}
}

func (ls *Leadership) checkExpectLeader() error {
	value, _, err := util.GetEtcdValue(ls.elector.client, getPurposeExpectPath(ls.elector.options.leaderPath, ls.purpose))
	if err != nil {
		return err
	}

	ls.logger.Info("expect leader loaded",
		campaignField,
		zap.String("expect-leader", string(value)))

	if len(value) == 0 {
		return nil
	}

	if string(value) != ls.nodeValue {
		return fmt.Errorf("expect leader is %+v", string(value))
	}

	return nil
}

func (ls *Leadership) addExpectLeader(newLeader string) error {
	ctx, cancel := context.WithTimeout(ls.elector.client.Ctx(), option.DefaultRequestTimeout)
	leaseResp, err := ls.elector.lease.Grant(ctx, ls.elector.options.leaseSec)
	cancel()

	if err != nil {
		return err
	}

	resp, err := util.LeaderTxn(ls.elector.client, ls.leaderKey, ls.nodeValue).
		Then(clientv3.OpPut(ls.leaderKey,
			string(newLeader),
			clientv3.WithLease(leaseResp.ID))).
		Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return fmt.Errorf("not leader")
	}

	ls.logger.Info("expect leader added",
		zap.String("expect-leader", newLeader),
		zap.Int64("ttl-in-seconds", ls.elector.options.leaseSec))
	return nil
}
