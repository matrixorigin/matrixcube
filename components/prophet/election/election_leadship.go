package election

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/option"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc/mvccpb"
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

	ctx                          context.Context
	allowCampaign                bool
	becomeLeader, becomeFollower func(string) bool
}

func newLeadership(elector *elector, purpose, nodeName, nodeValue string, allowCampaign bool, becomeLeader, becomeFollower func(string) bool) *Leadership {
	return &Leadership{
		tag:            fmt.Sprintf("[%s/%s]", purpose, nodeName),
		purpose:        purpose,
		elector:        elector,
		leaderKey:      getPurposePath(elector.options.leaderPath, purpose),
		nodeValue:      nodeValue,
		nodeName:       nodeName,
		allowCampaign:  allowCampaign,
		becomeLeader:   becomeLeader,
		becomeFollower: becomeFollower,
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
	lease := ls.GetLease()
	if lease != nil {
		return lease.Close(ls.elector.client.Ctx())
	}

	return nil
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
func (ls *Leadership) ElectionLoop(ctx context.Context) {
	ls.ctx = ctx

	for {
		select {
		case <-ctx.Done():
			util.GetLogger().Infof("%s/loop: exit", ls.tag)
			return
		default:
			currentLeader, rev, err := ls.CurrentLeader()
			if err != nil {
				util.GetLogger().Errorf("%s/loop: load current leader failed with %+v, retry later",
					ls.tag,
					err)
				time.Sleep(loopInterval)
				continue
			}
			util.GetLogger().Infof("%s/loop: current leader is [%s]",
				ls.tag,
				currentLeader)

			if len(currentLeader) > 0 {
				if ls.nodeValue != "" && currentLeader == ls.nodeValue {
					// oh, we are already leader, we may meet something wrong
					// in previous campaignLeader. we can resign and campaign again.
					util.GetLogger().Warningf("%s/loop: matched, resign and campaign again",
						ls.tag)
					if err = ls.resign(); err != nil {
						util.GetLogger().Warningf("%s: resign leader %+v failed with %+v",
							ls.tag,
							currentLeader,
							err)
						time.Sleep(loopInterval)
						continue
					}
				} else {
					util.GetLogger().Infof("%s/loop: start watch leader %s",
						ls.tag,
						currentLeader)
					ls.becomeFollower(currentLeader)
					ls.watch(rev)
					ls.becomeFollower("")
					util.GetLogger().Infof("%s/loop: leader %s was out",
						ls.tag,
						currentLeader)
				}
			}

			if ls.allowCampaign {
				// check expect leader exists
				err := ls.checkExpectLeader()
				if err != nil {
					util.GetLogger().Errorf("%s/loop: check expect leader failed with %+v",
						ls.tag,
						err)
					time.Sleep(200 * time.Millisecond)
					continue
				}

				if err = ls.campaign(); err != nil {
					util.GetLogger().Errorf("%s/loop: campaign leader failed with %+v",
						ls.tag,
						err)
					time.Sleep(time.Second * time.Duration(ls.elector.options.leaseSec))
					continue
				}
			}

			time.Sleep(loopInterval)
		}
	}
}

func (ls *Leadership) campaign() error {
	util.GetLogger().Infof("%s/campaign: start",
		ls.tag)

	l := newLease(ls.tag, ls.purpose, ls.elector.client)
	ls.setLease(l)

	err := l.grant(ls.ctx, ls.elector.options.leaseSec)
	if err != nil {
		return err
	}

	util.GetLogger().Infof("%s/campaign: grant lease ok",
		ls.tag)

	// The leader key must not exist, so the CreateRevision is 0.
	resp, err := util.Txn(ls.elector.client).
		If(clientv3.Compare(clientv3.CreateRevision(ls.leaderKey), "=", 0)).
		Then(clientv3.OpPut(ls.leaderKey,
			ls.nodeValue,
			clientv3.WithLease(clientv3.LeaseID(l.id)))).
		Commit()
	if err != nil {
		ls.GetLease().Close(ls.elector.client.Ctx())
		return err
	}
	if !resp.Succeeded {
		ls.GetLease().Close(ls.elector.client.Ctx())
		return errors.New("campaign leader failed, other server may campaign ok")
	}

	util.GetLogger().Infof("%s/campaign: completed, start keepalive",
		ls.tag)

	// Start keepalive
	ctx, cancel := context.WithCancel(ls.ctx)
	defer cancel()
	go l.keepAlive(ctx)

	var lock *concurrency.Mutex
	if ls.elector.options.lockIfBecomeLeader {
		session, err := concurrency.NewSession(ls.elector.client,
			concurrency.WithLease(clientv3.LeaseID(l.id)))
		if err != nil {
			util.GetLogger().Errorf("create etcd concurrency lock session failed with %+v", err)
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
				util.GetLogger().Errorf("%s/keepalive: get lock failed with %+v, retry later",
					ls.tag,
					err)
				continue
			}

			cancel()
			break
		}

		util.GetLogger().Infof("%s/keepalive: get lock", ls.tag)
	}

	// if ls.elector.options.tso != nil {
	// 	if err := ls.elector.options.tso.SyncTimestamp(l); err != nil {
	// 		util.GetLogger().Error("sync timestamp failed with %+v", err)
	// 		return err
	// 	}

	// 	defer ls.elector.options.tso.ResetTimestamp()
	// }

	if !ls.becomeLeader(ls.nodeValue) {
		util.GetLogger().Infof("%s/keepalive: become leader func return false",
			ls.tag)
		return nil
	}

	defer func() {
		ls.becomeFollower("")
		if lock != nil {
			lock.Unlock(ls.ctx)
			util.GetLogger().Infof("%s/keepalive: lock released",
				ls.tag)
		}
	}()

	// tsTicker := time.NewTicker(UpdateTimestampStep)
	// defer tsTicker.Stop()
	leaderTicker := time.NewTicker(leaderTickInterval)
	defer leaderTicker.Stop()

	for {
		select {
		case <-leaderTicker.C:
			if l.IsExpired() {
				util.GetLogger().Infof("%s/keepalive: exit with lease expired",
					ls.tag)
				return nil
			}

			if ls.elector.options.etcd != nil {
				etcdLeader := ls.elector.options.etcd.Server.Lead()
				if etcdLeader != uint64(ls.elector.options.etcd.Server.ID()) {
					util.GetLogger().Infof("%s/keepalive: exit with etcd leader changed",
						ls.tag)
					return nil
				}
			}
		// case <-tsTicker.C:
		// 	if ls.elector.options.tso != nil {
		// 		if err = ls.elector.options.tso.UpdateTimestamp(); err != nil {
		// 			util.GetLogger().Errorf("update timestamp failed with %+v", err)
		// 			return err
		// 		}
		// 	}
		case <-ls.elector.client.Ctx().Done():
			util.GetLogger().Infof("%s/keepalive: exit with client context done",
				ls.tag)
			return errors.New("etcd client closed")
		case <-ctx.Done():
			util.GetLogger().Infof("%s/keepalive: exit with context done",
				ls.tag)
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
				util.GetLogger().Warningf("%s/watch: required revision %d has been compacted, use the compact revision %d",
					ls.tag,
					revision,
					wresp.CompactRevision)
				revision = wresp.CompactRevision
				break
			}

			if wresp.Canceled {
				util.GetLogger().Infof("%s/watch: exit with watcher failed",
					ls.tag)
				return
			}

			for _, ev := range wresp.Events {
				if ev.Type == mvccpb.DELETE {
					util.GetLogger().Infof("%s/watch: leader deleted",
						ls.tag)
					return
				} else if ev.Type == mvccpb.PUT {
					if ev.PrevKv != nil {
						util.GetLogger().Infof("%s/watch: leader updated from %s to %s",
							ls.tag,
							string(ev.PrevKv.Value),
							string(ev.Kv.Value))
					} else {
						util.GetLogger().Infof("%s/watch: leader updated to %s",
							ls.tag,
							string(ev.Kv.Value))
					}
				}
			}
		}

		select {
		case <-ls.ctx.Done():
			util.GetLogger().Infof("%s/watch: exit with context done",
				ls.tag)
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

	util.GetLogger().Infof("%s/campaign: load expect leader: [%s]",
		ls.tag,
		string(value))

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
	leaseResp, err := ls.elector.lessor.Grant(ctx, ls.elector.options.leaseSec)
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

	util.GetLogger().Infof("%s: expect leader %s added, with %d secs ttl",
		ls.tag,
		newLeader,
		ls.elector.options.leaseSec)
	return nil
}
