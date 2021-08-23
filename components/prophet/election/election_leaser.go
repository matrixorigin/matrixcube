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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/option"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const revokeLeaseTimeout = time.Second

// LeaderLease is used for renewing leadership.
type LeaderLease struct {
	tag          string
	purpose      string
	id           clientv3.LeaseID
	lease        clientv3.Lease
	leaseTimeout time.Duration
	expireTime   atomic.Value
}

func newLease(tag, purpose string, lease clientv3.Lease) *LeaderLease {
	return &LeaderLease{
		tag:     tag,
		purpose: purpose,
		lease:   lease,
	}
}

// Grant uses `lease.Grant` to initialize the lease and expireTime.
func (l *LeaderLease) grant(ctx context.Context, leaseTimeout int64) error {
	start := time.Now()

	c, cancel := context.WithTimeout(ctx, option.DefaultRequestTimeout)
	leaseResp, err := l.lease.Grant(c, leaseTimeout)
	cancel()

	if err != nil {
		return err
	}
	if cost := time.Since(start); cost > option.DefaultSlowRequestTime {
		util.GetLogger().Warningf("%s/lease: lessor grants too slow, cost=<%s>",
			l.tag, cost)
	}

	l.id = leaseResp.ID
	l.leaseTimeout = time.Duration(leaseTimeout) * time.Second
	l.expireTime.Store(start.Add(time.Duration(leaseResp.TTL) * time.Second))
	return nil
}

// Close releases the lease.
func (l *LeaderLease) Close(pctx context.Context) error {
	// Reset expire time.
	l.expireTime.Store(time.Time{})

	if l.lease != nil {
		// Try to revoke lease to make subsequent elections faster.
		ctx, cancel := context.WithTimeout(pctx, revokeLeaseTimeout)
		defer cancel()

		_, err := l.lease.Revoke(ctx, l.id)
		if err != nil {
			util.GetLogger().Infof("%s/lease: close failed with %+v",
				l.tag,
				err)
			return err
		}
	}

	util.GetLogger().Infof("%s/lease: closed",
		l.tag)
	return nil
}

// IsExpired checks if the lease is expired. If it returns true, current
// node should step down and try to re-elect again.
func (l *LeaderLease) IsExpired() bool {
	return time.Now().After(l.expireTime.Load().(time.Time))
}

func (l *LeaderLease) keepAlive(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	timeCh := l.keepAliveWorker(ctx, l.leaseTimeout/3)

	var maxExpire time.Time
	for {
		select {
		case t := <-timeCh:
			if t.After(maxExpire) {
				maxExpire = t
				l.expireTime.Store(t)
			}
		case <-time.After(l.leaseTimeout):
			util.GetLogger().Infof("%s/lease: exit with timeout",
				l.tag)
			return
		case <-ctx.Done():
			util.GetLogger().Infof("%s/lease: exit with context done",
				l.tag)
			return
		}
	}
}

func (l *LeaderLease) keepAliveWorker(ctx context.Context, interval time.Duration) <-chan time.Time {
	ch := make(chan time.Time)

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			go func() {
				start := time.Now()
				ctx1, cancel := context.WithTimeout(ctx, l.leaseTimeout)
				defer cancel()
				res, err := l.lease.KeepAliveOnce(ctx1, l.id)
				if err != nil {
					util.GetLogger().Errorf("%s/lease: keep lease failed with %+v, retry later",
						l.tag,
						err)
					return
				}
				if res.TTL > 0 {
					expire := start.Add(time.Duration(res.TTL) * time.Second)
					select {
					case ch <- expire:
					case <-ctx1.Done():
					}
				}
			}()

			select {
			case <-ctx.Done():
				util.GetLogger().Errorf("%s/lease: keep lease exit",
					l.tag)
				return
			case <-ticker.C:
			}
		}
	}()

	return ch
}
