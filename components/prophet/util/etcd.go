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

package util

import (
	"context"
	"fmt"
	"net/http"

	"github.com/matrixorigin/matrixcube/components/prophet/option"
	"go.etcd.io/etcd/client/pkg/v3/types"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/etcdserver"
)

// GetPrefixRangeEnd get prefix range end
var GetPrefixRangeEnd = clientv3.GetPrefixRangeEnd

// CheckClusterID checks etcd cluster ID, returns an error if mismatch.
// This function will never block even quorum is not satisfied.
func CheckClusterID(localClusterID types.ID, um types.URLsMap) error {
	if len(um) == 0 {
		return nil
	}

	var peerURLs []string
	for _, urls := range um {
		peerURLs = append(peerURLs, urls.StringSlice()...)
	}

	for _, u := range peerURLs {
		trp := &http.Transport{}
		remoteCluster, gerr := etcdserver.GetClusterFromRemotePeers(nil, []string{u}, trp)
		trp.CloseIdleConnections()
		if gerr != nil {
			continue
		}

		remoteClusterID := remoteCluster.ID()
		if remoteClusterID != localClusterID {
			return fmt.Errorf("etcd cluster ID mismatch, expect %d, got %d", localClusterID, remoteClusterID)
		}
	}
	return nil
}

// AddEtcdMember add a member to etcd
func AddEtcdMember(client *clientv3.Client, urls []string) (*clientv3.MemberAddResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), option.DefaultRequestTimeout)
	addResp, err := client.MemberAdd(ctx, urls)
	cancel()
	return addResp, err
}

// ListEtcdMembers returns etcd members
func ListEtcdMembers(client *clientv3.Client) (*clientv3.MemberListResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), option.DefaultRequestTimeout)
	listResp, err := client.MemberList(ctx)
	cancel()
	return listResp, err
}

// GetEtcdValue returns value from etcd storage
func GetEtcdValue(client *clientv3.Client, key string, opts ...clientv3.OpOption) ([]byte, int64, error) {
	resp, err := GetEtcdResp(client, key, opts...)
	if err != nil {
		return nil, 0, err
	}

	if resp.Count == 0 {
		return nil, 0, nil
	} else if resp.Count > 1 {
		return nil, 0, fmt.Errorf("invalid get value resp %v, must only one", resp.Kvs)
	}

	return resp.Kvs[0].Value, resp.Kvs[0].ModRevision, nil
}

// GetEtcdResp returns etcd resp
func GetEtcdResp(client *clientv3.Client, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), option.DefaultRequestTimeout)
	defer cancel()

	resp, err := clientv3.NewKV(client).Get(ctx, key, opts...)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

// PutEtcdWithTTL put (key, value) into etcd with a ttl of ttlSeconds
func PutEtcdWithTTL(ctx context.Context, c *clientv3.Client, key string, value string, ttlSeconds int64) (*clientv3.PutResponse, error) {
	kv := clientv3.NewKV(c)
	grantResp, err := c.Grant(ctx, ttlSeconds)
	if err != nil {
		return nil, err
	}
	return kv.Put(ctx, key, value, clientv3.WithLease(grantResp.ID))
}

// GetCurrentClusterMembers returns etcd current members
func GetCurrentClusterMembers(client *clientv3.Client) (*clientv3.MemberListResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), option.DefaultRequestTimeout)
	members, err := client.MemberList(ctx)
	cancel()

	return members, err
}

// LeaderTxn returns leader txn
func LeaderTxn(client *clientv3.Client, leaderKey, leaderValue string, cs ...clientv3.Cmp) clientv3.Txn {
	return newSlowLogTxn(client).If(append(cs, leaderCmp(leaderKey, leaderValue))...)
}

// Txn returns etcd txn
func Txn(client *clientv3.Client) clientv3.Txn {
	return newSlowLogTxn(client)
}

func leaderCmp(leaderKey, leaderValue string) clientv3.Cmp {
	return clientv3.Compare(clientv3.Value(leaderKey), "=", leaderValue)
}

// slowLogTxn wraps etcd transaction and log slow one.
type slowLogTxn struct {
	clientv3.Txn
	cancel context.CancelFunc
}

func newSlowLogTxn(client *clientv3.Client) clientv3.Txn {
	ctx, cancel := context.WithTimeout(client.Ctx(), option.DefaultRequestTimeout)
	return &slowLogTxn{
		Txn:    client.Txn(ctx),
		cancel: cancel,
	}
}

func (t *slowLogTxn) If(cs ...clientv3.Cmp) clientv3.Txn {
	return &slowLogTxn{
		Txn:    t.Txn.If(cs...),
		cancel: t.cancel,
	}
}

func (t *slowLogTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	return &slowLogTxn{
		Txn:    t.Txn.Then(ops...),
		cancel: t.cancel,
	}
}

// Commit implements Txn Commit interface.
func (t *slowLogTxn) Commit() (*clientv3.TxnResponse, error) {
	resp, err := t.Txn.Commit()
	t.cancel()
	return resp, err
}
