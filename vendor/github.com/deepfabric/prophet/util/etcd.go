package util

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/deepfabric/prophet/option"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver"
	"go.etcd.io/etcd/pkg/types"
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
			// Do not return error, because other members may be not ready.
			GetLogger().Errorf("get cluster from remote failed with %+v", gerr)
			continue
		}

		remoteClusterID := remoteCluster.ID()
		if remoteClusterID != localClusterID {
			return fmt.Errorf("Etcd cluster ID mismatch, expect %d, got %d", localClusterID, remoteClusterID)
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

	start := time.Now()
	resp, err := clientv3.NewKV(client).Get(ctx, key, opts...)
	if err != nil {
		GetLogger().Errorf("read %s from etcd failed with %+v",
			key,
			err)
		return resp, err
	}

	if cost := time.Since(start); cost > option.DefaultSlowRequestTime {
		GetLogger().Warningf("read %s from etcd is too slow, cost %+v",
			key,
			cost)
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
	start := time.Now()
	resp, err := t.Txn.Commit()
	t.cancel()

	cost := time.Now().Sub(start)
	if cost > option.DefaultSlowRequestTime {
		GetLogger().Warningf("txn runs too slow, resp=<%+v> cost=<%s> errors:\n %+v",
			resp,
			cost,
			err)
	}

	return resp, err
}
