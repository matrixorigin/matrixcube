package prophet

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/embed"
	"github.com/golang/mock/gomock"
)

var (
	mutex sync.Mutex
	ports = 10000
)

func nextTestPorts() int {
	mutex.Lock()
	defer mutex.Unlock()

	ports++
	return ports
}

func startTestSingleEtcd(t *testing.T) (chan interface{}, int, error) {
	port := nextTestPorts()
	peerPort := nextTestPorts()

	now := time.Now().UnixNano()

	cfg := embed.NewConfig()
	cfg.Name = "p1"
	cfg.Dir = fmt.Sprintf("%s/prophet/test-%d", os.TempDir(), now)
	cfg.WalDir = ""
	cfg.InitialCluster = fmt.Sprintf("p1=http://127.0.0.1:%d", peerPort)
	cfg.ClusterState = embed.ClusterStateFlagNew
	cfg.EnablePprof = false
	cfg.Debug = false
	cfg.LPUrls, _ = parseUrls(fmt.Sprintf("http://127.0.0.1:%d", peerPort))
	cfg.APUrls = cfg.LPUrls
	cfg.LCUrls, _ = parseUrls(fmt.Sprintf("http://127.0.0.1:%d", port))
	cfg.ACUrls = cfg.LCUrls

	etcd, err := embed.StartEtcd(cfg)
	if err != nil {
		return nil, 0, err
	}

	select {
	case <-etcd.Server.ReadyNotify():
		time.Sleep(time.Millisecond * 100)
		stopC := make(chan interface{})
		go func() {
			<-stopC
			etcd.Server.Stop()
			os.RemoveAll(cfg.Dir)
		}()

		return stopC, port, nil
	case <-time.After(time.Minute * 5):
		return nil, 0, errors.New("start embed etcd timeout")
	}
}

type memLocalStorage struct {
	sync.Mutex
	kvs map[string][]byte
}

func newMemLocalStorage() LocalStorage {
	return &memLocalStorage{
		kvs: make(map[string][]byte),
	}
}

func (ls *memLocalStorage) Get(key []byte) ([]byte, error) {
	ls.Lock()
	defer ls.Unlock()

	if value, ok := ls.kvs[string(key)]; ok {
		return value, nil
	}

	return nil, nil
}

func (ls *memLocalStorage) Set(pairs ...[]byte) error {
	ls.Lock()
	defer ls.Unlock()

	n := len(pairs)
	for i := 0; i < n/2; i++ {
		ls.kvs[string(pairs[2*i])] = pairs[2*i+1]
	}

	return nil
}

func (ls *memLocalStorage) Remove(keys ...[]byte) error {
	ls.Lock()
	defer ls.Unlock()

	for _, key := range keys {
		delete(ls.kvs, string(key))
	}

	return nil
}

func (ls *memLocalStorage) Range(prefix []byte, limit uint64, fn func(key, value []byte) bool) error {
	ls.Lock()
	defer ls.Unlock()

	var keys []string
	for key := range ls.kvs {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	n := uint64(0)
	for _, key := range keys {
		if limit > 0 && n >= limit {
			break
		}

		if strings.HasPrefix(key, string(prefix)) {
			next := fn([]byte(key), ls.kvs[key])
			n++

			if !next {
				break
			}
		}
	}

	return nil
}

type testResource struct {
	ResID     uint64  `json:"id"`
	Version   uint64  `json:"version"`
	ResPeers  []*Peer `json:"peers"`
	ResLabels []Pair  `json:"labels"`
	err       bool
}

func newTestResource() *testResource {
	return &testResource{}
}

func (res *testResource) SetID(id uint64) {
	res.ResID = id
}

func (res *testResource) ID() uint64 {
	return res.ResID
}

func (res *testResource) Peers() []*Peer {
	return res.ResPeers
}

func (res *testResource) SetPeers(peers []*Peer) {
	res.ResPeers = peers
}

func (res *testResource) Stale(other Resource) bool {
	return res.Version > other.(*testResource).Version
}

func (res *testResource) Changed(other Resource) bool {
	return res.Version < other.(*testResource).Version
}

func (res *testResource) Labels() []Pair {
	return res.ResLabels
}

func (res *testResource) Clone() Resource {
	data, _ := res.Marshal()
	value := newTestResource()
	value.Unmarshal(data)
	return value
}

func (res *testResource) ScaleCompleted(uint64) bool {
	return false
}

func (res *testResource) Marshal() ([]byte, error) {
	if res.err {
		return nil, errors.New("test error")
	}

	return json.Marshal(res)
}

func (res *testResource) Unmarshal(data []byte) error {
	if res.err {
		return errors.New("test error")
	}

	return json.Unmarshal(data, res)
}

// SupportRebalance support rebalance the resource
func (res *testResource) SupportRebalance() bool {
	return true
}

// SupportTransferLeader support transfer leader
func (res *testResource) SupportTransferLeader() bool {
	return true
}

type testContainer struct {
	CShardAddr string `json:"shardAddr"`
	CID        uint64 `json:"cid"`
	CLabels    []Pair `json:"labels"`
	CState     State  `json:"state"`
	CAction    Action `json:"action"`
}

func newTestContainer() *testContainer {
	return &testContainer{}
}

func (c *testContainer) ShardAddr() string {
	return c.CShardAddr
}

func (c *testContainer) SetID(id uint64) {
	c.CID = id
}

func (c *testContainer) ID() uint64 {
	return c.CID
}

func (c *testContainer) Labels() []Pair {
	return c.CLabels
}

func (c *testContainer) State() State {
	return c.CState
}

func (c *testContainer) ActionOnJoinCluster() Action {
	return c.CAction
}

func (c *testContainer) Clone() Container {
	value := newTestContainer()
	data, _ := c.Marshal()
	value.Unmarshal(data)
	return value
}

func (c *testContainer) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

func (c *testContainer) Unmarshal(data []byte) error {
	return json.Unmarshal(data, c)
}

func newTestRPC(ctrl *gomock.Controller, start uint64) RPC {
	value := start
	fn := func() (uint64, error) {
		return atomic.AddUint64(&value, 1), nil
	}

	rpc := NewMockRPC(ctrl)
	rpc.EXPECT().AllocID().AnyTimes().DoAndReturn(fn)
	rpc.EXPECT().TiggerResourceHeartbeat(gomock.Any()).AnyTimes().DoAndReturn(func(uint64) {})
	rpc.EXPECT().TiggerContainerHeartbeat().AnyTimes()
	return rpc
}

func newTestProphet(ctrl *gomock.Controller, store Store, rpc RPC, client *clientv3.Client, startFunc func()) Prophet {
	pd := NewMockProphet(ctrl)
	pd.EXPECT().GetRPC().AnyTimes().Return(rpc)
	pd.EXPECT().GetStore().AnyTimes().Return(store)
	pd.EXPECT().GetEtcdClient().AnyTimes().Return(client)
	pd.EXPECT().Start().AnyTimes().Do(startFunc)
	return pd
}

func newTestAdapter(ctrl *gomock.Controller) Adapter {
	value := NewMockAdapter(ctrl)
	value.EXPECT().NewContainer().AnyTimes().DoAndReturn(newTestContainer)
	value.EXPECT().NewResource().AnyTimes().DoAndReturn(newTestResource)
	return value
}

func newTestResourceStore(ctrl *gomock.Controller, meta Container) ResourceStore {
	value := NewMockResourceStore(ctrl)
	value.EXPECT().Meta().AnyTimes().Return(meta)
	return value
}

func newTestPeerReplicaHandler(ctrl *gomock.Controller) PeerReplicaHandler {
	value := NewMockPeerReplicaHandler(ctrl)
	value.EXPECT().ResourceBecomeLeader(gomock.Any()).AnyTimes()
	value.EXPECT().ResourceBecomeFollower(gomock.Any()).AnyTimes()
	return value
}
