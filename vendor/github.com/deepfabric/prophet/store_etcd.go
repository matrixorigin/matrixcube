package prophet

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/format"
)

const (
	// DefaultTimeout default timeout
	DefaultTimeout = time.Second * 3
	// DefaultRequestTimeout default request timeout
	DefaultRequestTimeout = 10 * time.Second
	// DefaultSlowRequestTime default slow request time
	DefaultSlowRequestTime = time.Second * 1

	idBatch = 1000
)

var (
	errMaybeNotLeader    = errors.New("may be not leader")
	errSchedulerExisted  = errors.New("scheduler is existed")
	errSchedulerNotFound = errors.New("scheduler is not found")
)

var (
	endID = uint64(math.MaxUint64)
)

func getCurrentClusterMembers(client *clientv3.Client) (*clientv3.MemberListResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	members, err := client.MemberList(ctx)
	cancel()

	return members, err
}

type etcdStore struct {
	sync.Mutex
	adapter       Adapter
	client        *clientv3.Client
	idPath        string
	resourcePath  string
	containerPath string
	clusterPath   string

	store Store
	base  uint64
	end   uint64

	signature string
	elector   Elector
}

func newEtcdStore(client *clientv3.Client, adapter Adapter, signature string, elector Elector) Store {
	return &etcdStore{
		adapter:       adapter,
		client:        client,
		signature:     signature,
		elector:       elector,
		idPath:        "/meta/id",
		resourcePath:  "/meta/resources",
		containerPath: "/meta/containers",
		clusterPath:   "/cluster",
	}
}

// PutContainer returns nil if container is add or update succ
func (s *etcdStore) PutContainer(meta Container) error {
	key := s.getKey(meta.ID(), s.containerPath)
	data, err := meta.Marshal()
	if err != nil {
		return err
	}

	return s.save(key, string(data))
}

func (s *etcdStore) GetResource(id uint64) (Resource, error) {
	key := s.getKey(id, s.resourcePath)
	data, _, err := s.getValue(key)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	res := s.adapter.NewResource()
	err = res.Unmarshal(data)
	if err != nil {
		return nil, err
	}

	return res, nil
}

// GetContainer returns the spec container
func (s *etcdStore) GetContainer(id uint64) (Container, error) {
	key := s.getKey(id, s.containerPath)
	data, _, err := s.getValue(key)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	c := s.adapter.NewContainer()
	err = c.Unmarshal(data)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// PutResource returns nil if resource is add or update succ
func (s *etcdStore) PutResource(meta Resource) error {
	key := s.getKey(meta.ID(), s.resourcePath)
	data, err := meta.Marshal()
	if err != nil {
		return err
	}

	return s.save(key, string(data))
}

func (s *etcdStore) LoadResources(limit int64, do func(Resource)) error {
	startID := uint64(0)
	endKey := s.getKey(endID, s.resourcePath)
	withRange := clientv3.WithRange(endKey)
	withLimit := clientv3.WithLimit(limit)

	for {
		startKey := s.getKey(startID, s.resourcePath)
		resp, err := s.get(startKey, withRange, withLimit)
		if err != nil {
			return err
		}

		for _, item := range resp.Kvs {
			v := s.adapter.NewResource()
			err := v.Unmarshal(item.Value)
			if err != nil {
				return err
			}

			startID = v.ID() + 1
			do(v)
		}

		// read complete
		if len(resp.Kvs) < int(limit) {
			break
		}
	}

	return nil
}

func (s *etcdStore) LoadContainers(limit int64, do func(Container)) error {
	startID := uint64(0)
	endKey := s.getKey(endID, s.containerPath)
	withRange := clientv3.WithRange(endKey)
	withLimit := clientv3.WithLimit(limit)

	for {
		startKey := s.getKey(startID, s.containerPath)
		resp, err := s.get(startKey, withRange, withLimit)
		if err != nil {
			return err
		}

		for _, item := range resp.Kvs {
			v := s.adapter.NewContainer()
			err := v.Unmarshal(item.Value)
			if err != nil {
				return err
			}

			startID = v.ID() + 1
			do(v)
		}

		// read complete
		if len(resp.Kvs) < int(limit) {
			break
		}
	}

	return nil
}

func (s *etcdStore) AllocID() (uint64, error) {
	s.Lock()
	defer s.Unlock()

	if s.base == s.end {
		end, err := s.generate()
		if err != nil {
			return 0, err
		}

		s.end = end
		s.base = s.end - idBatch
	}

	s.base++
	return s.base, nil
}

func (s *etcdStore) generate() (uint64, error) {
	value, err := s.getID()
	if err != nil {
		return 0, err
	}

	max := value + idBatch

	// create id
	if value == 0 {
		max := value + idBatch
		err := s.createID(s.signature, max)
		if err != nil {
			return 0, err
		}

		return max, nil
	}

	err = s.updateID(s.signature, value, max)
	if err != nil {
		return 0, err
	}

	return max, nil
}

func (s *etcdStore) AlreadyBootstrapped() (bool, error) {
	resp, err := s.get(s.clusterPath, clientv3.WithCountOnly())
	if err != nil {
		return false, nil
	}

	return resp.Count > 0, nil
}

// PutBootstrapped put cluster is bootstrapped
func (s *etcdStore) PutBootstrapped(container Container, resources ...Resource) (bool, error) {
	clusterID, err := s.AllocID()
	if err != nil {
		return false, err
	}

	ctx, cancel := context.WithTimeout(s.client.Ctx(), DefaultTimeout)
	defer cancel()

	// build operations
	var ops []clientv3.Op
	ops = append(ops, clientv3.OpPut(s.clusterPath, string(goetty.Uint64ToBytes(clusterID))))

	meta, err := container.Marshal()
	if err != nil {
		return false, err
	}
	ops = append(ops, clientv3.OpPut(s.getKey(container.ID(), s.containerPath), string(meta)))

	resp, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(s.clusterPath), "=", 0)).
		Then(ops...).
		Commit()

	if err != nil {
		return false, err
	}

	// already bootstrapped
	if !resp.Succeeded {
		return false, nil
	}

	ops = ops[:0]
	for idx, res := range resources {
		meta, err = res.Marshal()
		if err != nil {
			return false, err
		}
		ops = append(ops, clientv3.OpPut(s.getKey(res.ID(), s.resourcePath), string(meta)))

		if idx > 0 && idx%16 == 0 {
			resp, err = s.client.Txn(ctx).
				Then(ops...).
				Commit()
			if err != nil {
				return false, err
			}

			ops = ops[:0]
		}
	}

	if len(ops) > 0 {
		resp, err = s.client.Txn(ctx).
			Then(ops...).
			Commit()
		if err != nil {
			return false, err
		}
	}

	return true, nil
}

func (s *etcdStore) PutIfNotExists(path string, value []byte) (bool, []byte, error) {
	resp, err := s.txn().
		If(clientv3.Compare(clientv3.CreateRevision(path), "=", 0)).
		Then(clientv3.OpPut(path, string(value))).
		Commit()
	if err != nil {
		return false, nil, err
	}

	if !resp.Succeeded {
		data, _, err := s.getValue(path)
		if err != nil {
			return false, nil, err
		}

		return false, data, nil
	}

	return true, nil, nil
}

func (s *etcdStore) RemoveIfValueMatched(path string, expect []byte) (bool, error) {
	resp, err := s.txn().
		If(clientv3.Compare(clientv3.Value(path), "=", string(expect))).
		Then(clientv3.OpDelete(path)).
		Commit()
	if err != nil {
		return false, err
	}

	if !resp.Succeeded {
		return false, nil
	}

	return true, nil
}

func (s *etcdStore) getID() (uint64, error) {
	resp, _, err := s.getValue(s.idPath)
	if err != nil {
		return 0, err
	}

	if len(resp) == 0 {
		return 0, nil
	}

	return format.BytesToUint64(resp)
}

func (s *etcdStore) createID(leaderSignature string, value uint64) error {
	cmp := clientv3.Compare(clientv3.CreateRevision(s.idPath), "=", 0)
	op := clientv3.OpPut(s.idPath, string(format.Uint64ToBytes(value)))

	ok, err := s.elector.DoIfLeader(math.MaxUint64, leaderSignature, []clientv3.Cmp{cmp}, op)
	if err != nil {
		return err
	}

	if !ok {
		return errMaybeNotLeader
	}

	return nil
}

func (s *etcdStore) updateID(leaderSignature string, old, value uint64) error {
	cmp := clientv3.Compare(clientv3.Value(s.idPath), "=", string(format.Uint64ToBytes(old)))
	op := clientv3.OpPut(s.idPath, string(format.Uint64ToBytes(value)))

	ok, err := s.elector.DoIfLeader(math.MaxUint64, leaderSignature, []clientv3.Cmp{cmp}, op)
	if err != nil {
		return err
	}

	if !ok {
		return errMaybeNotLeader
	}

	return nil
}

func (s *etcdStore) getKey(id uint64, base string) string {
	return fmt.Sprintf("%s/%020d", base, id)
}

func (s *etcdStore) txn() clientv3.Txn {
	return newSlowLogTxn(s.client)
}

func (s *etcdStore) getValue(key string, opts ...clientv3.OpOption) ([]byte, int64, error) {
	resp, err := s.get(key, opts...)
	if err != nil {
		return nil, 0, err
	}

	if n := len(resp.Kvs); n == 0 {
		return nil, 0, nil
	} else if n > 1 {
		return nil, 0, fmt.Errorf("invalid get value resp %v, must only one", resp.Kvs)
	}

	return resp.Kvs[0].Value, resp.Kvs[0].ModRevision, nil
}

func (s *etcdStore) get(key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(s.client.Ctx(), DefaultRequestTimeout)
	defer cancel()

	start := time.Now()
	resp, err := clientv3.NewKV(s.client).Get(ctx, key, opts...)
	if err != nil {
		log.Errorf("read option failure, key=<%s>, errors:\n %+v",
			key,
			err)
		return resp, err
	}

	if cost := time.Since(start); cost > DefaultSlowRequestTime {
		log.Warningf("read option is too slow, key=<%s>, cost=<%d>",
			key,
			cost)
	}

	return resp, nil
}

func (s *etcdStore) save(key, value string) error {
	ok, err := s.elector.DoIfLeader(math.MaxUint64, s.signature, nil, clientv3.OpPut(key, value))
	if err != nil {
		return err
	}

	if !ok {
		return errMaybeNotLeader
	}

	return nil
}

func (s *etcdStore) create(key, value string) error {
	resp, err := s.txn().If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).Then(clientv3.OpPut(key, value)).Commit()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return errMaybeNotLeader
	}

	return nil
}
