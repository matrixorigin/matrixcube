package storage

import (
	"fmt"
	"strings"
	"sync"

	"github.com/deepfabric/prophet/election"
	"github.com/deepfabric/prophet/util"
	"github.com/fagongzi/util/format"
	"go.etcd.io/etcd/clientv3"
)

const (
	idBatch = 1000
)

type etcdKV struct {
	sync.Mutex

	rootPath string
	idPath   string
	client   *clientv3.Client
	leadship *election.Leadership

	// id alloc
	base uint64
	end  uint64
}

// NewEtcdKV returns a etcd kv
func NewEtcdKV(rootPath string, client *clientv3.Client, leadship *election.Leadership) KV {
	return &etcdKV{
		client:   client,
		leadship: leadship,
		rootPath: rootPath,
		idPath:   fmt.Sprintf("%s/meta/id", rootPath),
	}
}

func (s *etcdKV) Batch(batch *Batch) error {
	var ops []clientv3.Op
	for i := range batch.SaveKeys {
		ops = append(ops, clientv3.OpPut(batch.SaveKeys[i], batch.SaveValues[i]))
	}
	for _, k := range batch.RemoveKeys {
		ops = append(ops, clientv3.OpDelete(k))
	}

	ok, err := s.leadship.DoIfLeader(nil, ops...)
	if err != nil {
		return err
	}

	if !ok {
		return util.ErrNotLeader
	}

	return nil
}

func (s *etcdKV) Save(key, value string) error {
	ok, err := s.leadship.DoIfLeader(nil, clientv3.OpPut(key, value))
	if err != nil {
		return err
	}

	if !ok {
		return util.ErrNotLeader
	}

	return nil
}

func (s *etcdKV) Load(key string) (string, error) {
	data, _, err := util.GetEtcdValue(s.client, key)
	if err != nil {
		return "", err
	}

	if len(data) == 0 {
		return "", nil
	}

	return string(data), nil
}

func (s *etcdKV) Remove(key string) error {
	ok, err := s.leadship.DoIfLeader(nil, clientv3.OpDelete(key))
	if err != nil {
		return err
	}

	if !ok {
		return util.ErrNotLeader
	}

	return nil
}

func (s *etcdKV) LoadRange(key, endKey string, limit int64) ([]string, []string, error) {
	// Note: reason to use `strings.Join` instead of `path.Join` is that the latter will
	// removes suffix '/' of the joined string.
	// As a result, when we try to scan from "foo/", it ends up scanning from "/pd/foo"
	// internally, and returns unexpected keys such as "foo_bar/baz".
	key = strings.Join([]string{key}, "/")
	endKey = strings.Join([]string{endKey}, "/")

	withRange := clientv3.WithRange(endKey)
	withLimit := clientv3.WithLimit(limit)
	resp, err := util.GetEtcdResp(s.client, key, withRange, withLimit)
	if err != nil {
		return nil, nil, err
	}
	keys := make([]string, 0, len(resp.Kvs))
	values := make([]string, 0, len(resp.Kvs))
	for _, item := range resp.Kvs {
		keys = append(keys, strings.TrimPrefix(strings.TrimPrefix(string(item.Key), s.rootPath), "/"))
		values = append(values, string(item.Value))
	}
	return keys, values, nil
}

func (s *etcdKV) CountRange(key, endKey string) (uint64, error) {
	// Note: reason to use `strings.Join` instead of `path.Join` is that the latter will
	// removes suffix '/' of the joined string.
	// As a result, when we try to scan from "foo/", it ends up scanning from "/pd/foo"
	// internally, and returns unexpected keys such as "foo_bar/baz".
	key = strings.Join([]string{key}, "/")
	endKey = strings.Join([]string{endKey}, "/")

	withRange := clientv3.WithRange(endKey)
	resp, err := util.GetEtcdResp(s.client, key, withRange, clientv3.WithCountOnly())
	if err != nil {
		return 0, err
	}

	return uint64(resp.Count), nil
}

func (s *etcdKV) SaveIfNotExists(key string, value string, batch *Batch) (bool, string, error) {
	var ops []clientv3.Op
	ops = append(ops, clientv3.OpPut(key, value))
	if batch != nil {
		for i := range batch.SaveKeys {
			ops = append(ops, clientv3.OpPut(batch.SaveKeys[i], batch.SaveValues[i]))
		}

		for _, k := range batch.RemoveKeys {
			ops = append(ops, clientv3.OpDelete(k))
		}
	}

	resp, err := util.Txn(s.client).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(ops...).
		Commit()
	if err != nil {
		return false, "", err
	}

	if !resp.Succeeded {
		data, _, err := util.GetEtcdValue(s.client, key)
		if err != nil {
			return false, "", err
		}

		return false, string(data), nil
	}

	return true, "", nil
}

func (s *etcdKV) RemoveIfValueMatched(key string, expect string) (bool, error) {
	resp, err := util.Txn(s.client).
		If(clientv3.Compare(clientv3.Value(key), "=", string(expect))).
		Then(clientv3.OpDelete(key)).
		Commit()
	if err != nil {
		return false, err
	}

	if !resp.Succeeded {
		return false, nil
	}

	return true, nil
}

func (s *etcdKV) AllocID() (uint64, error) {
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

func (s *etcdKV) generate() (uint64, error) {
	value, err := s.getID()
	if err != nil {
		return 0, err
	}

	max := value + idBatch

	// create id
	if value == 0 {
		max := value + idBatch
		err := s.createID(max)
		if err != nil {
			return 0, err
		}

		return max, nil
	}

	err = s.updateID(value, max)
	if err != nil {
		return 0, err
	}

	return max, nil
}

func (s *etcdKV) getID() (uint64, error) {
	resp, _, err := util.GetEtcdValue(s.client, s.idPath)
	if err != nil {
		return 0, err
	}

	if len(resp) == 0 {
		return 0, nil
	}

	return format.BytesToUint64(resp)
}

func (s *etcdKV) createID(value uint64) error {
	cmp := clientv3.Compare(clientv3.CreateRevision(s.idPath), "=", 0)
	op := clientv3.OpPut(s.idPath, string(format.Uint64ToBytes(value)))

	ok, err := s.leadship.DoIfLeader([]clientv3.Cmp{cmp}, op)
	if err != nil {
		return err
	}

	if !ok {
		return util.ErrNotLeader
	}

	return nil
}

func (s *etcdKV) updateID(old, value uint64) error {
	cmp := clientv3.Compare(clientv3.Value(s.idPath), "=", string(format.Uint64ToBytes(old)))
	op := clientv3.OpPut(s.idPath, string(format.Uint64ToBytes(value)))

	ok, err := s.leadship.DoIfLeader([]clientv3.Cmp{cmp}, op)
	if err != nil {
		return err
	}

	if !ok {
		return util.ErrNotLeader
	}

	return nil
}
