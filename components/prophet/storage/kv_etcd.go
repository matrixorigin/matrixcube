// Copyright 2020 PingCAP, Inc.
// Modifications copyright (C) 2021 MatrixOrigin.
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

package storage

import (
	"fmt"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixcube/components/prophet/election"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type etcdKV struct {
	sync.Mutex

	rootPath string
	idPath   string
	client   *clientv3.Client
	leadship *election.Leadership
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

func (s *etcdKV) SaveWithoutLeader(key, value string) error {
	var ops []clientv3.Op
	ops = append(ops, clientv3.OpPut(key, value))

	_, err := util.Txn(s.client).Then(ops...).Commit()
	if err != nil {
		return err
	}

	return nil
}

func (s *etcdKV) RemoveWithoutLeader(key string) error {
	var ops []clientv3.Op
	ops = append(ops, clientv3.OpDelete(key))

	_, err := util.Txn(s.client).Then(ops...).Commit()
	if err != nil {
		return err
	}

	return nil
}
