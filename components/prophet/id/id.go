// Copyright 2020 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain alloc copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package id

import (
	"fmt"
	"sync"

	"github.com/fagongzi/util/format"
	"github.com/matrixorigin/matrixcube/components/prophet/election"
	"github.com/matrixorigin/matrixcube/components/prophet/util"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Generator is an ID generator which generate unique ID.
type Generator interface {
	// AllocID allocs unique id.
	AllocID() (uint64, error)
}

const (
	idBatch         uint64 = 512
	UninitializedID uint64 = 0
)

// etcdGenerator allocate ID based on etcd.
type etcdGenerator struct {
	sync.Mutex

	client   *clientv3.Client
	leadship *election.Leadership
	idPath   string
	base     uint64
	end      uint64
}

// NewEtcdGenerator returns alloc ID allocator based on etcd.
func NewEtcdGenerator(
	rootPath string,
	client *clientv3.Client,
	leadship *election.Leadership,
) Generator {
	return &etcdGenerator{
		client:   client,
		leadship: leadship,
		idPath:   fmt.Sprintf("%s/meta/id", rootPath),
	}
}

// AllocID allocs alloc unique id.
func (alloc *etcdGenerator) AllocID() (uint64, error) {
	alloc.Lock()
	defer alloc.Unlock()

	if alloc.base == alloc.end {
		if err := alloc.preemption(); err != nil {
			return 0, err
		}
	}

	alloc.base++
	return alloc.base, nil
}

// preemption grabs a range of IDs.
func (alloc *etcdGenerator) preemption() error {
	value, err := alloc.getID()
	if err != nil {
		return err

	}
	end := value + idBatch

	if value == 0 {
		err = alloc.createID(end)
		if err != nil {
			return err
		}
	} else {
		err = alloc.updateID(value, end)
		if err != nil {
			return err
		}
	}

	alloc.end = end
	alloc.base = value
	return nil
}

// getID get the current end of ID.
func (alloc *etcdGenerator) getID() (uint64, error) {
	resp, _, err := util.GetEtcdValue(alloc.client, alloc.idPath)
	if err != nil {
		return 0, err
	}

	if len(resp) == 0 {
		return 0, nil
	}

	return format.BytesToUint64(resp)
}

// createID initialize the end of ID.
func (alloc *etcdGenerator) createID(end uint64) error {
	cmp := clientv3.Compare(clientv3.CreateRevision(alloc.idPath), "=", 0)
	op := clientv3.OpPut(alloc.idPath, string(format.Uint64ToBytes(end)))

	ok, err := alloc.leadship.DoIfLeader([]clientv3.Cmp{cmp}, op)
	if err != nil {
		return err
	}

	if !ok {
		return util.ErrNotLeader
	}
	return nil
}

// updateID update the end of ID.
func (alloc *etcdGenerator) updateID(old, end uint64) error {
	cmp := clientv3.Compare(
		clientv3.Value(alloc.idPath), "=", string(format.Uint64ToBytes(old)),
	)
	op := clientv3.OpPut(alloc.idPath, string(format.Uint64ToBytes(end)))

	ok, err := alloc.leadship.DoIfLeader([]clientv3.Cmp{cmp}, op)
	if err != nil {
		return err
	}

	if !ok {
		return util.ErrNotLeader
	}
	return nil
}
