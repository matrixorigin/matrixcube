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

package metadata

import (
	"sync"

	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

type ShardWithRWLock struct {
	sync.RWMutex

	Shard metapb.Shard
}

func NewShardWithRWLock() *ShardWithRWLock {
	return &ShardWithRWLock{}
}

// NewShardWithRWLockFromShard create a prophet Shard from an existed Shard
func NewShardWithRWLockFromShard(meta metapb.Shard) *ShardWithRWLock {
	return &ShardWithRWLock{Shard: meta}
}

func (shardWithRWLock *ShardWithRWLock) ID() uint64 {
	shardWithRWLock.RLock()
	defer shardWithRWLock.RUnlock()

	return shardWithRWLock.Shard.ID
}

func (shardWithRWLock *ShardWithRWLock) SetID(id uint64) {
	shardWithRWLock.Lock()
	defer shardWithRWLock.Unlock()

	shardWithRWLock.Shard.ID = id
}

func (shardWithRWLock *ShardWithRWLock) Group() uint64 {
	shardWithRWLock.RLock()
	defer shardWithRWLock.RUnlock()

	return shardWithRWLock.Shard.Group
}

func (shardWithRWLock *ShardWithRWLock) SetGroup(group uint64) {
	shardWithRWLock.Shard.Group = group
}

func (shardWithRWLock *ShardWithRWLock) Replicas() []metapb.Replica {
	shardWithRWLock.RLock()
	defer shardWithRWLock.RUnlock()

	return shardWithRWLock.Shard.Replicas
}

func (shardWithRWLock *ShardWithRWLock) SetReplicas(replicas []metapb.Replica) {
	shardWithRWLock.Lock()
	defer shardWithRWLock.Unlock()

	shardWithRWLock.Shard.Replicas = replicas
}

func (shardWithRWLock *ShardWithRWLock) AppendReplica(replica metapb.Replica) {
	shardWithRWLock.Lock()
	defer shardWithRWLock.Unlock()

	shardWithRWLock.Shard.Replicas = append(shardWithRWLock.Shard.Replicas, replica)
}

func (shardWithRWLock *ShardWithRWLock) Range() ([]byte, []byte) {
	shardWithRWLock.RLock()
	defer shardWithRWLock.RUnlock()

	return shardWithRWLock.Shard.Start, shardWithRWLock.Shard.End
}

func (shardWithRWLock *ShardWithRWLock) SetStartKey(value []byte) {
	shardWithRWLock.Lock()
	defer shardWithRWLock.Unlock()

	shardWithRWLock.Shard.Start = value
}

func (shardWithRWLock *ShardWithRWLock) SetEndKey(value []byte) {
	shardWithRWLock.Lock()
	defer shardWithRWLock.Unlock()

	shardWithRWLock.Shard.End = value
}

func (shardWithRWLock *ShardWithRWLock) Epoch() metapb.ShardEpoch {
	shardWithRWLock.RLock()
	defer shardWithRWLock.RUnlock()

	return shardWithRWLock.Shard.Epoch
}

func (shardWithRWLock *ShardWithRWLock) SetEpoch(value metapb.ShardEpoch) {
	shardWithRWLock.Lock()
	defer shardWithRWLock.Unlock()

	shardWithRWLock.Shard.Epoch = value
}

func (shardWithRWLock *ShardWithRWLock) State() metapb.ShardState {
	shardWithRWLock.RLock()
	defer shardWithRWLock.RUnlock()

	return shardWithRWLock.Shard.State
}

func (shardWithRWLock *ShardWithRWLock) SetState(state metapb.ShardState) {
	shardWithRWLock.Lock()
	defer shardWithRWLock.Unlock()

	shardWithRWLock.Shard.State = state
}

func (shardWithRWLock *ShardWithRWLock) Unique() string {
	shardWithRWLock.RLock()
	defer shardWithRWLock.RUnlock()

	return shardWithRWLock.Shard.Unique
}

func (shardWithRWLock *ShardWithRWLock) SetUnique(value string) {
	shardWithRWLock.Lock()
	defer shardWithRWLock.Unlock()

	shardWithRWLock.Shard.Unique = value
}

func (shardWithRWLock *ShardWithRWLock) Data() []byte {
	shardWithRWLock.RLock()
	defer shardWithRWLock.RUnlock()

	return shardWithRWLock.Shard.Data
}

func (shardWithRWLock *ShardWithRWLock) SetData(value []byte) {
	shardWithRWLock.Lock()
	defer shardWithRWLock.Unlock()

	shardWithRWLock.Shard.Data = value
}

func (shardWithRWLock *ShardWithRWLock) RuleGroups() []string {
	shardWithRWLock.RLock()
	defer shardWithRWLock.RUnlock()

	return shardWithRWLock.Shard.RuleGroups
}

func (shardWithRWLock *ShardWithRWLock) SetRuleGroups(values ...string) {
	shardWithRWLock.Lock()
	defer shardWithRWLock.Unlock()

	shardWithRWLock.Shard.RuleGroups = values
}

func (shardWithRWLock *ShardWithRWLock) Labels() []metapb.Pair {
	shardWithRWLock.RLock()
	defer shardWithRWLock.RUnlock()

	return shardWithRWLock.Shard.Labels
}

func (shardWithRWLock *ShardWithRWLock) SetLabels(labels []metapb.Pair) {
	shardWithRWLock.Lock()
	defer shardWithRWLock.Unlock()

	shardWithRWLock.Shard.Labels = labels
}

func (shardWithRWLock *ShardWithRWLock) Marshal() ([]byte, error) {
	shardWithRWLock.RLock()
	defer shardWithRWLock.RUnlock()

	return protoc.MustMarshal(&shardWithRWLock.Shard), nil
}

func (shardWithRWLock *ShardWithRWLock) Unmarshal(data []byte) error {
	shardWithRWLock.Lock()
	defer shardWithRWLock.Unlock()

	protoc.MustUnmarshal(&shardWithRWLock.Shard, data)
	return nil
}

func (shardWithRWLock *ShardWithRWLock) Clone() *ShardWithRWLock {
	value := &ShardWithRWLock{}
	data, _ := shardWithRWLock.Marshal()
	value.Unmarshal(data)
	return value
}

type StoreWithRWLock struct {
	sync.RWMutex

	Store metapb.Store
}

func NewStoreWithRWLock() *StoreWithRWLock {
	return &StoreWithRWLock{}
}

func (storeWithRWLock *StoreWithRWLock) SetAddrs(addr, shardAddr string) {
	storeWithRWLock.Lock()
	defer storeWithRWLock.Unlock()

	storeWithRWLock.Store.ClientAddr = addr
	storeWithRWLock.Store.RaftAddr = shardAddr
}

func (storeWithRWLock *StoreWithRWLock) Addr() string {
	storeWithRWLock.RLock()
	defer storeWithRWLock.RUnlock()

	return storeWithRWLock.Store.ClientAddr
}

func (storeWithRWLock *StoreWithRWLock) ShardAddr() string {
	storeWithRWLock.RLock()
	defer storeWithRWLock.RUnlock()

	return storeWithRWLock.Store.RaftAddr
}

func (storeWithRWLock *StoreWithRWLock) SetID(id uint64) {
	storeWithRWLock.Lock()
	defer storeWithRWLock.Unlock()

	storeWithRWLock.Store.ID = id
}

func (storeWithRWLock *StoreWithRWLock) ID() uint64 {
	storeWithRWLock.RLock()
	defer storeWithRWLock.RUnlock()

	return storeWithRWLock.Store.ID
}

func (storeWithRWLock *StoreWithRWLock) Labels() []metapb.Pair {
	storeWithRWLock.RLock()
	defer storeWithRWLock.RUnlock()

	return storeWithRWLock.Store.Labels
}

func (storeWithRWLock *StoreWithRWLock) SetLabels(labels []metapb.Pair) {
	storeWithRWLock.Lock()
	defer storeWithRWLock.Unlock()

	storeWithRWLock.Store.Labels = labels
}

func (storeWithRWLock *StoreWithRWLock) StartTimestamp() int64 {
	storeWithRWLock.RLock()
	defer storeWithRWLock.RUnlock()

	return storeWithRWLock.Store.StartTime
}

func (storeWithRWLock *StoreWithRWLock) SetStartTimestamp(value int64) {
	storeWithRWLock.Lock()
	defer storeWithRWLock.Unlock()

	storeWithRWLock.Store.StartTime = value
}

func (storeWithRWLock *StoreWithRWLock) Version() (string, string) {
	storeWithRWLock.RLock()
	defer storeWithRWLock.RUnlock()

	return storeWithRWLock.Store.Version, storeWithRWLock.Store.GitHash
}

func (storeWithRWLock *StoreWithRWLock) SetVersion(version string, githash string) {
	storeWithRWLock.Lock()
	defer storeWithRWLock.Unlock()

	storeWithRWLock.Store.Version = version
	storeWithRWLock.Store.GitHash = githash
}

func (storeWithRWLock *StoreWithRWLock) DeployPath() string {
	storeWithRWLock.RLock()
	defer storeWithRWLock.RUnlock()

	return storeWithRWLock.Store.DeployPath
}

func (storeWithRWLock *StoreWithRWLock) SetDeployPath(value string) {
	storeWithRWLock.Lock()
	defer storeWithRWLock.Unlock()

	storeWithRWLock.Store.DeployPath = value
}

func (storeWithRWLock *StoreWithRWLock) State() metapb.StoreState {
	storeWithRWLock.RLock()
	defer storeWithRWLock.RUnlock()

	return storeWithRWLock.Store.State
}

func (storeWithRWLock *StoreWithRWLock) SetState(value metapb.StoreState) {
	storeWithRWLock.Lock()
	defer storeWithRWLock.Unlock()

	storeWithRWLock.Store.State = value
}

func (storeWithRWLock *StoreWithRWLock) LastHeartbeat() int64 {
	storeWithRWLock.RLock()
	defer storeWithRWLock.RUnlock()

	return storeWithRWLock.Store.LastHeartbeatTime
}

func (storeWithRWLock *StoreWithRWLock) SetLastHeartbeat(value int64) {
	storeWithRWLock.Lock()
	defer storeWithRWLock.Unlock()

	storeWithRWLock.Store.LastHeartbeatTime = value
}

func (storeWithRWLock *StoreWithRWLock) PhysicallyDestroyed() bool {
	storeWithRWLock.RLock()
	defer storeWithRWLock.RUnlock()

	return storeWithRWLock.Store.PhysicallyDestroyed
}

func (storeWithRWLock *StoreWithRWLock) SetPhysicallyDestroyed(v bool) {
	storeWithRWLock.Lock()
	defer storeWithRWLock.Unlock()

	storeWithRWLock.Store.PhysicallyDestroyed = v
}

func (storeWithRWLock *StoreWithRWLock) Marshal() ([]byte, error) {
	storeWithRWLock.RLock()
	defer storeWithRWLock.RUnlock()

	return protoc.MustMarshal(&storeWithRWLock.Store), nil
}

func (storeWithRWLock *StoreWithRWLock) Unmarshal(data []byte) error {
	storeWithRWLock.Lock()
	defer storeWithRWLock.Unlock()

	protoc.MustUnmarshal(&storeWithRWLock.Store, data)
	return nil
}

func (storeWithRWLock *StoreWithRWLock) Clone() *StoreWithRWLock {
	value := &StoreWithRWLock{}
	data, _ := storeWithRWLock.Marshal()
	value.Unmarshal(data)
	return value
}

func NewTestStore(storeID uint64) *StoreWithRWLock {
	return &StoreWithRWLock{
		Store: metapb.Store{
			ID: storeID,
		},
	}
}

func NewTestShard(shardID uint64) *ShardWithRWLock {
	return &ShardWithRWLock{
		Shard: metapb.Shard{
			ID: shardID,
		},
	}
}
