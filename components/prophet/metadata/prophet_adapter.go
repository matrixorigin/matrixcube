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

type Shard struct {
	sync.RWMutex

	Shard metapb.Shard
}

func NewShard() *Shard {
	return &Shard{}
}

// NewShardFromShard create a prophet Shard from an existed Shard
func NewShardFromShard(meta metapb.Shard) *Shard {
	return &Shard{Shard: meta}
}

func (shard *Shard) ID() uint64 {
	// shard.RLock()
	// defer shard.RUnlock()

	return shard.Shard.ID
}

func (shard *Shard) SetID(id uint64) {
	// shard.Lock()
	// defer shard.Unlock()

	shard.Shard.ID = id
}

func (shard *Shard) Group() uint64 {
	// shard.RLock()
	// defer shard.RUnlock()

	return shard.Shard.Group
}

func (shard *Shard) SetGroup(group uint64) {
	shard.Shard.Group = group
}

func (shard *Shard) Replicas() []metapb.Replica {
	// shard.RLock()
	// defer shard.RUnlock()

	return shard.Shard.Replicas
}

func (shard *Shard) SetReplicas(replicas []metapb.Replica) {
	// shard.Lock()
	// defer shard.Unlock()

	shard.Shard.Replicas = replicas
}

func (shard *Shard) AppendReplica(replica metapb.Replica) {
	// shard.Lock()
	// defer shard.Unlock()

	shard.Shard.Replicas = append(shard.Shard.Replicas, replica)
}

func (shard *Shard) Range() ([]byte, []byte) {
	// shard.RLock()
	// defer shard.RUnlock()

	return shard.Shard.Start, shard.Shard.End
}

func (shard *Shard) SetStartKey(value []byte) {
	// shard.Lock()
	// defer shard.Unlock()

	shard.Shard.Start = value
}

func (shard *Shard) SetEndKey(value []byte) {
	// shard.Lock()
	// defer shard.Unlock()

	shard.Shard.End = value
}

func (shard *Shard) Epoch() metapb.ShardEpoch {
	// shard.RLock()
	// defer shard.RUnlock()

	return shard.Shard.Epoch
}

func (shard *Shard) SetEpoch(value metapb.ShardEpoch) {
	// shard.Lock()
	// defer shard.Unlock()

	shard.Shard.Epoch = value
}

func (shard *Shard) State() metapb.ShardState {
	// shard.RLock()
	// defer shard.RUnlock()

	return shard.Shard.State
}

func (shard *Shard) StateLocked() metapb.ShardState {
	shard.RLock()
	defer shard.RUnlock()

	return shard.Shard.State
}

func (shard *Shard) SetState(state metapb.ShardState) {
	// shard.Lock()
	// defer shard.Unlock()

	shard.Shard.State = state
}

func (shard *Shard) SetStateLocked(state metapb.ShardState) {
	shard.Lock()
	defer shard.Unlock()

	shard.Shard.State = state
}

func (shard *Shard) Unique() string {
	// shard.RLock()
	// defer shard.RUnlock()

	return shard.Shard.Unique
}

func (shard *Shard) SetUnique(value string) {
	// shard.Lock()
	// defer shard.Unlock()

	shard.Shard.Unique = value
}

func (shard *Shard) Data() []byte {
	// shard.RLock()
	// defer shard.RUnlock()

	return shard.Shard.Data
}

func (shard *Shard) SetData(value []byte) {
	// shard.Lock()
	// defer shard.Unlock()

	shard.Shard.Data = value
}

func (shard *Shard) RuleGroups() []string {
	// shard.RLock()
	// defer shard.RUnlock()

	return shard.Shard.RuleGroups
}

func (shard *Shard) SetRuleGroups(values ...string) {
	// shard.Lock()
	// defer shard.Unlock()

	shard.Shard.RuleGroups = values
}

func (shard *Shard) Labels() []metapb.Pair {
	// shard.RLock()
	// defer shard.RUnlock()

	return shard.Shard.Labels
}

func (shard *Shard) SetLabels(labels []metapb.Pair) {
	// shard.Lock()
	// defer shard.Unlock()

	shard.Shard.Labels = labels
}

func (shard *Shard) Marshal() ([]byte, error) {
	// shard.RLock()
	// defer shard.RUnlock()

	return protoc.MustMarshal(&shard.Shard), nil
}

func (shard *Shard) Unmarshal(data []byte) error {
	// shard.Lock()
	// defer shard.Unlock()

	protoc.MustUnmarshal(&shard.Shard, data)
	return nil
}

func (shard *Shard) Clone() *Shard {
	value := &Shard{}
	data, _ := shard.Marshal()
	value.Unmarshal(data)
	return value
}

type Store struct {
	// sync.RWMutex

	Store metapb.Store
}

func NewStore() *Store {
	return &Store{}
}

func (store *Store) SetAddrs(addr, shardAddr string) {
	// store.Lock()
	// defer store.Unlock()

	store.Store.ClientAddr = addr
	store.Store.RaftAddr = shardAddr
}

func (store *Store) Addr() string {
	// store.RLock()
	// defer store.RUnlock()

	return store.Store.ClientAddr
}

func (store *Store) ShardAddr() string {
	// store.RLock()
	// defer store.RUnlock()

	return store.Store.RaftAddr
}

func (store *Store) SetID(id uint64) {
	// store.Lock()
	// defer store.Unlock()

	store.Store.ID = id
}

func (store *Store) ID() uint64 {
	// store.RLock()
	// defer store.RUnlock()

	return store.Store.ID
}

func (store *Store) Labels() []metapb.Pair {
	// store.RLock()
	// defer store.RUnlock()

	return store.Store.Labels
}

func (store *Store) SetLabels(labels []metapb.Pair) {
	// store.Lock()
	// defer store.Unlock()

	store.Store.Labels = labels
}

func (store *Store) StartTimestamp() int64 {
	// store.RLock()
	// defer store.RUnlock()

	return store.Store.StartTime
}

func (store *Store) SetStartTimestamp(value int64) {
	// store.Lock()
	// defer store.Unlock()

	store.Store.StartTime = value
}

func (store *Store) Version() (string, string) {
	// store.RLock()
	// defer store.RUnlock()

	return store.Store.Version, store.Store.GitHash
}

func (store *Store) SetVersion(version string, githash string) {
	// store.Lock()
	// defer store.Unlock()

	store.Store.Version = version
	store.Store.GitHash = githash
}

func (store *Store) DeployPath() string {
	// store.RLock()
	// defer store.RUnlock()

	return store.Store.DeployPath
}

func (store *Store) SetDeployPath(value string) {
	// store.Lock()
	// defer store.Unlock()

	store.Store.DeployPath = value
}

func (store *Store) State() metapb.StoreState {
	// store.RLock()
	// defer store.RUnlock()

	return store.Store.State
}

func (store *Store) SetState(value metapb.StoreState) {
	// store.Lock()
	// defer store.Unlock()

	store.Store.State = value
}

func (store *Store) LastHeartbeat() int64 {
	// store.RLock()
	// defer store.RUnlock()

	return store.Store.LastHeartbeatTime
}

func (store *Store) SetLastHeartbeat(value int64) {
	// store.Lock()
	// defer store.Unlock()

	store.Store.LastHeartbeatTime = value
}

func (store *Store) PhysicallyDestroyed() bool {
	// store.RLock()
	// defer store.RUnlock()

	return store.Store.PhysicallyDestroyed
}

func (store *Store) SetPhysicallyDestroyed(v bool) {
	// store.Lock()
	// defer store.Unlock()

	store.Store.PhysicallyDestroyed = v
}

func (store *Store) Marshal() ([]byte, error) {
	// store.RLock()
	// defer store.RUnlock()

	return protoc.MustMarshal(&store.Store), nil
}

func (store *Store) Unmarshal(data []byte) error {
	// store.Lock()
	// defer store.Unlock()

	protoc.MustUnmarshal(&store.Store, data)
	return nil
}

func (store *Store) Clone() *Store {
	value := &Store{}
	data, _ := store.Marshal()
	value.Unmarshal(data)
	return value
}

func NewTestStore(storeID uint64) *Store {
	return &Store{
		Store: metapb.Store{
			ID: storeID,
		},
	}
}

func NewTestShard(shardID uint64) *Shard {
	return &Shard{
		Shard: metapb.Shard{
			ID: shardID,
		},
	}
}
