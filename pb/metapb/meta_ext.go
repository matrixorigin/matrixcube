// Copyright 2021 MatrixOrigin.
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

package metapb

import (
	"bytes"

	"github.com/fagongzi/util/protoc"
)

// IsLastFileChunk returns a boolean value indicating whether the chunk is the
// last chunk of a snapshot file.
func (c *SnapshotChunk) IsLastFileChunk() bool {
	return c.FileChunkID+1 == c.FileChunkCount
}

// IsLastChunk returns a boolean value indicating whether the current chunk is
// the last one for the snapshot.
func (c *SnapshotChunk) IsLastChunk() bool {
	return c.ChunkCount == c.ChunkID+1
}

func NewShard() *Shard {
	return &Shard{}
}

func (m *Shard) GetRange() ([]byte, []byte) {
	return m.Start, m.End
}

func (m *Shard) SetID(shardID uint64) {
	m.ID = shardID
}

func (m *Shard) SetState(state ShardState) {
	m.State = state
}

func (m *Shard) SetStartKey(value []byte) {
	m.Start = value
}

func (m *Shard) SetEndKey(value []byte) {
	m.End = value
}

func (m *Shard) SetEpoch(epoch ShardEpoch) {
	m.Epoch = epoch
}

func (m *Shard) SetUnique(value string) {
	m.Unique = value
}

func (m *Shard) SetRuleGroups(values ...string) {
	m.RuleGroups = values
}

func (m *Shard) SetReplicas(replicas []Replica) {
	m.Replicas = replicas
}

// Clone clones the shard returns the pointer
func (m *Shard) Clone() *Shard {
	value := &Shard{}
	protoc.MustUnmarshal(value, protoc.MustMarshal(m))
	return value
}

func NewStore() *Store {
	return &Store{}
}

// Clone clones the shard returns the pointer
func (m *Store) Clone() *Store {
	value := &Store{}
	protoc.MustUnmarshal(value, protoc.MustMarshal(m))
	return value
}

func (m *Store) GetVersionAndGitHash() (string, string) {
	return m.Version, m.CommitID
}

func (m *Store) SetID(storeID uint64) {
	m.ID = storeID
}

func (m *Store) SetLabels(labels []Label) {
	m.Labels = labels
}

func (m *Store) SetAddrs(clientAddr, raftAddr string) {
	m.ClientAddress = clientAddr
	m.RaftAddress = raftAddr
}

func (m *Store) SetStartTime(value int64) {
	m.StartTime = value
}

func (m *Store) SetVersionAndCommitID(version, commitID string) {
	m.Version = version
	m.CommitID = commitID
}

func (m *Store) SetDeployPath(value string) {
	m.DeployPath = value
}

func (m *Store) SetDestroyed(value bool) {
	m.Destroyed = value
}

func (m *Store) SetState(value StoreState) {
	m.State = value
}

func (m *Store) SetLastHeartbeat(value int64) {
	m.LastHeartbeatTime = value
}

// ContainsKey returns true if the shard contains the key
func (m *Shard) ContainsKey(key []byte) bool {
	return (len(m.Start) == 0 || bytes.Compare(key, m.Start) >= 0) &&
		(len(m.End) == 0 || bytes.Compare(key, m.End) < 0)
}

// MinEnd returns min(end, shard.end)
func (m *Shard) MinEnd(end []byte) []byte {
	if len(m.End) > 0 && bytes.Compare(end, m.End) < 0 {
		return end
	}
	return m.End
}
