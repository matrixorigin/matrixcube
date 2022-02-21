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
	"encoding/json"
	"errors"

	"github.com/matrixorigin/matrixcube/pb/metapb"
)

var (
	// TestShardFactory test factory
	TestShardFactory = func() Shard {
		return &TestShard{}
	}
)

type TestShard struct {
	ResID         uint64               `json:"id"`
	ResGroup      uint64               `json:"group"`
	Version       uint64               `json:"version"`
	ResPeers      []metapb.Replica     `json:"peers"`
	ResLabels     []metapb.Pair        `json:"labels"`
	Start         []byte               `json:"start"`
	End           []byte               `json:"end"`
	ResEpoch      metapb.ShardEpoch `json:"epoch"`
	ResState      metapb.ShardState `json:"state"`
	ResUnique     string               `json:"unique"`
	ResData       []byte               `json:"data"`
	ResRuleGroups []string             `json:"rule-groups"`
	Err           bool                 `json:"-"`
}

func NewTestShard(id uint64) *TestShard {
	return &TestShard{ResID: id}
}

func (res *TestShard) State() metapb.ShardState {
	return res.ResState
}

func (res *TestShard) SetState(state metapb.ShardState) {
	res.ResState = state
}

// ID mock
func (res *TestShard) ID() uint64 {
	return res.ResID
}

// SetID mock
func (res *TestShard) SetID(id uint64) {
	res.ResID = id
}

func (res *TestShard) Group() uint64 {
	return res.ResGroup
}

// SetGroup set raft group
func (res *TestShard) SetGroup(group uint64) {
	res.ResGroup = group
}

// Peers mock
func (res *TestShard) Peers() []metapb.Replica {
	return res.ResPeers
}

// SetPeers mock
func (res *TestShard) SetPeers(peers []metapb.Replica) {
	res.ResPeers = peers
}

// Range mock
func (res *TestShard) Range() ([]byte, []byte) {
	return []byte(res.Start), []byte(res.End)
}

// SetStartKey mock
func (res *TestShard) SetStartKey(value []byte) {
	res.Start = value
}

// SetEndKey mock
func (res *TestShard) SetEndKey(value []byte) {
	res.End = value
}

// Epoch mock
func (res *TestShard) Epoch() metapb.ShardEpoch {
	return res.ResEpoch
}

// SetEpoch mock
func (res *TestShard) SetEpoch(value metapb.ShardEpoch) {
	res.ResEpoch = value
}

// Data mock
func (res *TestShard) Data() []byte {
	return res.ResData
}

// SetData mock
func (res *TestShard) SetData(data []byte) {
	res.ResData = data
}

// Labels mock
func (res *TestShard) Labels() []metapb.Pair {
	return res.ResLabels
}

// SetLabels mock
func (res *TestShard) SetLabels(labels []metapb.Pair) {
	res.ResLabels = labels
}

// Unique mock
func (res *TestShard) Unique() string {
	return res.ResUnique
}

// SetUnique mock
func (res *TestShard) SetUnique(value string) {
	res.ResUnique = value
}

// RuleGroups mock
func (res *TestShard) RuleGroups() []string {
	return res.ResRuleGroups
}

// SetRuleGroups mock
func (res *TestShard) SetRuleGroups(ruleGroups ...string) {
	res.ResRuleGroups = ruleGroups
}

// Clone mock
func (res *TestShard) Clone() Shard {
	data, _ := res.Marshal()
	value := NewTestShard(res.ResID)
	value.Unmarshal(data)
	return value
}

// ScaleCompleted mock
func (res *TestShard) ScaleCompleted(uint64) bool {
	return false
}

// Marshal mock
func (res *TestShard) Marshal() ([]byte, error) {
	if res.Err {
		return nil, errors.New("test error")
	}

	return json.Marshal(res)
}

// Unmarshal mock
func (res *TestShard) Unmarshal(data []byte) error {
	if res.Err {
		return errors.New("test error")
	}

	return json.Unmarshal(data, res)
}

// SupportRebalance mock
func (res *TestShard) SupportRebalance() bool {
	return true
}

// SupportTransferLeader mock
func (res *TestShard) SupportTransferLeader() bool {
	return true
}

// TestStore mock
type TestStore struct {
	CID                  uint64        `json:"cid"`
	CAddr                string        `json:"addr"`
	CShardAddr           string        `json:"shardAddr"`
	CLabels              []metapb.Pair `json:"labels"`
	StartTS              int64         `json:"startTS"`
	CLastHeartbeat       int64         `json:"lastHeartbeat"`
	CVerion              string        `json:"version"`
	CGitHash             string        `json:"gitHash"`
	CDeployPath          string        `json:"deployPath"`
	CPhysicallyDestroyed bool          `json:"physicallyDestroyed"`

	CState  metapb.StoreState `json:"state"`
	CAction metapb.Action         `json:"action"`
}

// NewTestStore mock
func NewTestStore(id uint64) *TestStore {
	return &TestStore{CID: id}
}

// SetAddrs mock
func (c *TestStore) SetAddrs(addr, shardAddr string) {
	c.CAddr = addr
	c.CShardAddr = shardAddr
}

// Addr mock
func (c *TestStore) Addr() string {
	return c.CAddr
}

// ShardAddr mock
func (c *TestStore) ShardAddr() string {
	return c.CShardAddr
}

// SetID mock
func (c *TestStore) SetID(id uint64) {
	c.CID = id
}

// ID mock
func (c *TestStore) ID() uint64 {
	return c.CID
}

// Labels mock
func (c *TestStore) Labels() []metapb.Pair {
	return c.CLabels
}

// SetLabels mock
func (c *TestStore) SetLabels(labels []metapb.Pair) {
	c.CLabels = labels
}

// StartTimestamp mock
func (c *TestStore) StartTimestamp() int64 {
	return c.StartTS
}

// SetStartTimestamp mock
func (c *TestStore) SetStartTimestamp(startTS int64) {
	c.StartTS = startTS
}

// LastHeartbeat mock
func (c *TestStore) LastHeartbeat() int64 {
	return c.CLastHeartbeat
}

//SetLastHeartbeat mock.
func (c *TestStore) SetLastHeartbeat(value int64) {
	c.CLastHeartbeat = value
}

// Version returns version and githash
func (c *TestStore) Version() (string, string) {
	return c.CVerion, c.CGitHash
}

// SetVersion set version
func (c *TestStore) SetVersion(version string, githash string) {
	c.CVerion = version
	c.CGitHash = githash
}

// DeployPath returns the container deploy path
func (c *TestStore) DeployPath() string {
	return c.CDeployPath
}

// SetDeployPath set deploy path
func (c *TestStore) SetDeployPath(value string) {
	c.CDeployPath = value
}

// State mock
func (c *TestStore) State() metapb.StoreState {
	return c.CState
}

// SetState mock
func (c *TestStore) SetState(state metapb.StoreState) {
	c.CState = state
}

// PhysicallyDestroyed mock
func (c *TestStore) PhysicallyDestroyed() bool {
	return c.CPhysicallyDestroyed
}

// SetPhysicallyDestroyed mock
func (c *TestStore) SetPhysicallyDestroyed(v bool) {
	c.CPhysicallyDestroyed = v
}

// Clone mock
func (c *TestStore) Clone() Store {
	value := NewTestStore(c.CID)
	data, _ := c.Marshal()
	value.Unmarshal(data)
	return value
}

// Marshal mock
func (c *TestStore) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

// Unmarshal mock
func (c *TestStore) Unmarshal(data []byte) error {
	return json.Unmarshal(data, c)
}
