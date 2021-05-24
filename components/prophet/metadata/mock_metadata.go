package metadata

import (
	"encoding/json"
	"errors"

	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
)

var (
	// TestResourceFactory test factory
	TestResourceFactory = func() Resource {
		return &TestResource{}
	}
)

// TestResource test resource
type TestResource struct {
	ResID     uint64               `json:"id"`
	Version   uint64               `json:"version"`
	ResPeers  []metapb.Peer        `json:"peers"`
	ResLabels []metapb.Pair        `json:"labels"`
	Start     []byte               `json:"start"`
	End       []byte               `json:"end"`
	ResEpoch  metapb.ResourceEpoch `json:"epoch"`
	ResState  metapb.ResourceState `json:"state"`
	ResUnique string               `json:"unique"`
	Err       bool                 `json:"-"`
}

// NewTestResource create test resource
func NewTestResource(id uint64) *TestResource {
	return &TestResource{ResID: id}
}

// State resource state
func (res *TestResource) State() metapb.ResourceState {
	return res.ResState
}

// SetState set resource state
func (res *TestResource) SetState(state metapb.ResourceState) {
	res.ResState = state
}

// SetID mock
func (res *TestResource) SetID(id uint64) {
	res.ResID = id
}

// ID mock
func (res *TestResource) ID() uint64 {
	return res.ResID
}

// Peers mock
func (res *TestResource) Peers() []metapb.Peer {
	return res.ResPeers
}

// SetPeers mock
func (res *TestResource) SetPeers(peers []metapb.Peer) {
	res.ResPeers = peers
}

// Range mock
func (res *TestResource) Range() ([]byte, []byte) {
	return []byte(res.Start), []byte(res.End)
}

// SetStartKey mock
func (res *TestResource) SetStartKey(value []byte) {
	res.Start = value
}

// SetEndKey mock
func (res *TestResource) SetEndKey(value []byte) {
	res.End = value
}

// Epoch mock
func (res *TestResource) Epoch() metapb.ResourceEpoch {
	return res.ResEpoch
}

// SetEpoch mock
func (res *TestResource) SetEpoch(value metapb.ResourceEpoch) {
	res.ResEpoch = value
}

// Stale mock
func (res *TestResource) Stale(other Resource) bool {
	return res.Version > other.(*TestResource).Version
}

// Changed mock
func (res *TestResource) Changed(other Resource) bool {
	return res.Version < other.(*TestResource).Version
}

// Labels mock
func (res *TestResource) Labels() []metapb.Pair {
	return res.ResLabels
}

func (res *TestResource) Unique() string {
	return res.ResUnique
}

func (res *TestResource) SetUnique(value string) {
	res.ResUnique = value
}

// Clone mock
func (res *TestResource) Clone() Resource {
	data, _ := res.Marshal()
	value := NewTestResource(res.ResID)
	value.Unmarshal(data)
	return value
}

// ScaleCompleted mock
func (res *TestResource) ScaleCompleted(uint64) bool {
	return false
}

// Marshal mock
func (res *TestResource) Marshal() ([]byte, error) {
	if res.Err {
		return nil, errors.New("test error")
	}

	return json.Marshal(res)
}

// Unmarshal mock
func (res *TestResource) Unmarshal(data []byte) error {
	if res.Err {
		return errors.New("test error")
	}

	return json.Unmarshal(data, res)
}

// SupportRebalance mock
func (res *TestResource) SupportRebalance() bool {
	return true
}

// SupportTransferLeader mock
func (res *TestResource) SupportTransferLeader() bool {
	return true
}

// TestContainer mock
type TestContainer struct {
	CID            uint64        `json:"cid"`
	CAddr          string        `json:"addr"`
	CShardAddr     string        `json:"shardAddr"`
	CLabels        []metapb.Pair `json:"labels"`
	StartTS        int64         `json:"startTS"`
	CLastHeartbeat int64         `json:"lastHeartbeat"`
	CVerion        string        `json:"version"`
	CGitHash       string        `json:"gitHash"`
	CDeployPath    string        `json:"deployPath"`

	CState  metapb.ContainerState `json:"state"`
	CAction metapb.Action         `json:"action"`
}

// NewTestContainer mock
func NewTestContainer(id uint64) *TestContainer {
	return &TestContainer{CID: id}
}

// SetAddrs mock
func (c *TestContainer) SetAddrs(addr, shardAddr string) {
	c.CAddr = addr
	c.CShardAddr = shardAddr
}

// Addr mock
func (c *TestContainer) Addr() string {
	return c.CAddr
}

// ShardAddr mock
func (c *TestContainer) ShardAddr() string {
	return c.CShardAddr
}

// SetID mock
func (c *TestContainer) SetID(id uint64) {
	c.CID = id
}

// ID mock
func (c *TestContainer) ID() uint64 {
	return c.CID
}

// Labels mock
func (c *TestContainer) Labels() []metapb.Pair {
	return c.CLabels
}

// SetLabels mock
func (c *TestContainer) SetLabels(labels []metapb.Pair) {
	c.CLabels = labels
}

// StartTimestamp mock
func (c *TestContainer) StartTimestamp() int64 {
	return c.StartTS
}

// SetStartTimestamp mock
func (c *TestContainer) SetStartTimestamp(startTS int64) {
	c.StartTS = startTS
}

// LastHeartbeat mock
func (c *TestContainer) LastHeartbeat() int64 {
	return c.CLastHeartbeat
}

//SetLastHeartbeat mock.
func (c *TestContainer) SetLastHeartbeat(value int64) {
	c.CLastHeartbeat = value
}

// Version returns version and githash
func (c *TestContainer) Version() (string, string) {
	return c.CVerion, c.CGitHash
}

// SetVersion set version
func (c *TestContainer) SetVersion(version string, githash string) {
	c.CVerion = version
	c.CGitHash = githash
}

// DeployPath returns the container deploy path
func (c *TestContainer) DeployPath() string {
	return c.CDeployPath
}

// SetDeployPath set deploy path
func (c *TestContainer) SetDeployPath(value string) {
	c.CDeployPath = value
}

// State mock
func (c *TestContainer) State() metapb.ContainerState {
	return c.CState
}

// SetState mock
func (c *TestContainer) SetState(state metapb.ContainerState) {
	c.CState = state
}

// ActionOnJoinCluster mock
func (c *TestContainer) ActionOnJoinCluster() metapb.Action {
	return c.CAction
}

// Clone mock
func (c *TestContainer) Clone() Container {
	value := NewTestContainer(c.CID)
	data, _ := c.Marshal()
	value.Unmarshal(data)
	return value
}

// Marshal mock
func (c *TestContainer) Marshal() ([]byte, error) {
	return json.Marshal(c)
}

// Unmarshal mock
func (c *TestContainer) Unmarshal(data []byte) error {
	return json.Unmarshal(data, c)
}
