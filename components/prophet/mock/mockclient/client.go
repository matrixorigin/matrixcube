// Code generated by MockGen. DO NOT EDIT.
// Source: ../client.go

// Package mockclient is a generated GoMock package.
package mockclient

import (
	reflect "reflect"

	roaring64 "github.com/RoaringBitmap/roaring/roaring64"
	gomock "github.com/golang/mock/gomock"
	prophet "github.com/matrixorigin/matrixcube/components/prophet"
	metadata "github.com/matrixorigin/matrixcube/components/prophet/metadata"
	metapb "github.com/matrixorigin/matrixcube/pb/metapb"
	rpcpb "github.com/matrixorigin/matrixcube/pb/rpcpb"
)

// MockClient is a mock of Client interface.
type MockClient struct {
	ctrl     *gomock.Controller
	recorder *MockClientMockRecorder
}

// MockClientMockRecorder is the mock recorder for MockClient.
type MockClientMockRecorder struct {
	mock *MockClient
}

// NewMockClient creates a new mock instance.
func NewMockClient(ctrl *gomock.Controller) *MockClient {
	mock := &MockClient{ctrl: ctrl}
	mock.recorder = &MockClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClient) EXPECT() *MockClientMockRecorder {
	return m.recorder
}

// AddSchedulingRule mocks base method.
func (m *MockClient) AddSchedulingRule(group uint64, ruleName, groupByLabel string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddSchedulingRule", group, ruleName, groupByLabel)
	ret0, _ := ret[0].(error)
	return ret0
}

// AddSchedulingRule indicates an expected call of AddSchedulingRule.
func (mr *MockClientMockRecorder) AddSchedulingRule(group, ruleName, groupByLabel interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddSchedulingRule", reflect.TypeOf((*MockClient)(nil).AddSchedulingRule), group, ruleName, groupByLabel)
}

// AllocID mocks base method.
func (m *MockClient) AllocID() (uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AllocID")
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AllocID indicates an expected call of AllocID.
func (mr *MockClientMockRecorder) AllocID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AllocID", reflect.TypeOf((*MockClient)(nil).AllocID))
}

// AskBatchSplit mocks base method.
func (m *MockClient) AskBatchSplit(res *metadata.Shard, count uint32) ([]rpcpb.SplitID, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AskBatchSplit", res, count)
	ret0, _ := ret[0].([]rpcpb.SplitID)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AskBatchSplit indicates an expected call of AskBatchSplit.
func (mr *MockClientMockRecorder) AskBatchSplit(res, count interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AskBatchSplit", reflect.TypeOf((*MockClient)(nil).AskBatchSplit), res, count)
}

// AsyncAddShards mocks base method.
func (m *MockClient) AsyncAddShards(resources ...*metadata.Shard) error {
	m.ctrl.T.Helper()
	varargs := []interface{}{}
	for _, a := range resources {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "AsyncAddShards", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// AsyncAddShards indicates an expected call of AsyncAddShards.
func (mr *MockClientMockRecorder) AsyncAddShards(resources ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AsyncAddShards", reflect.TypeOf((*MockClient)(nil).AsyncAddShards), resources...)
}

// AsyncAddShardsWithLeastPeers mocks base method.
func (m *MockClient) AsyncAddShardsWithLeastPeers(resources []*metadata.Shard, leastPeers []int) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AsyncAddShardsWithLeastPeers", resources, leastPeers)
	ret0, _ := ret[0].(error)
	return ret0
}

// AsyncAddShardsWithLeastPeers indicates an expected call of AsyncAddShardsWithLeastPeers.
func (mr *MockClientMockRecorder) AsyncAddShardsWithLeastPeers(resources, leastPeers interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AsyncAddShardsWithLeastPeers", reflect.TypeOf((*MockClient)(nil).AsyncAddShardsWithLeastPeers), resources, leastPeers)
}

// AsyncRemoveShards mocks base method.
func (m *MockClient) AsyncRemoveShards(ids ...uint64) error {
	m.ctrl.T.Helper()
	varargs := []interface{}{}
	for _, a := range ids {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "AsyncRemoveShards", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// AsyncRemoveShards indicates an expected call of AsyncRemoveShards.
func (mr *MockClientMockRecorder) AsyncRemoveShards(ids ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AsyncRemoveShards", reflect.TypeOf((*MockClient)(nil).AsyncRemoveShards), ids...)
}

// CheckShardState mocks base method.
func (m *MockClient) CheckShardState(resources *roaring64.Bitmap) (rpcpb.CheckShardStateRsp, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CheckShardState", resources)
	ret0, _ := ret[0].(rpcpb.CheckShardStateRsp)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CheckShardState indicates an expected call of CheckShardState.
func (mr *MockClientMockRecorder) CheckShardState(resources interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CheckShardState", reflect.TypeOf((*MockClient)(nil).CheckShardState), resources)
}

// Close mocks base method.
func (m *MockClient) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockClientMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockClient)(nil).Close))
}

// StoreHeartbeat mocks base method.
func (m *MockClient) StoreHeartbeat(hb rpcpb.StoreHeartbeatReq) (rpcpb.StoreHeartbeatRsp, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StoreHeartbeat", hb)
	ret0, _ := ret[0].(rpcpb.StoreHeartbeatRsp)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StoreHeartbeat indicates an expected call of StoreHeartbeat.
func (mr *MockClientMockRecorder) StoreHeartbeat(hb interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StoreHeartbeat", reflect.TypeOf((*MockClient)(nil).StoreHeartbeat), hb)
}

// CreateDestroying mocks base method.
func (m *MockClient) CreateDestroying(id, index uint64, removeData bool, replicas []uint64) (metapb.ShardState, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateDestroying", id, index, removeData, replicas)
	ret0, _ := ret[0].(metapb.ShardState)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateDestroying indicates an expected call of CreateDestroying.
func (mr *MockClientMockRecorder) CreateDestroying(id, index, removeData, replicas interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateDestroying", reflect.TypeOf((*MockClient)(nil).CreateDestroying), id, index, removeData, replicas)
}

// CreateJob mocks base method.
func (m *MockClient) CreateJob(arg0 metapb.Job) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateJob", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// CreateJob indicates an expected call of CreateJob.
func (mr *MockClientMockRecorder) CreateJob(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateJob", reflect.TypeOf((*MockClient)(nil).CreateJob), arg0)
}

// ExecuteJob mocks base method.
func (m *MockClient) ExecuteJob(arg0 metapb.Job, arg1 []byte) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ExecuteJob", arg0, arg1)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ExecuteJob indicates an expected call of ExecuteJob.
func (mr *MockClientMockRecorder) ExecuteJob(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecuteJob", reflect.TypeOf((*MockClient)(nil).ExecuteJob), arg0, arg1)
}

// GetAppliedRules mocks base method.
func (m *MockClient) GetAppliedRules(id uint64) ([]rpcpb.PlacementRule, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAppliedRules", id)
	ret0, _ := ret[0].([]rpcpb.PlacementRule)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAppliedRules indicates an expected call of GetAppliedRules.
func (mr *MockClientMockRecorder) GetAppliedRules(id interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAppliedRules", reflect.TypeOf((*MockClient)(nil).GetAppliedRules), id)
}

// GetStore mocks base method.
func (m *MockClient) GetStore(containerID uint64) (*metadata.Store, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetStore", containerID)
	ret0, _ := ret[0].(*metadata.Store)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetStore indicates an expected call of GetStore.
func (mr *MockClientMockRecorder) GetStore(containerID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetStore", reflect.TypeOf((*MockClient)(nil).GetStore), containerID)
}

// GetDestroying mocks base method.
func (m *MockClient) GetDestroying(id uint64) (*metapb.DestroyingStatus, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetDestroying", id)
	ret0, _ := ret[0].(*metapb.DestroyingStatus)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetDestroying indicates an expected call of GetDestroying.
func (mr *MockClientMockRecorder) GetDestroying(id interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDestroying", reflect.TypeOf((*MockClient)(nil).GetDestroying), id)
}

// GetShardHeartbeatRspNotifier mocks base method.
func (m *MockClient) GetShardHeartbeatRspNotifier() (chan rpcpb.ShardHeartbeatRsp, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetShardHeartbeatRspNotifier")
	ret0, _ := ret[0].(chan rpcpb.ShardHeartbeatRsp)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetShardHeartbeatRspNotifier indicates an expected call of GetShardHeartbeatRspNotifier.
func (mr *MockClientMockRecorder) GetShardHeartbeatRspNotifier() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetShardHeartbeatRspNotifier", reflect.TypeOf((*MockClient)(nil).GetShardHeartbeatRspNotifier))
}

// GetSchedulingRules mocks base method.
func (m *MockClient) GetSchedulingRules() ([]metapb.ScheduleGroupRule, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSchedulingRules")
	ret0, _ := ret[0].([]metapb.ScheduleGroupRule)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSchedulingRules indicates an expected call of GetSchedulingRules.
func (mr *MockClientMockRecorder) GetSchedulingRules() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSchedulingRules", reflect.TypeOf((*MockClient)(nil).GetSchedulingRules))
}

// NewWatcher mocks base method.
func (m *MockClient) NewWatcher(flag uint32) (prophet.Watcher, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewWatcher", flag)
	ret0, _ := ret[0].(prophet.Watcher)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NewWatcher indicates an expected call of NewWatcher.
func (mr *MockClientMockRecorder) NewWatcher(flag interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewWatcher", reflect.TypeOf((*MockClient)(nil).NewWatcher), flag)
}

// PutStore mocks base method.
func (m *MockClient) PutStore(container *metadata.Store) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PutStore", container)
	ret0, _ := ret[0].(error)
	return ret0
}

// PutStore indicates an expected call of PutStore.
func (mr *MockClientMockRecorder) PutStore(container interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PutStore", reflect.TypeOf((*MockClient)(nil).PutStore), container)
}

// PutPlacementRule mocks base method.
func (m *MockClient) PutPlacementRule(rule rpcpb.PlacementRule) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PutPlacementRule", rule)
	ret0, _ := ret[0].(error)
	return ret0
}

// PutPlacementRule indicates an expected call of PutPlacementRule.
func (mr *MockClientMockRecorder) PutPlacementRule(rule interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PutPlacementRule", reflect.TypeOf((*MockClient)(nil).PutPlacementRule), rule)
}

// RemoveJob mocks base method.
func (m *MockClient) RemoveJob(arg0 metapb.Job) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemoveJob", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// RemoveJob indicates an expected call of RemoveJob.
func (mr *MockClientMockRecorder) RemoveJob(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemoveJob", reflect.TypeOf((*MockClient)(nil).RemoveJob), arg0)
}

// ReportDestroyed mocks base method.
func (m *MockClient) ReportDestroyed(id, replicaID uint64) (metapb.ShardState, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReportDestroyed", id, replicaID)
	ret0, _ := ret[0].(metapb.ShardState)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReportDestroyed indicates an expected call of ReportDestroyed.
func (mr *MockClientMockRecorder) ReportDestroyed(id, replicaID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReportDestroyed", reflect.TypeOf((*MockClient)(nil).ReportDestroyed), id, replicaID)
}

// ShardHeartbeat mocks base method.
func (m *MockClient) ShardHeartbeat(meta *metadata.Shard, hb rpcpb.ShardHeartbeatReq) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ShardHeartbeat", meta, hb)
	ret0, _ := ret[0].(error)
	return ret0
}

// ShardHeartbeat indicates an expected call of ShardHeartbeat.
func (mr *MockClientMockRecorder) ShardHeartbeat(meta, hb interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ShardHeartbeat", reflect.TypeOf((*MockClient)(nil).ShardHeartbeat), meta, hb)
}
