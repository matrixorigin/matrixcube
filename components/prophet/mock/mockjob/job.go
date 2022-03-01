// Code generated by MockGen. DO NOT EDIT.
// Source: ../config/config_job.go

// Package mockjob is a generated GoMock package.
package mockjob

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	config "github.com/matrixorigin/matrixcube/components/prophet/config"
	core "github.com/matrixorigin/matrixcube/components/prophet/core"
	storage "github.com/matrixorigin/matrixcube/components/prophet/storage"
	metapb "github.com/matrixorigin/matrixcube/pb/metapb"
)

// MockShardsAware is a mock of ShardsAware interface.
type MockShardsAware struct {
	ctrl     *gomock.Controller
	recorder *MockShardsAwareMockRecorder
}

// MockShardsAwareMockRecorder is the mock recorder for MockShardsAware.
type MockShardsAwareMockRecorder struct {
	mock *MockShardsAware
}

// NewMockShardsAware creates a new mock instance.
func NewMockShardsAware(ctrl *gomock.Controller) *MockShardsAware {
	mock := &MockShardsAware{ctrl: ctrl}
	mock.recorder = &MockShardsAwareMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockShardsAware) EXPECT() *MockShardsAwareMockRecorder {
	return m.recorder
}

// ForeachShards mocks base method.
func (m *MockShardsAware) ForeachShards(group uint64, fn func(metapb.Shard)) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ForeachShards", group, fn)
}

// ForeachShards indicates an expected call of ForeachShards.
func (mr *MockShardsAwareMockRecorder) ForeachShards(group, fn interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ForeachShards", reflect.TypeOf((*MockShardsAware)(nil).ForeachShards), group, fn)
}

// ForeachWaittingCreateShards mocks base method.
func (m *MockShardsAware) ForeachWaittingCreateShards(do func(metapb.Shard)) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ForeachWaittingCreateShards", do)
}

// ForeachWaittingCreateShards indicates an expected call of ForeachWaittingCreateShards.
func (mr *MockShardsAwareMockRecorder) ForeachWaittingCreateShards(do interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ForeachWaittingCreateShards", reflect.TypeOf((*MockShardsAware)(nil).ForeachWaittingCreateShards), do)
}

// GetShard mocks base method.
func (m *MockShardsAware) GetShard(resourceID uint64) *core.CachedShard {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetShard", resourceID)
	ret0, _ := ret[0].(*core.CachedShard)
	return ret0
}

// GetShard indicates an expected call of GetShard.
func (mr *MockShardsAwareMockRecorder) GetShard(resourceID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetShard", reflect.TypeOf((*MockShardsAware)(nil).GetShard), resourceID)
}

// MockJobProcessor is a mock of JobProcessor interface.
type MockJobProcessor struct {
	ctrl     *gomock.Controller
	recorder *MockJobProcessorMockRecorder
}

// MockJobProcessorMockRecorder is the mock recorder for MockJobProcessor.
type MockJobProcessorMockRecorder struct {
	mock *MockJobProcessor
}

// NewMockJobProcessor creates a new mock instance.
func NewMockJobProcessor(ctrl *gomock.Controller) *MockJobProcessor {
	mock := &MockJobProcessor{ctrl: ctrl}
	mock.recorder = &MockJobProcessorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockJobProcessor) EXPECT() *MockJobProcessorMockRecorder {
	return m.recorder
}

// Execute mocks base method.
func (m *MockJobProcessor) Execute(arg0 []byte, arg1 storage.JobStorage, arg2 config.ShardsAware) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Execute", arg0, arg1, arg2)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Execute indicates an expected call of Execute.
func (mr *MockJobProcessorMockRecorder) Execute(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Execute", reflect.TypeOf((*MockJobProcessor)(nil).Execute), arg0, arg1, arg2)
}

// Remove mocks base method.
func (m *MockJobProcessor) Remove(arg0 metapb.Job, arg1 storage.JobStorage, arg2 config.ShardsAware) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Remove", arg0, arg1, arg2)
}

// Remove indicates an expected call of Remove.
func (mr *MockJobProcessorMockRecorder) Remove(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Remove", reflect.TypeOf((*MockJobProcessor)(nil).Remove), arg0, arg1, arg2)
}

// Start mocks base method.
func (m *MockJobProcessor) Start(arg0 metapb.Job, arg1 storage.JobStorage, arg2 config.ShardsAware) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Start", arg0, arg1, arg2)
}

// Start indicates an expected call of Start.
func (mr *MockJobProcessorMockRecorder) Start(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockJobProcessor)(nil).Start), arg0, arg1, arg2)
}

// Stop mocks base method.
func (m *MockJobProcessor) Stop(arg0 metapb.Job, arg1 storage.JobStorage, arg2 config.ShardsAware) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop", arg0, arg1, arg2)
}

// Stop indicates an expected call of Stop.
func (mr *MockJobProcessorMockRecorder) Stop(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockJobProcessor)(nil).Stop), arg0, arg1, arg2)
}
