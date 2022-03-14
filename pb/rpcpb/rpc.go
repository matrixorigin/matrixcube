package rpcpb

import (
	"github.com/fagongzi/util/protoc"
)

// IsAdmin returns true if has a admin request
func (m *RequestBatch) IsAdmin() bool {
	return len(m.Requests) == 1 && m.Requests[0].Type == Admin
}

// GetAdminCmdType returns the admin cmd type
func (m *RequestBatch) GetAdminCmdType() AdminCmdType {
	return AdminCmdType(m.Requests[0].CustomType)
}

// GetAdminRequest returns the admin request
func (m *RequestBatch) GetAdminRequest() Request {
	return m.Requests[0]
}

// GetConfigChangeRequest return ConfigChangeRequest request
func (m *RequestBatch) GetConfigChangeRequest() ConfigChangeRequest {
	var req ConfigChangeRequest
	protoc.MustUnmarshal(&req, m.GetAdminRequest().Cmd)
	return req
}

// GetCompactLogRequest return CompactLogRequest request
func (m *RequestBatch) GetCompactLogRequest() CompactLogRequest {
	var req CompactLogRequest
	protoc.MustUnmarshal(&req, m.GetAdminRequest().Cmd)
	return req
}

// GetBatchSplitRequest return BatchSplitRequest request
func (m *RequestBatch) GetBatchSplitRequest() BatchSplitRequest {
	var req BatchSplitRequest
	protoc.MustUnmarshal(&req, m.GetAdminRequest().Cmd)
	return req
}

// GetUpdateMetadataRequest return UpdateMetadataRequest request
func (m *RequestBatch) GetUpdateMetadataRequest() UpdateMetadataRequest {
	var req UpdateMetadataRequest
	protoc.MustUnmarshal(&req, m.GetAdminRequest().Cmd)
	return req
}

// GetTransferLeaderRequest return TransferLeaderRequest request
func (m *RequestBatch) GetTransferLeaderRequest() TransferLeaderRequest {
	var req TransferLeaderRequest
	protoc.MustUnmarshal(&req, m.GetAdminRequest().Cmd)
	return req
}

// GetUpdateLabelsRequest return UpdateLabelsRequest request
func (m *RequestBatch) GetUpdateLabelsRequest() UpdateLabelsRequest {
	var req UpdateLabelsRequest
	protoc.MustUnmarshal(&req, m.GetAdminRequest().Cmd)
	return req
}

// IsEmpty returns true if is a empty batch
func (m *RequestBatch) IsEmpty() bool {
	return len(m.Header.ID) == 0
}

// IsEmpty returns true if is a empty header
func (m *ResponseBatchHeader) IsEmpty() bool {
	return m.Error.Message == ""
}

// IsEmpty returns true if is a empty header
func (m *RequestBatchHeader) IsEmpty() bool {
	return m.ShardID == 0
}

// IsAdmin returns true if has a admin request
func (m *ResponseBatch) IsAdmin() bool {
	return len(m.Responses) == 1 && m.Responses[0].Type == Admin
}

// GetAdminCmdType returns the admin cmd type
func (m *ResponseBatch) GetAdminCmdType() AdminCmdType {
	return AdminCmdType(m.Responses[0].CustomType)
}

// GetAdminResponse returns the admin Response
func (m *ResponseBatch) GetAdminResponse() Response {
	return m.Responses[0]
}

// GetConfigChangeResponse return ConfigChangeResponse Response
func (m *ResponseBatch) GetConfigChangeResponse() ConfigChangeResponse {
	var req ConfigChangeResponse
	protoc.MustUnmarshal(&req, m.GetAdminResponse().Value)
	return req
}

// GetCompactLogResponse return CompactLogResponse Response
func (m *ResponseBatch) GetCompactLogResponse() CompactLogResponse {
	var req CompactLogResponse
	protoc.MustUnmarshal(&req, m.GetAdminResponse().Value)
	return req
}

// GetBatchSplitResponse return BatchSplitResponse Response
func (m *ResponseBatch) GetBatchSplitResponse() BatchSplitResponse {
	var req BatchSplitResponse
	protoc.MustUnmarshal(&req, m.GetAdminResponse().Value)
	return req
}

// GetUpdateMetadataResponse return UpdateMetadataResponse Response
func (m *ResponseBatch) GetUpdateMetadataResponse() UpdateMetadataResponse {
	var req UpdateMetadataResponse
	protoc.MustUnmarshal(&req, m.GetAdminResponse().Value)
	return req
}

// GetUpdateLabelsResponse return UpdateLabelsResponse Response
func (m *ResponseBatch) GetUpdateLabelsResponse() UpdateLabelsResponse {
	var req UpdateLabelsResponse
	protoc.MustUnmarshal(&req, m.GetAdminResponse().Value)
	return req
}

// GetTransferLeaderResponse return TransferLeaderResponse Response
func (m *ResponseBatch) GetTransferLeaderResponse() TransferLeaderResponse {
	var req TransferLeaderResponse
	protoc.MustUnmarshal(&req, m.GetAdminResponse().Value)
	return req
}
