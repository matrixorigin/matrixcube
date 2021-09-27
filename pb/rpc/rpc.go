package rpc

// IsAdmin returns true if has a admin request
func (m *RequestBatch) IsAdmin() bool {
	return len(m.Requests) == 0
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
