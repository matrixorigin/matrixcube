package raftstore

import "github.com/fagongzi/goetty"

const (
	attrBuf             = "internal.temp.buf"
	attrRequestsTotal   = "internal.batch.requests.total"
	attrRequestsCurrent = "internal.batch.requests.current"
)

// GetBuf returns byte buffer from attr
func GetBuf(attrs map[string]interface{}) *goetty.ByteBuf {
	if v, ok := attrs[attrBuf]; ok {
		return v.(*goetty.ByteBuf)
	}

	return nil
}

// IsFirstApplyRequest returns true if the current request is first in this apply batch
func IsFirstApplyRequest(attrs map[string]interface{}) bool {
	if value, ok := attrs[attrRequestsCurrent]; ok {
		return value.(int) == 0
	}

	return false
}

// IsLastApplyRequest returns true if the last request is first in this apply batch
func IsLastApplyRequest(attrs map[string]interface{}) bool {
	current, ok := attrs[attrRequestsCurrent]
	if !ok {
		return false
	}

	total, ok := attrs[attrRequestsTotal]
	if !ok {
		return false
	}

	return current == total
}
