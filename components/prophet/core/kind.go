package core

import (
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
)

// PriorityLevel lower level means higher priority
type PriorityLevel int

// Built-in priority level
const (
	LowPriority PriorityLevel = iota
	NormalPriority
	HighPriority
)

// ScheduleKind distinguishes resources and schedule policy.
type ScheduleKind struct {
	ResourceKind metapb.ResourceKind
	Policy       SchedulePolicy
}

// NewScheduleKind creates a schedule kind with resource kind and schedule policy.
func NewScheduleKind(kind metapb.ResourceKind, Policy SchedulePolicy) ScheduleKind {
	return ScheduleKind{
		ResourceKind: kind,
		Policy:       Policy,
	}
}

// SchedulePolicy distinguishes different kinds of schedule policies.
type SchedulePolicy int

const (
	// ByCount indicates that balance by count
	ByCount SchedulePolicy = iota
	// BySize indicates that balance by size
	BySize
)

func (k SchedulePolicy) String() string {
	switch k {
	case ByCount:
		return "count"
	case BySize:
		return "size"
	default:
		return "unknown"
	}
}

// StringToSchedulePolicy creates a schedule policy with string.
func StringToSchedulePolicy(input string) SchedulePolicy {
	switch input {
	case BySize.String():
		return BySize
	case ByCount.String():
		return ByCount
	default:
		panic("invalid schedule policy: " + input)
	}
}

// KeyType distinguishes different kinds of key types
type KeyType int

const (
	// Table indicates that the key is table key
	Table KeyType = iota
	// Raw indicates that the key is raw key.
	Raw
	// Txn indicates that the key is txn key.
	Txn
)

func (k KeyType) String() string {
	switch k {
	case Table:
		return "table"
	case Raw:
		return "raw"
	case Txn:
		return "txn"
	default:
		return "unknown"
	}
}

// StringToKeyType creates a key type with string.
func StringToKeyType(input string) KeyType {
	switch input {
	case Table.String():
		return Table
	case Raw.String():
		return Raw
	case Txn.String():
		return Txn
	default:
		panic("invalid key type: " + input)
	}
}
