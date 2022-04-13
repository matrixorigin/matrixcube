// Copyright 2022 MatrixOrigin.
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

package hlcpb

import (
	"math"
	"time"
)

/*
// String returns the string representation of the HLC timestamp.
func (lhs Timestamp) String() string {
	panic("not implemented")
}
*/

// IsEmpty returns a boolean value indicating whether the current timestamp
// is an empty value.
func (lhs Timestamp) IsEmpty() bool {
	return lhs.PhysicalTime == 0 && lhs.LogicalTime == 0
}

// ToStdTime converts the HLC timestamp to a regular golang stdlib UTC
// timestamp. The logical time component of the HLC is lost after the
// conversion.
func (lhs Timestamp) ToStdTime() time.Time {
	return time.Unix(0, lhs.PhysicalTime).UTC()
}

// Equal returns a boolean value indicating whether the lhs timestamp equals
// to the rhs timestamp.
func (lhs Timestamp) Equal(rhs Timestamp) bool {
	return lhs.PhysicalTime == rhs.PhysicalTime &&
		lhs.LogicalTime == rhs.LogicalTime
}

// Less returns a boolean value indicating whether the lhs timestamp is less
// than the rhs timestamp value.
func (lhs Timestamp) Less(rhs Timestamp) bool {
	return lhs.PhysicalTime < rhs.PhysicalTime ||
		(lhs.PhysicalTime == rhs.PhysicalTime && lhs.LogicalTime < rhs.LogicalTime)
}

// Greater returns a boolean value indicating whether the lhs timestamp is
// greater than the rhs timestamp value.
func (lhs Timestamp) Greater(rhs Timestamp) bool {
	return lhs.PhysicalTime > rhs.PhysicalTime ||
		(lhs.PhysicalTime == rhs.PhysicalTime && lhs.LogicalTime > rhs.LogicalTime)
}

// LessEq returns a boolean value indicating whether the lhs timestamp is
// less than or equal to the rhs timestamp value.
func (lhs Timestamp) LessEq(rhs Timestamp) bool {
	return lhs.Less(rhs) || lhs.Equal(rhs)
}

// GreaterEq returns a boolean value indicating whether the lhs timestamp is
// greater than or equal to the rhs timestamp value.
func (lhs Timestamp) GreaterEq(rhs Timestamp) bool {
	return lhs.Greater(rhs) || lhs.Equal(rhs)
}

// Next returns the smallest timestamp that is greater than the current
// timestamp.
func (lhs Timestamp) Next() Timestamp {
	if lhs.LogicalTime == math.MaxUint32 {
		return Timestamp{PhysicalTime: lhs.PhysicalTime + 1}
	}

	return Timestamp{
		PhysicalTime: lhs.PhysicalTime,
		LogicalTime:  lhs.LogicalTime + 1,
	}
}

// Prev returns the smallest timestamp that is less than the current
// timestamp.
func (lhs Timestamp) Prev() Timestamp {
	if lhs.LogicalTime == 0 {
		return Timestamp{PhysicalTime: lhs.PhysicalTime - 1, LogicalTime: math.MaxUint32}
	}

	return Timestamp{
		PhysicalTime: lhs.PhysicalTime,
		LogicalTime:  lhs.LogicalTime - 1,
	}
}
