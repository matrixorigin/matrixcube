// Copyright 2020 PingCAP, Inc.
// Modifications copyright (C) 2021 MatrixOrigin.
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

package statistics

// StoreStatKind represents the statistics type of Store.
type StoreStatKind int

// Different Store statistics kinds.
const (
	StoreReadBytes StoreStatKind = iota
	StoreReadKeys
	StoreWriteBytes
	StoreWriteKeys
	StoreCPUUsage
	StoreDiskReadRate
	StoreDiskWriteRate

	StoreStatCount
)

func (k StoreStatKind) String() string {
	switch k {
	case StoreReadBytes:
		return "container_read_bytes"
	case StoreReadKeys:
		return "container_read_keys"
	case StoreWriteBytes:
		return "container_write_bytes"
	case StoreWriteKeys:
		return "container_write_keys"
	case StoreCPUUsage:
		return "container_cpu_usage"
	case StoreDiskReadRate:
		return "container_disk_read_rate"
	case StoreDiskWriteRate:
		return "container_disk_write_rate"
	}

	return "unknown containerStatKind"
}
