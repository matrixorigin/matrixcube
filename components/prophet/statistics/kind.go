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

// ContainerStatKind represents the statistics type of Container.
type ContainerStatKind int

// Different Container statistics kinds.
const (
	ContainerReadBytes ContainerStatKind = iota
	ContainerReadKeys
	ContainerWriteBytes
	ContainerWriteKeys
	ContainerCPUUsage
	ContainerDiskReadRate
	ContainerDiskWriteRate

	ContainerStatCount
)

func (k ContainerStatKind) String() string {
	switch k {
	case ContainerReadBytes:
		return "container_read_bytes"
	case ContainerReadKeys:
		return "container_read_keys"
	case ContainerWriteBytes:
		return "container_write_bytes"
	case ContainerWriteKeys:
		return "container_write_keys"
	case ContainerCPUUsage:
		return "container_cpu_usage"
	case ContainerDiskReadRate:
		return "container_disk_read_rate"
	case ContainerDiskWriteRate:
		return "container_disk_write_rate"
	}

	return "unknown containerStatKind"
}
