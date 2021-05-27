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
