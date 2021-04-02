package statistics

import (
	"fmt"
)

const (
	// ContainerHeartBeatReportInterval is the heartbeat report interval of a container.
	ContainerHeartBeatReportInterval = 10
	// ResourceHeartBeatReportInterval is the heartbeat report interval of a resource.
	ResourceHeartBeatReportInterval = 60
)

func containerTag(id uint64) string {
	return fmt.Sprintf("container-%d", id)
}
