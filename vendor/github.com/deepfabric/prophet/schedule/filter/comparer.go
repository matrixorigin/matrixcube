package filter

import (
	"github.com/deepfabric/prophet/config"
	"github.com/deepfabric/prophet/core"
)

// ContainerComparer compares 2 containers. Often used for ContainerCandidates to
// sort candidate containers.
type ContainerComparer func(a, b *core.CachedContainer) int

// ResourceScoreComparer creates a ContainerComparer to sort container by resource
// score.
func ResourceScoreComparer(opt *config.PersistOptions) ContainerComparer {
	return func(a, b *core.CachedContainer) int {
		sa := a.ResourceScore(opt.GetResourceScoreFormulaVersion(), opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0, 0)
		sb := b.ResourceScore(opt.GetResourceScoreFormulaVersion(), opt.GetHighSpaceRatio(), opt.GetLowSpaceRatio(), 0, 0)
		switch {
		case sa > sb:
			return 1
		case sa < sb:
			return -1
		default:
			return 0
		}
	}
}

// IsolationComparer creates a ContainerComparer to sort container by isolation score.
func IsolationComparer(locationLabels []string, resourceContainers []*core.CachedContainer) ContainerComparer {
	return func(a, b *core.CachedContainer) int {
		sa := core.DistinctScore(locationLabels, resourceContainers, a)
		sb := core.DistinctScore(locationLabels, resourceContainers, b)
		switch {
		case sa > sb:
			return 1
		case sa < sb:
			return -1
		default:
			return 0
		}
	}
}
