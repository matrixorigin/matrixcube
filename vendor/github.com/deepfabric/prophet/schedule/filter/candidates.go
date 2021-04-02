package filter

import (
	"math/rand"
	"sort"

	"github.com/deepfabric/prophet/config"
	"github.com/deepfabric/prophet/core"
)

// ContainerCandidates wraps container list and provide utilities to select source or
// target container to schedule.
type ContainerCandidates struct {
	Containers []*core.CachedContainer
}

// NewCandidates creates ContainerCandidates with container list.
func NewCandidates(containers []*core.CachedContainer) *ContainerCandidates {
	return &ContainerCandidates{Containers: containers}
}

// FilterSource keeps containers that can pass all source filters.
func (c *ContainerCandidates) FilterSource(opt *config.PersistOptions, filters ...Filter) *ContainerCandidates {
	c.Containers = SelectSourceContainers(c.Containers, filters, opt)
	return c
}

// FilterTarget keeps containers that can pass all target filters.
func (c *ContainerCandidates) FilterTarget(opt *config.PersistOptions, filters ...Filter) *ContainerCandidates {
	c.Containers = SelectTargetContainers(c.Containers, filters, opt)
	return c
}

// Sort sorts container list by given comparer in ascending order.
func (c *ContainerCandidates) Sort(less ContainerComparer) *ContainerCandidates {
	sort.Slice(c.Containers, func(i, j int) bool { return less(c.Containers[i], c.Containers[j]) < 0 })
	return c
}

// Reverse reverses the candidate container list.
func (c *ContainerCandidates) Reverse() *ContainerCandidates {
	for i := len(c.Containers)/2 - 1; i >= 0; i-- {
		opp := len(c.Containers) - 1 - i
		c.Containers[i], c.Containers[opp] = c.Containers[opp], c.Containers[i]
	}
	return c
}

// Shuffle reorders all candidates randomly.
func (c *ContainerCandidates) Shuffle() *ContainerCandidates {
	rand.Shuffle(len(c.Containers), func(i, j int) { c.Containers[i], c.Containers[j] = c.Containers[j], c.Containers[i] })
	return c
}

// Top keeps all containers that have the same priority with the first container.
// The container list should be sorted before calling Top.
func (c *ContainerCandidates) Top(less ContainerComparer) *ContainerCandidates {
	var i int
	for i < len(c.Containers) && less(c.Containers[0], c.Containers[i]) == 0 {
		i++
	}
	c.Containers = c.Containers[:i]
	return c
}

// PickFirst returns the first container in candidate list.
func (c *ContainerCandidates) PickFirst() *core.CachedContainer {
	if len(c.Containers) == 0 {
		return nil
	}
	return c.Containers[0]
}

// RandomPick returns a random container from the list.
func (c *ContainerCandidates) RandomPick() *core.CachedContainer {
	if len(c.Containers) == 0 {
		return nil
	}
	return c.Containers[rand.Intn(len(c.Containers))]
}
