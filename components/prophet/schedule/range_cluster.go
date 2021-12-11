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

package schedule

import (
	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
)

// RangeCluster isolates the cluster by range.
type RangeCluster struct {
	opt.Cluster
	group             uint64
	subCluster        *core.BasicCluster // Collect all resources belong to the range.
	tolerantSizeRatio float64
}

// GenRangeCluster gets a range cluster by specifying start key and end key.
// The cluster can only know the resources within [startKey, endKey].
func GenRangeCluster(group uint64, cluster opt.Cluster, startKey, endKey []byte) *RangeCluster {
	subCluster := core.NewBasicCluster(cluster.GetResourceFactory(), cluster.GetLogger())
	for _, r := range cluster.ScanResources(group, startKey, endKey, -1) {
		subCluster.Resources.AddResource(r)
	}
	return &RangeCluster{
		Cluster:    cluster,
		subCluster: subCluster,
		group:      group,
	}
}

func (r *RangeCluster) updateCachedContainer(s *core.CachedContainer) *core.CachedContainer {
	id := s.Meta.ID()

	used := float64(s.GetUsedSize()) / (1 << 20)
	if used == 0 {
		return s
	}

	groupKeys := r.Cluster.GetScheduleGroupKeysWithPrefix(util.EncodeGroupKey(r.group, nil, nil))
	var amplification float64
	var opts []core.ContainerCreateOption
	var totalResourceSize int64
	for _, groupKey := range groupKeys {
		amplification += float64(s.GetResourceSize(groupKey))
		leaderCount := r.subCluster.GetContainerLeaderCount(groupKey, id)
		opts = append(opts, core.SetLeaderCount(groupKey, leaderCount))

		leaderSize := r.subCluster.GetContainerLeaderResourceSize(groupKey, id)
		opts = append(opts, core.SetLeaderSize(groupKey, leaderSize))

		resourceCount := r.subCluster.GetContainerResourceCount(groupKey, id)
		opts = append(opts, core.SetResourceCount(groupKey, resourceCount))

		resourceSize := r.subCluster.GetContainerResourceSize(groupKey, id)
		totalResourceSize += resourceSize
		opts = append(opts, core.SetResourceSize(groupKey, resourceSize))

		pendingPeerCount := r.subCluster.GetContainerPendingPeerCount(groupKey, id)
		opts = append(opts, core.SetPendingPeerCount(groupKey, pendingPeerCount))
	}

	amplification = amplification / used
	newStats := proto.Clone(s.GetContainerStats()).(*metapb.ContainerStats)
	newStats.UsedSize = uint64(float64(totalResourceSize)/amplification) * (1 << 20)
	newStats.Available = s.GetCapacity() - newStats.UsedSize
	opts = append(opts, core.SetNewContainerStats(newStats))
	newContainer := s.Clone(opts...)
	return newContainer
}

// GetContainer searches for a container by ID.
func (r *RangeCluster) GetContainer(id uint64) *core.CachedContainer {
	s := r.Cluster.GetContainer(id)
	if s == nil {
		return nil
	}
	return r.updateCachedContainer(s)
}

// GetContainers returns all Containers in the cluster.
func (r *RangeCluster) GetContainers() []*core.CachedContainer {
	containers := r.Cluster.GetContainers()
	newContainers := make([]*core.CachedContainer, 0, len(containers))
	for _, s := range containers {
		newContainers = append(newContainers, r.updateCachedContainer(s))
	}
	return newContainers
}

// SetTolerantSizeRatio sets the tolerant size ratio.
func (r *RangeCluster) SetTolerantSizeRatio(ratio float64) {
	r.tolerantSizeRatio = ratio
}

// GetTolerantSizeRatio gets the tolerant size ratio.
func (r *RangeCluster) GetTolerantSizeRatio() float64 {
	if r.tolerantSizeRatio != 0 {
		return r.tolerantSizeRatio
	}
	return r.Cluster.GetOpts().GetTolerantSizeRatio()
}

// RandFollowerResource returns a random resource that has a follower on the Container.
func (r *RangeCluster) RandFollowerResource(groupKey string, containerID uint64, ranges []core.KeyRange, opts ...core.ResourceOption) *core.CachedResource {
	return r.subCluster.RandFollowerResource(groupKey, containerID, ranges, opts...)
}

// RandLeaderResource returns a random resource that has leader on the container.
func (r *RangeCluster) RandLeaderResource(groupKey string, containerID uint64, ranges []core.KeyRange, opts ...core.ResourceOption) *core.CachedResource {
	return r.subCluster.RandLeaderResource(groupKey, containerID, ranges, opts...)
}

// GetAverageResourceSize returns the average resource approximate size.
func (r *RangeCluster) GetAverageResourceSize() int64 {
	return r.subCluster.GetAverageResourceSize()
}

// GetResourceContainers returns all containers that contains the resource's peer.
func (r *RangeCluster) GetResourceContainers(res *core.CachedResource) []*core.CachedContainer {
	containers := r.Cluster.GetResourceContainers(res)
	newContainers := make([]*core.CachedContainer, 0, len(containers))
	for _, s := range containers {
		newContainers = append(newContainers, r.updateCachedContainer(s))
	}
	return newContainers
}

// GetFollowerContainers returns all containers that contains the resource's follower peer.
func (r *RangeCluster) GetFollowerContainers(res *core.CachedResource) []*core.CachedContainer {
	containers := r.Cluster.GetFollowerContainers(res)
	newContainers := make([]*core.CachedContainer, 0, len(containers))
	for _, s := range containers {
		newContainers = append(newContainers, r.updateCachedContainer(s))
	}
	return newContainers
}

// GetLeaderContainer returns all containers that contains the resource's leader peer.
func (r *RangeCluster) GetLeaderContainer(res *core.CachedResource) *core.CachedContainer {
	s := r.Cluster.GetLeaderContainer(res)
	if s != nil {
		return r.updateCachedContainer(s)
	}
	return s
}
