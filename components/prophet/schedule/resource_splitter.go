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
	"bytes"
	"context"
	"errors"
	"math"
	"time"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/operator"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/opt"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

const (
	watchInterval = 100 * time.Millisecond
	timeout       = 1 * time.Minute
)

// SplitResourcesHandler used to handle resource splitting
type SplitResourcesHandler interface {
	SplitResourceByKeys(res *core.CachedResource, splitKeys [][]byte) error
	ScanResourcesByKeyRange(group uint64, groupKeys *resourceGroupKeys, results *splitKeyResults)
}

// NewSplitResourcesHandler return SplitResourcesHandler
func NewSplitResourcesHandler(cluster opt.Cluster, oc *OperatorController) SplitResourcesHandler {
	return &splitResourcesHandler{
		cluster: cluster,
		oc:      oc,
	}
}

// ResourceSplitter handles split resources
type ResourceSplitter struct {
	cluster opt.Cluster
	handler SplitResourcesHandler
}

// NewResourceSplitter return a resource splitter
func NewResourceSplitter(cluster opt.Cluster, handler SplitResourcesHandler) *ResourceSplitter {
	return &ResourceSplitter{
		cluster: cluster,
		handler: handler,
	}
}

// SplitResources support splitResources by given split keys.
func (r *ResourceSplitter) SplitResources(ctx context.Context, group uint64, splitKeys [][]byte, retryLimit int) (int, []uint64) {
	if len(splitKeys) < 1 {
		return 0, nil
	}
	unprocessedKeys := splitKeys
	newResources := make(map[uint64]struct{}, len(splitKeys))
	for i := 0; i <= retryLimit; i++ {
		unprocessedKeys = r.splitResourcesByKeys(ctx, group, unprocessedKeys, newResources)
		if len(unprocessedKeys) < 1 {
			break
		}
		// sleep for a while between each retry
		time.Sleep(typeutil.MinDuration(maxSleepDuration, time.Duration(math.Pow(2, float64(i)))*initialSleepDuration))
	}
	returned := make([]uint64, 0, len(newResources))
	for resID := range newResources {
		returned = append(returned, resID)
	}
	return 100 - len(unprocessedKeys)*100/len(splitKeys), returned
}

func (r *ResourceSplitter) splitResourcesByKeys(parCtx context.Context, resGroup uint64, splitKeys [][]byte, newResources map[uint64]struct{}) [][]byte {
	validGroups := r.groupKeysByResource(resGroup, splitKeys)
	for key, group := range validGroups {
		err := r.handler.SplitResourceByKeys(group.resource, group.keys)
		if err != nil {
			delete(validGroups, key)
			continue
		}
	}
	results := newSplitKeyResults()
	ticker := time.NewTicker(watchInterval)
	ctx, cancel := context.WithTimeout(parCtx, timeout)
	defer func() {
		ticker.Stop()
		cancel()
	}()
	for {
		select {
		case <-ticker.C:
			for _, groupKeys := range validGroups {
				if groupKeys.finished {
					continue
				}
				r.handler.ScanResourcesByKeyRange(resGroup, groupKeys, results)
			}
		case <-ctx.Done():
		}
		finished := true
		for _, groupKeys := range validGroups {
			if !groupKeys.finished {
				finished = false
			}
		}
		if finished {
			break
		}
	}
	for newID := range results.getSplitResources() {
		newResources[newID] = struct{}{}
	}
	return results.getUnProcessedKeys(splitKeys)
}

// groupKeysByResource separates keys into groups by their belonging Resources.
func (r *ResourceSplitter) groupKeysByResource(group uint64, keys [][]byte) map[uint64]*resourceGroupKeys {
	groups := make(map[uint64]*resourceGroupKeys, len(keys))
	for _, key := range keys {
		res := r.cluster.GetResourceByKey(group, key)
		if res == nil {
			r.cluster.GetLogger().Error("resource hollow",
				log.HexField("key", key))
			continue
		}
		// assert resource valid
		if !r.checkResourceValid(res) {
			continue
		}
		r.cluster.GetLogger().Info("found resource",
			log.ResourceField(res.Meta.ID()),
			log.HexField("key", key))
		_, ok := groups[res.Meta.ID()]
		if !ok {
			groups[res.Meta.ID()] = &resourceGroupKeys{
				resource: res,
				keys: [][]byte{
					key,
				},
			}
		} else {
			groups[res.Meta.ID()].keys = append(groups[res.Meta.ID()].keys, key)
		}
	}
	return groups
}

func (r *ResourceSplitter) checkResourceValid(res *core.CachedResource) bool {
	if r.cluster.IsResourceHot(res) {
		return false
	}
	if !opt.IsResourceReplicated(r.cluster, res) {
		r.cluster.AddSuspectResources(res.Meta.ID())
		return false
	}
	if res.GetLeader() == nil {
		return false
	}
	return true
}

type splitResourcesHandler struct {
	cluster opt.Cluster
	oc      *OperatorController
}

func (h *splitResourcesHandler) SplitResourceByKeys(res *core.CachedResource, splitKeys [][]byte) error {
	op, err := operator.CreateSplitResourceOperator("resource-splitter", res, 0, metapb.CheckPolicy_USEKEY, splitKeys)
	if err != nil {
		return err
	}

	if ok := h.oc.AddOperator(op); !ok {
		h.cluster.GetLogger().Warn("add resource split operator failed",
			log.ResourceField(res.Meta.ID()))
		return errors.New("add resource split operator failed")
	}
	return nil
}

func (h *splitResourcesHandler) ScanResourcesByKeyRange(group uint64, groupKeys *resourceGroupKeys, results *splitKeyResults) {
	splitKeys := groupKeys.keys
	startKey, endKey := groupKeys.resource.GetStartKey(), groupKeys.resource.GetEndKey()
	createdResources := make(map[uint64][]byte, len(splitKeys))
	defer func() {
		results.addResourcesID(createdResources)
	}()
	resources := h.cluster.ScanResources(group, startKey, endKey, -1)
	for _, res := range resources {
		for _, splitKey := range splitKeys {
			if bytes.Equal(splitKey, res.GetStartKey()) {
				h.cluster.GetLogger().Info("resource found split key",
					log.ResourceField(res.Meta.ID()),
					log.HexField("split-key", splitKey))
				createdResources[res.Meta.ID()] = splitKey
			}
		}
	}
	if len(createdResources) >= len(splitKeys) {
		groupKeys.finished = true
	}
}

type resourceGroupKeys struct {
	// finished indicates all the split resources have been found in `resource` according to the `keys`
	finished bool
	resource *core.CachedResource
	keys     [][]byte
}

type splitKeyResults struct {
	// newResourceID -> newResourceID's startKey
	newResources map[uint64][]byte
}

func newSplitKeyResults() *splitKeyResults {
	s := &splitKeyResults{}
	s.newResources = make(map[uint64][]byte)
	return s
}

func (r *splitKeyResults) addResourcesID(resourceIDs map[uint64][]byte) {
	for id, splitKey := range resourceIDs {
		r.newResources[id] = splitKey
	}
}

func (r *splitKeyResults) getSplitResources() map[uint64][]byte {
	return r.newResources
}

func (r *splitKeyResults) getUnProcessedKeys(splitKeys [][]byte) [][]byte {
	var unProcessedKeys [][]byte
	for _, splitKey := range splitKeys {
		processed := false
		for _, resStartKey := range r.newResources {
			if bytes.Equal(splitKey, resStartKey) {
				processed = true
				break
			}
		}
		if !processed {
			unProcessedKeys = append(unProcessedKeys, splitKey)
		}
	}
	return unProcessedKeys
}
