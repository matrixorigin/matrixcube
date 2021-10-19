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

package cluster

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/event"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"go.uber.org/zap"
)

// HandleResourceHeartbeat processes CachedResource reports from client.
func (c *RaftCluster) HandleResourceHeartbeat(res *core.CachedResource) error {
	c.RLock()
	co := c.coordinator
	c.RUnlock()

	if err := c.processResourceHeartbeat(res); err != nil {
		if err == errResourceRemoved {
			co.opController.DispatchDestoryDirectly(res, schedule.DispatchFromHeartBeat)
			return nil
		}
		return err
	}

	co.opController.Dispatch(res, schedule.DispatchFromHeartBeat)
	return nil
}

// HandleAskSplit handles the split request.
func (c *RaftCluster) HandleAskSplit(request *rpcpb.Request) (*rpcpb.AskSplitRsp, error) {
	reqResource := c.adapter.NewResource()
	err := reqResource.Unmarshal(request.AskSplit.Data)
	if err != nil {
		return nil, err
	}

	err = c.ValidRequestResource(reqResource)
	if err != nil {
		return nil, err
	}

	newResourceID, err := c.AllocID()
	if err != nil {
		return nil, err
	}

	peerIDs := make([]uint64, len(reqResource.Peers()))
	for i := 0; i < len(peerIDs); i++ {
		if peerIDs[i], err = c.AllocID(); err != nil {
			return nil, err
		}
	}

	// Disable merge for the 2 resources in a period of time.
	c.GetMergeChecker().RecordResourceSplit([]uint64{reqResource.ID(), newResourceID})

	split := &rpcpb.AskSplitRsp{}
	split.SplitID.NewID = newResourceID
	split.SplitID.NewReplicaIDs = peerIDs

	c.logger.Info("ids allocated for resource split",
		zap.Uint64("resource", newResourceID),
		zap.Any("peer-ids", peerIDs))
	return split, nil
}

// ValidRequestResource is used to decide if the resource is valid.
func (c *RaftCluster) ValidRequestResource(reqResource metadata.Resource) error {
	startKey, _ := reqResource.Range()
	res := c.GetResourceByKey(reqResource.Group(), startKey)
	if res == nil {
		return fmt.Errorf("resource not found, request resource: %v", reqResource)
	}
	// If the request epoch is less than current resource epoch, then returns an error.
	reqResourceEpoch := reqResource.Epoch()
	resourceEpoch := res.Meta.Epoch()
	if reqResourceEpoch.GetVersion() < resourceEpoch.GetVersion() ||
		reqResourceEpoch.GetConfVer() < resourceEpoch.GetConfVer() {
		return fmt.Errorf("invalid resource epoch, request: %v, current: %v", reqResourceEpoch, resourceEpoch)
	}
	return nil
}

// HandleAskBatchSplit handles the batch split request.
func (c *RaftCluster) HandleAskBatchSplit(request *rpcpb.Request) (*rpcpb.AskBatchSplitRsp, error) {
	reqResource := c.adapter.NewResource()
	err := reqResource.Unmarshal(request.AskBatchSplit.Data)
	if err != nil {
		return nil, err
	}

	splitCount := request.AskBatchSplit.Count
	err = c.ValidRequestResource(reqResource)
	if err != nil {
		return nil, err
	}
	splitIDs := make([]rpcpb.SplitID, 0, splitCount)
	recordResources := make([]uint64, 0, splitCount+1)

	for i := 0; i < int(splitCount); i++ {
		newResourceID, err := c.AllocID()
		if err != nil {
			return nil, err
		}

		peerIDs := make([]uint64, len(reqResource.Peers()))
		for i := 0; i < len(peerIDs); i++ {
			if peerIDs[i], err = c.AllocID(); err != nil {
				return nil, err
			}
		}

		recordResources = append(recordResources, newResourceID)
		splitIDs = append(splitIDs, rpcpb.SplitID{
			NewID:         newResourceID,
			NewReplicaIDs: peerIDs,
		})

		c.logger.Info("ids allocated for resource split",
			zap.Uint64("resource", newResourceID),
			zap.Any("peer-ids", peerIDs))
	}

	recordResources = append(recordResources, reqResource.ID())
	// Disable merge the resources in a period of time.
	c.GetMergeChecker().RecordResourceSplit(recordResources)

	// If resource splits during the scheduling process, resources with abnormal
	// status may be left, and these resources need to be checked with higher
	// priority.
	c.AddSuspectResources(recordResources...)

	return &rpcpb.AskBatchSplitRsp{SplitIDs: splitIDs}, nil
}

func (c *RaftCluster) checkSplitResource(left metadata.Resource, right metadata.Resource) error {
	if left == nil || right == nil {
		return errors.New("invalid split resource")
	}

	leftStart, leftEnd := left.Range()
	rightStart, rightEnd := right.Range()

	if !bytes.Equal(leftEnd, rightStart) {
		return errors.New("invalid split resource with leftEnd != rightStart")
	}

	if len(rightEnd) == 0 || bytes.Compare(leftStart, rightEnd) < 0 {
		return nil
	}

	return errors.New("invalid split resource")
}

func (c *RaftCluster) checkSplitResources(resources []metadata.Resource) error {
	if len(resources) <= 1 {
		return errors.New("invalid split resource")
	}

	for i := 1; i < len(resources); i++ {
		left := resources[i-1]
		right := resources[i]

		leftStart, leftEnd := left.Range()
		rightStart, rightEnd := right.Range()

		if !bytes.Equal(leftEnd, rightStart) {
			return errors.New("invalid split resource")
		}
		if len(rightEnd) != 0 && bytes.Compare(leftStart, rightEnd) >= 0 {
			return errors.New("invalid split resource")
		}
	}
	return nil
}

// HandleReportSplit handles the report split request.
func (c *RaftCluster) HandleReportSplit(request *rpcpb.Request) (*rpcpb.ReportSplitRsp, error) {
	left := c.adapter.NewResource()
	err := left.Unmarshal(request.ReportSplit.Left)
	if err != nil {
		return nil, err
	}

	right := c.adapter.NewResource()
	err = right.Unmarshal(request.ReportSplit.Right)
	if err != nil {
		return nil, err
	}

	err = c.checkSplitResource(left, right)
	if err != nil {
		c.logger.Warn("report split resource is invalid",
			zap.Any("left", left),
			zap.Any("right", right),
			zap.Error(err))
		return nil, err
	}

	// Build origin resource by using left and right.
	origin := right.Clone()
	origin.SetEpoch(metapb.ResourceEpoch{})
	start, _ := left.Range()
	origin.SetStartKey(start)
	c.logger.Info("resource split completed",
		zap.Uint64("resource", origin.ID()))
	return &rpcpb.ReportSplitRsp{}, nil
}

// HandleBatchReportSplit handles the batch report split request.
func (c *RaftCluster) HandleBatchReportSplit(request *rpcpb.Request) (*rpcpb.BatchReportSplitRsp, error) {
	var resources []metadata.Resource
	for _, data := range request.BatchReportSplit.Resources {
		res := c.adapter.NewResource()
		err := res.Unmarshal(data)
		if err != nil {
			return nil, err
		}
		resources = append(resources, res)
	}

	hrm := core.ResourcesToHexMeta(resources)
	err := c.checkSplitResources(resources)
	if err != nil {
		c.logger.Warn("report batch split resource is invalid",
			zap.String("resource-data", hrm.String()),
			zap.Error(err))
		return nil, err
	}
	last := len(resources) - 1
	origin := resources[last].Clone()
	hrm = core.ResourcesToHexMeta(resources[:last])
	c.logger.Info("resource batch split completed",
		zap.Uint64("resource", origin.ID()))
	return &rpcpb.BatchReportSplitRsp{}, nil
}

// HandleCreateResources handle create resources. It will create resources with full replica peers.
func (c *RaftCluster) HandleCreateResources(request *rpcpb.Request) (*rpcpb.CreateResourcesRsp, error) {
	if len(request.CreateResources.Resources) > 4 {
		return nil, fmt.Errorf("exceed the maximum batch size of create resources, max is %d current %d",
			4, len(request.CreateResources.Resources))
	}

	if request.CreateResources.LeastReplicas == nil {
		request.CreateResources.LeastReplicas = make([]uint64, len(request.CreateResources.Resources))
	}

	c.RLock()
	defer c.RUnlock()

	var createResources []metadata.Resource
	var leastPeers []int
	for idx, data := range request.CreateResources.Resources {
		res := c.adapter.NewResource()
		err := res.Unmarshal(data)
		if err != nil {
			return nil, err
		}
		if len(res.Peers()) > 0 {
			return nil, fmt.Errorf("cann't assign peers in create resources")
		}

		// check recreate
		create := true
		for _, cr := range c.core.GetResources() {
			if cr.Meta.Unique() == res.Unique() {
				create = false
				c.logger.Info("resource already created",
					zap.String("unique", res.Unique()))
				break
			}
		}
		if create {
			c.core.ForeachWaittingCreateResources(func(wres metadata.Resource) {
				if wres.Unique() == res.Unique() {
					create = false
					c.logger.Info("resource already in waitting create queue",
						zap.String("unique", res.Unique()))
				}
			})
		}

		if !create {
			continue
		}

		id, err := c.storage.KV().AllocID()
		if err != nil {
			return nil, err
		}
		res.SetID(id)
		res.SetState(metapb.ResourceState_WaittingCreate)

		_, err = c.core.PreCheckPutResource(core.NewCachedResource(res, nil))
		if err != nil {
			return nil, err
		}
		createResources = append(createResources, res)
		leastPeers = append(leastPeers, int(request.CreateResources.LeastReplicas[idx]))
	}

	for idx, res := range createResources {
		err := c.coordinator.checkers.FillReplicas(core.NewCachedResource(res, nil), leastPeers[idx])
		if err != nil {
			return nil, err
		}

		res.SetEpoch(metapb.ResourceEpoch{ConfVer: uint64(len(res.Peers()))})
		for idx := range res.Peers() {
			id, err := c.storage.KV().AllocID()
			if err != nil {
				return nil, err
			}

			res.Peers()[idx].ID = id
			res.Peers()[idx].InitialMember = true
		}

		c.logger.Info("resource created",
			zap.Uint64("resource", res.ID()),
			zap.Any("peers", res.Peers()))
	}

	err := c.storage.PutResources(createResources...)
	if err != nil {
		return nil, err
	}

	c.core.AddWaittingCreateResources(createResources...)
	c.triggerNotifyCreateResources()
	return &rpcpb.CreateResourcesRsp{}, nil
}

func (c *RaftCluster) triggerNotifyCreateResources() {
	select {
	case c.createResourceC <- struct{}{}:
	default:
	}
}

func (c *RaftCluster) doNotifyCreateResources() {
	c.core.ForeachWaittingCreateResources(func(res metadata.Resource) {
		c.changedEvents <- event.NewResourceEvent(res, 0, false, true)
	})
}

// HandleRemoveResources handle remove resources
func (c *RaftCluster) HandleRemoveResources(request *rpcpb.Request) (*rpcpb.RemoveResourcesRsp, error) {
	if len(request.RemoveResources.IDs) > 4 {
		return nil, fmt.Errorf("exceed the maximum batch size of remove resources, max is %d current %d",
			4, len(request.RemoveResources.IDs))
	}

	c.RLock()
	defer c.RUnlock()

	var targets []metadata.Resource
	var origin []metadata.Resource
	for _, id := range request.RemoveResources.IDs {
		v := c.core.GetResource(id)
		if v == nil {
			return nil, fmt.Errorf("resource %d not found in prophet", id)
		}
		origin = append(origin, v.Meta)

		res := v.Meta.Clone() // use cloned value
		res.SetState(metapb.ResourceState_Removed)
		targets = append(targets, res)
	}
	err := c.storage.PutResources(targets...)
	if err != nil {
		return nil, err
	}

	c.core.AddRemovedResources(request.RemoveResources.IDs...)
	for _, res := range origin {
		res.SetState(metapb.ResourceState_Removed)
		c.changedEvents <- event.NewResourceEvent(res, 0, true, false)
	}

	return &rpcpb.RemoveResourcesRsp{}, nil
}

// HandleCheckResourceState handle check resource state
func (c *RaftCluster) HandleCheckResourceState(request *rpcpb.Request) (*rpcpb.CheckResourceStateRsp, error) {
	return &rpcpb.CheckResourceStateRsp{
		Removed: c.core.GetRemovedResources(util.MustUnmarshalBM64(request.CheckResourceState.IDs)),
	}, nil
}

// HandlePutPlacementRule handle put placement rule
func (c *RaftCluster) HandlePutPlacementRule(request *rpcpb.Request) error {
	return c.GetRuleManager().SetRule(placement.NewRuleFromRPC(request.PutPlacementRule.Rule))
}

// HandleAppliedRules handle get applied rules
func (c *RaftCluster) HandleAppliedRules(request *rpcpb.Request) (*rpcpb.GetAppliedRulesRsp, error) {
	res := c.GetResource(request.GetAppliedRules.ResourceID)
	if res == nil {
		return nil, fmt.Errorf("resource %d not found", request.GetAppliedRules.ResourceID)
	}

	rules := c.GetRuleManager().GetRulesForApplyResource(res)
	return &rpcpb.GetAppliedRulesRsp{
		Rules: placement.RPCRules(rules),
	}, nil
}
