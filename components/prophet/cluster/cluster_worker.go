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
	"fmt"

	"github.com/fagongzi/util/protoc"
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
		if err == errResourceDestroyed {
			co.opController.DispatchDestoryDirectly(res, schedule.DispatchFromHeartBeat)
			return nil
		}
		return err
	}

	co.opController.Dispatch(res, schedule.DispatchFromHeartBeat)
	return nil
}

// HandleCreateDestorying handle create destroying
func (c *RaftCluster) HandleCreateDestorying(req rpcpb.CreateDestoryingReq) (metapb.ResourceState, error) {
	c.Lock()
	defer c.Unlock()

	if c.core.AlreadyRemoved(req.ID) {
		return metapb.ResourceState_Destroyed, nil
	}

	status, err := c.getDestroyingStatusLocked(req.ID)
	if err != nil {
		return metapb.ResourceState_Destroying, err
	}
	if status != nil {
		return status.State, nil
	}

	status = &metapb.DestroyingStatus{
		State:      metapb.ResourceState_Destroying,
		Index:      req.Index,
		Replicas:   make(map[uint64]bool),
		RemoveData: req.RemoveData,
	}
	for _, id := range req.Replicas {
		status.Replicas[id] = false
	}
	if err := c.saveDestroyingStatusLocked(req.ID, status); err != nil {
		return metapb.ResourceState_Destroying, err
	}

	return status.State, nil
}

// HandleReportDestoryed handle report destroyed
func (c *RaftCluster) HandleReportDestoryed(req rpcpb.ReportDestoryedReq) (metapb.ResourceState, error) {
	c.Lock()
	defer c.Unlock()

	if c.core.AlreadyRemoved(req.ID) {
		return metapb.ResourceState_Destroyed, nil
	}

	status, err := c.getDestroyingStatusLocked(req.ID)
	if err != nil {
		return metapb.ResourceState_Destroying, err
	}
	if status == nil {
		c.logger.Fatal("BUG: missing destroying status",
			zap.Uint64("resource", req.ID))
		return metapb.ResourceState_Destroying, nil
	}

	if status.State == metapb.ResourceState_Destroyed {
		return metapb.ResourceState_Destroyed, nil
	}
	if v, ok := status.Replicas[req.ReplicaID]; !ok || v {
		return status.State, nil
	}

	status.Replicas[req.ReplicaID] = true
	n := 0
	for _, destroyed := range status.Replicas {
		if destroyed {
			n++
		}
	}
	if n == len(status.Replicas) {
		status.State = metapb.ResourceState_Destroyed
		status.Replicas = nil
	}
	if err := c.saveDestroyingStatusLocked(req.ID, status); err != nil {
		return metapb.ResourceState_Destroying, err
	}

	return status.State, nil
}

// HandleGetDestorying returns resource destroying status
func (c *RaftCluster) HandleGetDestorying(req rpcpb.GetDestoryingReq) (*metapb.DestroyingStatus, error) {
	c.RLock()
	defer c.RUnlock()

	return c.getDestroyingStatusLocked(req.ID)
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

		id, _, err := c.storage.KV().AllocID(1)
		if err != nil {
			return nil, err
		}
		res.SetID(id)
		res.SetState(metapb.ResourceState_Creating)

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
			id, _, err := c.storage.KV().AllocID(1)
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
		if c.core.AlreadyRemoved(id) {
			continue
		}

		v := c.core.GetResource(id)
		if v == nil {
			return nil, fmt.Errorf("resource %d not found in prophet", id)
		}
		origin = append(origin, v.Meta)

		res := v.Meta.Clone() // use cloned value
		res.SetState(metapb.ResourceState_Destroyed)
		targets = append(targets, res)
	}
	err := c.storage.PutResources(targets...)
	if err != nil {
		return nil, err
	}

	c.core.AddRemovedResources(request.RemoveResources.IDs...)
	for _, res := range origin {
		res.SetState(metapb.ResourceState_Destroyed)
		c.changedEvents <- event.NewResourceEvent(res, 0, true, false)
	}

	return &rpcpb.RemoveResourcesRsp{}, nil
}

// HandleCheckResourceState handle check resource state
func (c *RaftCluster) HandleCheckResourceState(request *rpcpb.Request) (*rpcpb.CheckResourceStateRsp, error) {
	c.RLock()
	defer c.RUnlock()

	destroyed, destroying := c.core.GetDestroyResources(util.MustUnmarshalBM64(request.CheckResourceState.IDs))
	return &rpcpb.CheckResourceStateRsp{
		Destroyed:  util.MustMarshalBM64(destroyed),
		Destroying: util.MustMarshalBM64(destroying),
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

func (c *RaftCluster) getDestroyingStatusLocked(id uint64) (*metapb.DestroyingStatus, error) {
	status := c.core.GetDestroyingStatus(id)
	if status != nil {
		return status, nil
	}

	v, err := c.storage.GetResourceExtra(id)
	if err != nil {
		return nil, err
	}

	if len(v) > 0 {
		status = &metapb.DestroyingStatus{}
		protoc.MustUnmarshal(status, []byte(v))
		return status, nil
	}
	return nil, nil
}

func (c *RaftCluster) saveDestroyingStatusLocked(id uint64, status *metapb.DestroyingStatus) error {
	if status.State == metapb.ResourceState_Destroyed {
		res := c.core.GetResource(id)
		if res == nil {
			c.logger.Fatal("missing resource to set destoryed",
				zap.Uint64("resource", id))
			return nil
		}

		v := res.Meta.Clone()
		v.SetState(metapb.ResourceState_Destroyed)
		if err := c.storage.PutResourceAndExtra(v, protoc.MustMarshal(status)); err != nil {
			return err
		}
		c.core.AddRemovedResources(id)
		res.Meta.SetState(metapb.ResourceState_Destroyed)
	} else {
		err := c.storage.PutResourceExtra(id, protoc.MustMarshal(status))
		if err != nil {
			return err
		}
	}

	c.core.UpdateDestroyingStatus(id, status)
	return nil
}
