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
	"github.com/matrixorigin/matrixcube/components/prophet/schedule"
	"github.com/matrixorigin/matrixcube/components/prophet/schedule/placement"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"go.uber.org/zap"
)

// HandleShardHeartbeat processes CachedShard reports from client.
func (c *RaftCluster) HandleShardHeartbeat(res *core.CachedShard) error {
	c.RLock()
	co := c.coordinator
	c.RUnlock()

	if err := c.processShardHeartbeat(res); err != nil {
		if err == errShardDestroyed {
			co.opController.DispatchDestroyDirectly(res, schedule.DispatchFromHeartBeat)
			return nil
		}
		return err
	}

	co.opController.Dispatch(res, schedule.DispatchFromHeartBeat)
	return nil
}

// HandleCreateDestroying handle create destroying
func (c *RaftCluster) HandleCreateDestroying(req rpcpb.CreateDestroyingReq) (metapb.ShardState, error) {
	c.Lock()
	defer c.Unlock()

	if c.core.AlreadyRemoved(req.ID) {
		return metapb.ShardState_Destroyed, nil
	}

	status, err := c.getDestroyingStatusLocked(req.ID)
	if err != nil {
		return metapb.ShardState_Destroying, err
	}
	if status != nil {
		return status.State, nil
	}

	status = &metapb.DestroyingStatus{
		State:      metapb.ShardState_Destroying,
		Index:      req.Index,
		Replicas:   make(map[uint64]bool),
		RemoveData: req.RemoveData,
	}
	for _, id := range req.Replicas {
		status.Replicas[id] = false
	}
	if err := c.saveDestroyingStatusLocked(req.ID, status); err != nil {
		return metapb.ShardState_Destroying, err
	}

	return status.State, nil
}

// HandleReportDestroyed handle report destroyed
func (c *RaftCluster) HandleReportDestroyed(req rpcpb.ReportDestroyedReq) (metapb.ShardState, error) {
	c.Lock()
	defer c.Unlock()

	if c.core.AlreadyRemoved(req.ID) {
		return metapb.ShardState_Destroyed, nil
	}

	status, err := c.getDestroyingStatusLocked(req.ID)
	if err != nil {
		return metapb.ShardState_Destroying, err
	}
	if status == nil {
		c.logger.Fatal("BUG: missing destroying status",
			zap.Uint64("resource", req.ID))
		return metapb.ShardState_Destroying, nil
	}

	if status.State == metapb.ShardState_Destroyed {
		return metapb.ShardState_Destroyed, nil
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
		status.State = metapb.ShardState_Destroyed
		status.Replicas = nil
	}
	if err := c.saveDestroyingStatusLocked(req.ID, status); err != nil {
		return metapb.ShardState_Destroying, err
	}

	return status.State, nil
}

// HandleGetDestroying returns resource destroying status
func (c *RaftCluster) HandleGetDestroying(req rpcpb.GetDestroyingReq) (*metapb.DestroyingStatus, error) {
	c.RLock()
	defer c.RUnlock()

	return c.getDestroyingStatusLocked(req.ID)
}

// ValidRequestShard is used to decide if the resource is valid.
func (c *RaftCluster) ValidRequestShard(reqShard *metapb.Shard) error {
	startKey, _ := reqShard.GetRange()
	res := c.GetShardByKey(reqShard.GetGroup(), startKey)
	if res == nil {
		return fmt.Errorf("resource not found, request resource: %v", reqShard)
	}
	// If the request epoch is less than current resource epoch, then returns an error.
	reqShardEpoch := reqShard.GetEpoch()
	resourceEpoch := res.Meta.GetEpoch()
	if reqShardEpoch.GetVersion() < resourceEpoch.GetVersion() ||
		reqShardEpoch.GetConfVer() < resourceEpoch.GetConfVer() {
		return fmt.Errorf("invalid resource epoch, request: %v, current: %v", reqShardEpoch, resourceEpoch)
	}
	return nil
}

// HandleAskBatchSplit handles the batch split request.
func (c *RaftCluster) HandleAskBatchSplit(request *rpcpb.ProphetRequest) (*rpcpb.AskBatchSplitRsp, error) {
	reqShard := metapb.NewShard()
	err := reqShard.Unmarshal(request.AskBatchSplit.Data)
	if err != nil {
		return nil, err
	}

	splitCount := request.AskBatchSplit.Count
	err = c.ValidRequestShard(reqShard)
	if err != nil {
		return nil, err
	}
	splitIDs := make([]rpcpb.SplitID, 0, splitCount)
	recordShards := make([]uint64, 0, splitCount+1)

	for i := 0; i < int(splitCount); i++ {
		newShardID, err := c.AllocID()
		if err != nil {
			return nil, err
		}

		peerIDs := make([]uint64, len(reqShard.GetReplicas()))
		for i := 0; i < len(peerIDs); i++ {
			if peerIDs[i], err = c.AllocID(); err != nil {
				return nil, err
			}
		}

		recordShards = append(recordShards, newShardID)
		splitIDs = append(splitIDs, rpcpb.SplitID{
			NewID:         newShardID,
			NewReplicaIDs: peerIDs,
		})

		c.logger.Info("ids allocated for resource split",
			zap.Uint64("resource", newShardID),
			zap.Any("peer-ids", peerIDs))
	}

	recordShards = append(recordShards, reqShard.GetID())
	// Disable merge the resources in a period of time.
	c.GetMergeChecker().RecordShardSplit(recordShards)

	// If resource splits during the scheduling process, resources with abnormal
	// status may be left, and these resources need to be checked with higher
	// priority.
	c.AddSuspectShards(recordShards...)

	return &rpcpb.AskBatchSplitRsp{SplitIDs: splitIDs}, nil
}

// HandleCreateShards handle create resources. It will create resources with full replica peers.
func (c *RaftCluster) HandleCreateShards(request *rpcpb.ProphetRequest) (*rpcpb.CreateShardsRsp, error) {
	if len(request.CreateShards.Shards) > 4 {
		return nil, fmt.Errorf("exceed the maximum batch size of create resources, max is %d current %d",
			4, len(request.CreateShards.Shards))
	}

	if request.CreateShards.LeastReplicas == nil {
		request.CreateShards.LeastReplicas = make([]uint64, len(request.CreateShards.Shards))
	}

	c.RLock()
	defer c.RUnlock()

	var createShards []*metapb.Shard
	var leastPeers []int
	for idx, data := range request.CreateShards.Shards {
		res := metapb.NewShard()
		err := res.Unmarshal(data)
		if err != nil {
			return nil, err
		}
		if len(res.GetReplicas()) > 0 {
			return nil, fmt.Errorf("cann't assign peers in create resources")
		}

		// check recreate
		create := true
		for _, cr := range c.core.GetShards() {
			if cr.Meta.GetUnique() == res.GetUnique() {
				create = false
				c.logger.Info("resource already created",
					zap.String("unique", res.GetUnique()))
				break
			}
		}
		if create {
			c.core.ForeachWaittingCreateShards(func(wres *metapb.Shard) {
				if wres.GetUnique() == res.GetUnique() {
					create = false
					c.logger.Info("resource already in waitting create queue",
						zap.String("unique", res.GetUnique()))
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
		res.SetState(metapb.ShardState_Creating)

		_, err = c.core.PreCheckPutShard(core.NewCachedShard(res, nil))
		if err != nil {
			return nil, err
		}
		createShards = append(createShards, res)
		leastPeers = append(leastPeers, int(request.CreateShards.LeastReplicas[idx]))
	}

	for idx, res := range createShards {
		err := c.coordinator.checkers.FillReplicas(core.NewCachedShard(res, nil), leastPeers[idx])
		if err != nil {
			return nil, err
		}

		res.SetEpoch(metapb.ShardEpoch{ConfVer: uint64(len(res.GetReplicas()))})
		for idx := range res.GetReplicas() {
			id, err := c.storage.KV().AllocID()
			if err != nil {
				return nil, err
			}

			res.GetReplicas()[idx].ID = id
			res.GetReplicas()[idx].InitialMember = true
		}

		c.logger.Info("resource created",
			zap.Uint64("resource", res.GetID()),
			zap.Any("peers", res.GetReplicas()))
	}

	err := c.storage.PutShards(createShards...)
	if err != nil {
		return nil, err
	}

	c.core.AddWaittingCreateShards(createShards...)
	c.triggerNotifyCreateShards()
	return &rpcpb.CreateShardsRsp{}, nil
}

// HandleRemoveShards handle remove resources
func (c *RaftCluster) HandleRemoveShards(request *rpcpb.ProphetRequest) (*rpcpb.RemoveShardsRsp, error) {
	if len(request.RemoveShards.IDs) > 4 {
		return nil, fmt.Errorf("exceed the maximum batch size of remove resources, max is %d current %d",
			4, len(request.RemoveShards.IDs))
	}

	c.RLock()
	defer c.RUnlock()

	var targets []*metapb.Shard
	var origin []*metapb.Shard
	for _, id := range request.RemoveShards.IDs {
		if c.core.AlreadyRemoved(id) {
			continue
		}

		v := c.core.GetShard(id)
		if v == nil {
			return nil, fmt.Errorf("resource %d not found in prophet", id)
		}
		origin = append(origin, v.Meta)

		res := v.Meta.Clone() // use cloned value
		res.SetState(metapb.ShardState_Destroyed)
		targets = append(targets, res)
	}
	err := c.storage.PutShards(targets...)
	if err != nil {
		return nil, err
	}

	c.core.AddRemovedShards(request.RemoveShards.IDs...)
	for _, res := range origin {
		res.SetState(metapb.ShardState_Destroyed)
		c.addNotifyLocked(event.NewShardEvent(res, 0, true, false))
	}

	return &rpcpb.RemoveShardsRsp{}, nil
}

// HandleCheckShardState handle check resource state
func (c *RaftCluster) HandleCheckShardState(request *rpcpb.ProphetRequest) (*rpcpb.CheckShardStateRsp, error) {
	c.RLock()
	defer c.RUnlock()

	destroyed, destroying := c.core.GetDestroyShards(util.MustUnmarshalBM64(request.CheckShardState.IDs))
	return &rpcpb.CheckShardStateRsp{
		Destroyed:  util.MustMarshalBM64(destroyed),
		Destroying: util.MustMarshalBM64(destroying),
	}, nil
}

// HandlePutPlacementRule handle put placement rule
func (c *RaftCluster) HandlePutPlacementRule(request *rpcpb.ProphetRequest) error {
	return c.GetRuleManager().SetRule(placement.NewRuleFromRPC(request.PutPlacementRule.Rule))
}

// HandleAppliedRules handle get applied rules
func (c *RaftCluster) HandleAppliedRules(request *rpcpb.ProphetRequest) (*rpcpb.GetAppliedRulesRsp, error) {
	res := c.GetShard(request.GetAppliedRules.ShardID)
	if res == nil {
		return nil, fmt.Errorf("resource %d not found", request.GetAppliedRules.ShardID)
	}

	rules := c.GetRuleManager().GetRulesForApplyShard(res)
	return &rpcpb.GetAppliedRulesRsp{
		Rules: placement.RPCRules(rules),
	}, nil
}

func (c *RaftCluster) HandleAddScheduleGroupRule(request *rpcpb.ProphetRequest) error {
	c.RLock()
	defer c.RUnlock()

	id, err := c.AllocID()
	if err != nil {
		return err
	}
	request.AddScheduleGroupRule.Rule.ID = id
	if !c.core.AddScheduleGroupRule(request.AddScheduleGroupRule.Rule) {
		return nil
	}
	return c.storage.PutScheduleGroupRule(request.AddScheduleGroupRule.Rule)
}

func (c *RaftCluster) HandleGetScheduleGroupRule(request *rpcpb.ProphetRequest) ([]metapb.ScheduleGroupRule, error) {
	c.RLock()
	defer c.RUnlock()
	return c.core.ScheduleGroupRules, nil
}

func (c *RaftCluster) triggerNotifyCreateShards() {
	if c.createShardC != nil {
		select {
		case c.createShardC <- struct{}{}:
		default:
		}
	}
}

func (c *RaftCluster) doNotifyCreateShards() {
	c.core.ForeachWaittingCreateShards(func(res *metapb.Shard) {
		c.addNotifyLocked(event.NewShardEvent(res, 0, false, true))
	})
}

func (c *RaftCluster) getDestroyingStatusLocked(id uint64) (*metapb.DestroyingStatus, error) {
	status := c.core.GetDestroyingStatus(id)
	if status != nil {
		return status, nil
	}

	v, err := c.storage.GetShardExtra(id)
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
	if status.State == metapb.ShardState_Destroyed {
		res := c.core.GetShard(id)
		if res == nil {
			c.logger.Fatal("missing resource to set destroyed",
				zap.Uint64("resource", id))
			return nil
		}

		v := res.Meta.Clone()
		v.SetState(metapb.ShardState_Destroyed)
		if err := c.storage.PutShardAndExtra(v, protoc.MustMarshal(status)); err != nil {
			return err
		}
		c.core.AddRemovedShards(id)
		res.Lock()
		defer res.Unlock()
		res.Meta.SetState(metapb.ShardState_Destroyed)
	} else {
		err := c.storage.PutShardExtra(id, protoc.MustMarshal(status))
		if err != nil {
			return err
		}
	}

	c.core.UpdateDestroyingStatus(id, status)
	return nil
}
