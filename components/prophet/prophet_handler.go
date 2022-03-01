// Copyright 2020 MatrixOrigin.
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

package prophet

import (
	"errors"
	"fmt"
	"github.com/fagongzi/goetty"
	"github.com/matrixorigin/matrixcube/components/prophet/cluster"
	"github.com/matrixorigin/matrixcube/components/prophet/core"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"go.uber.org/zap"
)

type heartbeatStream struct {
	containerID uint64
	rs          goetty.IOSession
}

func (hs heartbeatStream) Send(resp *rpcpb.ShardHeartbeatRsp) error {
	rsp := &rpcpb.ProphetResponse{}
	rsp.Type = rpcpb.TypeShardHeartbeatRsp
	rsp.ShardHeartbeat = *resp
	return hs.rs.WriteAndFlush(rsp)
}

func (p *defaultProphet) handleRPCRequest(rs goetty.IOSession, data interface{}, received uint64) error {
	req := data.(*rpcpb.ProphetRequest)
	if req.Type == rpcpb.TypeRegisterStore {
		p.hbStreams.BindStream(req.StoreID, &heartbeatStream{containerID: req.StoreID, rs: rs})
		p.logger.Info("heartbeat stream binded",
			zap.Uint64("contianer", req.StoreID))
		return nil
	}

	p.logger.Debug("rpc request received",
		zap.Uint64("id", req.ID),
		zap.String("from", rs.RemoteAddr()),
		zap.String("type", req.Type.String()))

	if p.cfg.Prophet.DisableResponse {
		p.logger.Debug("skip response")
		return nil
	}

	doResponse := true
	resp := &rpcpb.ProphetResponse{}
	resp.ID = req.ID
	rc := p.GetRaftCluster()
	if p.cfg.Prophet.EnableResponseNotLeader || rc == nil || (p.member != nil && !p.member.IsLeader()) {
		resp.Error = util.ErrNotLeader.Error()
		resp.Leader = p.member.GetLeader().GetAddr()
		return rs.WriteAndFlush(resp)
	}

	switch req.Type {
	case rpcpb.TypePutStoreReq:
		resp.Type = rpcpb.TypePutStoreRsp
		err := p.handlePutStore(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeShardHeartbeatReq:
		resp.Type = rpcpb.TypeShardHeartbeatRsp
		err := p.handleShardHeartbeat(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeStoreHeartbeatReq:
		resp.Type = rpcpb.TypeStoreHeartbeatRsp
		err := p.handleStoreHeartbeat(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeCreateDestroyingReq:
		resp.Type = rpcpb.TypeCreateDestroyingRsp
		err := p.handleCreateDestroying(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeReportDestroyedReq:
		resp.Type = rpcpb.TypeReportDestroyedRsp
		err := p.handleReportDestroyed(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeGetDestroyingReq:
		resp.Type = rpcpb.TypeGetDestroyingRsp
		err := p.handleGetDestroying(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeAllocIDReq:
		resp.Type = rpcpb.TypeAllocIDRsp
		err := p.handleAllocID(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeGetStoreReq:
		resp.Type = rpcpb.TypeGetStoreRsp
		err := p.handleGetStore(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeAskBatchSplitReq:
		resp.Type = rpcpb.TypeAskBatchSplitRsp
		err := p.handleAskBatchSplit(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeCreateWatcherReq:
		resp.Type = rpcpb.TypeEventNotify
		p.mu.RLock()
		wn := p.mu.wn
		p.mu.RUnlock()
		if wn != nil {
			err := wn.handleCreateWatcher(req, resp, rs)
			if err != nil {
				return err
			}
		} else {
			return fmt.Errorf("leader not init completed")
		}
	case rpcpb.TypeCreateShardsReq:
		resp.Type = rpcpb.TypeCreateShardsRsp
		err := p.handleCreateShards(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeRemoveShardsReq:
		resp.Type = rpcpb.TypeRemoveShardsRsp
		err := p.handleRemoveShards(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeCheckShardStateReq:
		resp.Type = rpcpb.TypeCheckShardStateRsp
		err := p.handleCheckShardState(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypePutPlacementRuleReq:
		resp.Type = rpcpb.TypePutPlacementRuleRsp
		err := p.handlePutPlacementRule(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeGetAppliedRulesReq:
		resp.Type = rpcpb.TypeGetAppliedRulesRsp
		err := p.handleGetAppliedRule(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeCreateJobReq:
		resp.Type = rpcpb.TypeCreateJobRsp
		err := p.handleCreateJob(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeRemoveJobReq:
		resp.Type = rpcpb.TypeCreateJobRsp
		err := p.handleRemoveJob(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeExecuteJobReq:
		resp.Type = rpcpb.TypeExecuteJobRsp
		err := p.handleExecuteJob(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeAddScheduleGroupRuleReq:
		resp.Type = rpcpb.TypeAddScheduleGroupRuleRsp
		err := p.handleAddScheduleGroupRule(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeGetScheduleGroupRuleReq:
		resp.Type = rpcpb.TypeGetScheduleGroupRuleRsp
		err := p.handleGetScheduleGroupRule(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	default:
		return fmt.Errorf("type %s not support", req.Type.String())
	}

	if doResponse {
		p.logger.Debug("send rpc response",
			zap.Uint64("id", req.ID),
			zap.String("to", rs.RemoteAddr()),
			zap.String("type", req.Type.String()))
		return rs.WriteAndFlush(resp)
	}

	return nil
}

func (p *defaultProphet) handlePutStore(rc *cluster.RaftCluster, req *rpcpb.ProphetRequest, resp *rpcpb.ProphetResponse) error {
	meta := metapb.Store{}
	err := meta.Unmarshal(req.PutStore.Store)
	if err != nil {
		return err
	}

	if err := checkStore(rc, meta.GetID()); err != nil {
		return err
	}

	if err := rc.PutStore(meta); err != nil {
		return err
	}

	return nil
}

func (p *defaultProphet) handleShardHeartbeat(rc *cluster.RaftCluster, req *rpcpb.ProphetRequest, resp *rpcpb.ProphetResponse) error {
	meta := metapb.Shard{}
	err := meta.Unmarshal(req.ShardHeartbeat.Shard)
	if err != nil {
		return err
	}

	storeID := req.ShardHeartbeat.GetLeader().GetStoreID()
	store := rc.GetStore(storeID)
	if store == nil {
		return fmt.Errorf("invalid contianer ID %d, not found", storeID)
	}

	res := core.ShardFromHeartbeat(req.ShardHeartbeat, meta)
	if res.GetLeader() == nil {
		err := errors.New("invalid request, the leader is nil")
		p.logger.Error("invalid request, the leader is nil")
		return err
	}
	if res.Meta.GetID() == 0 {
		return fmt.Errorf("invalid request resource, %v", res.Meta)
	}

	// If the resource peer count is 0, then we should not handle this.
	if len(res.Meta.GetReplicas()) == 0 {
		err := errors.New("invalid resource, zero resource peer count")
		p.logger.Warn("invalid resource, zero resource peer count",
			zap.Uint64("resource", res.Meta.GetID()))
		return err
	}

	return rc.HandleShardHeartbeat(res)
}

func (p *defaultProphet) handleStoreHeartbeat(rc *cluster.RaftCluster, req *rpcpb.ProphetRequest, resp *rpcpb.ProphetResponse) error {
	if err := checkStore(rc, req.StoreHeartbeat.Stats.StoreID); err != nil {
		return err
	}

	err := rc.HandleStoreHeartbeat(&req.StoreHeartbeat.Stats)
	if err != nil {
		return err
	}

	if p.cfg.Prophet.StoreHeartbeatDataProcessor != nil {
		data, err := p.cfg.Prophet.StoreHeartbeatDataProcessor.HandleHeartbeatReq(req.StoreHeartbeat.Stats.StoreID,
			req.StoreHeartbeat.Data, p.GetStorage())
		if err != nil {
			return err
		}
		resp.StoreHeartbeat.Data = data
	}

	return nil
}

func (p *defaultProphet) handleCreateDestroying(rc *cluster.RaftCluster, req *rpcpb.ProphetRequest, resp *rpcpb.ProphetResponse) error {
	state, err := rc.HandleCreateDestroying(req.CreateDestroying)
	if err != nil {
		return err
	}
	resp.CreateDestroying.State = state
	return nil
}

func (p *defaultProphet) handleGetDestroying(rc *cluster.RaftCluster, req *rpcpb.ProphetRequest, resp *rpcpb.ProphetResponse) error {
	status, err := rc.HandleGetDestroying(req.GetDestroying)
	if err != nil {
		return err
	}
	resp.GetDestroying.Status = status
	return nil
}

func (p *defaultProphet) handleReportDestroyed(rc *cluster.RaftCluster, req *rpcpb.ProphetRequest, resp *rpcpb.ProphetResponse) error {
	state, err := rc.HandleReportDestroyed(req.ReportDestroyed)
	if err != nil {
		return err
	}
	resp.ReportDestroyed.State = state
	return nil
}

func (p *defaultProphet) handleGetStore(rc *cluster.RaftCluster, req *rpcpb.ProphetRequest, resp *rpcpb.ProphetResponse) error {
	storeID := req.GetStore.ID
	store := rc.GetStore(storeID)
	if store == nil {
		return fmt.Errorf("invalid container ID %d, not found", storeID)
	}

	data, err := store.Meta.Marshal()
	if err != nil {
		return err
	}

	resp.GetStore.Data = data
	resp.GetStore.Stats = store.GetStoreStats()
	return nil
}

func (p *defaultProphet) handleAllocID(rc *cluster.RaftCluster, req *rpcpb.ProphetRequest, resp *rpcpb.ProphetResponse) error {
	id, err := p.storage.KV().AllocID()
	if err != nil {
		return err
	}

	resp.AllocID.ID = id
	return nil
}

func (p *defaultProphet) handleAskBatchSplit(rc *cluster.RaftCluster, req *rpcpb.ProphetRequest, resp *rpcpb.ProphetResponse) error {
	split, err := rc.HandleAskBatchSplit(req)
	if err != nil {
		return err
	}

	resp.AskBatchSplit = *split
	return nil
}

func (p *defaultProphet) handleCreateShards(rc *cluster.RaftCluster, req *rpcpb.ProphetRequest, resp *rpcpb.ProphetResponse) error {
	rsp, err := rc.HandleCreateShards(req)
	if err != nil {
		return err
	}

	resp.CreateShards = *rsp
	return nil
}

func (p *defaultProphet) handleRemoveShards(rc *cluster.RaftCluster, req *rpcpb.ProphetRequest, resp *rpcpb.ProphetResponse) error {
	rsp, err := rc.HandleRemoveShards(req)
	if err != nil {
		return err
	}

	resp.RemoveShards = *rsp
	return nil
}

func (p *defaultProphet) handleCheckShardState(rc *cluster.RaftCluster, req *rpcpb.ProphetRequest, resp *rpcpb.ProphetResponse) error {
	rsp, err := rc.HandleCheckShardState(req)
	if err != nil {
		return err
	}

	resp.CheckShardState = *rsp
	return nil
}

func (p *defaultProphet) handlePutPlacementRule(rc *cluster.RaftCluster, req *rpcpb.ProphetRequest, resp *rpcpb.ProphetResponse) error {
	return rc.HandlePutPlacementRule(req)
}

func (p *defaultProphet) handleGetAppliedRule(rc *cluster.RaftCluster, req *rpcpb.ProphetRequest, resp *rpcpb.ProphetResponse) error {
	rsp, err := rc.HandleAppliedRules(req)
	if err != nil {
		return err
	}

	resp.GetAppliedRules = *rsp
	return nil
}

func (p *defaultProphet) handleAddScheduleGroupRule(rc *cluster.RaftCluster, req *rpcpb.ProphetRequest, resp *rpcpb.ProphetResponse) error {
	err := rc.HandleAddScheduleGroupRule(req)
	if err != nil {
		return err
	}
	return nil
}

func (p *defaultProphet) handleGetScheduleGroupRule(rc *cluster.RaftCluster, req *rpcpb.ProphetRequest, resp *rpcpb.ProphetResponse) error {
	rules, err := rc.HandleGetScheduleGroupRule(req)
	if err != nil {
		return err
	}
	resp.GetScheduleGroupRule.Rules = rules
	return nil
}

// checkStore returns an error response if the store exists and is in tombstone state.
// It returns nil if it can't get the store.
func checkStore(rc *cluster.RaftCluster, storeID uint64) error {
	store := rc.GetStore(storeID)
	if store != nil {
		if store.GetState() == metapb.StoreState_StoreTombstone {
			return errors.New("container is tombstone")
		}
	}
	return nil
}
