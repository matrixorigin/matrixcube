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
	"github.com/matrixorigin/matrixcube/components/prophet/pb/metapb"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"go.uber.org/zap"
)

type heartbeatStream struct {
	containerID uint64
	rs          goetty.IOSession
}

func (hs heartbeatStream) Send(resp *rpcpb.ResourceHeartbeatRsp) error {
	rsp := &rpcpb.Response{}
	rsp.Type = rpcpb.TypeResourceHeartbeatRsp
	rsp.ResourceHeartbeat = *resp
	return hs.rs.WriteAndFlush(rsp)
}

func (p *defaultProphet) handleRPCRequest(rs goetty.IOSession, data interface{}, received uint64) error {
	req := data.(*rpcpb.Request)
	if req.Type == rpcpb.TypeRegisterContainer {
		p.hbStreams.BindStream(req.ContainerID, &heartbeatStream{containerID: req.ContainerID, rs: rs})
		p.logger.Info("heartbeat stream binded",
			zap.Uint64("contianer", req.ContainerID))
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
	resp := &rpcpb.Response{}
	resp.ID = req.ID
	rc := p.GetRaftCluster()
	if p.cfg.Prophet.EnableResponseNotLeader || rc == nil || (p.member != nil && !p.member.IsLeader()) {
		resp.Error = util.ErrNotLeader.Error()
		resp.Leader = p.member.GetLeader().GetAddr()
		return rs.WriteAndFlush(resp)
	}

	switch req.Type {
	case rpcpb.TypePutContainerReq:
		resp.Type = rpcpb.TypePutContainerRsp
		err := p.handlePutContainer(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeResourceHeartbeatReq:
		resp.Type = rpcpb.TypeResourceHeartbeatRsp
		err := p.handleResourceHeartbeat(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeContainerHeartbeatReq:
		resp.Type = rpcpb.TypeContainerHeartbeatRsp
		err := p.handleContainerHeartbeat(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeCreateDestoryingReq:
		resp.Type = rpcpb.TypeCreateDestoryingRsp
		err := p.handleCreateDestorying(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeReportDestoryedReq:
		resp.Type = rpcpb.TypeReportDestoryedRsp
		err := p.handleReportDestoryed(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeGetDestoryingReq:
		resp.Type = rpcpb.TypeGetDestoryingRsp
		err := p.handleGetDestorying(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeAllocIDReq:
		resp.Type = rpcpb.TypeAllocIDRsp
		err := p.handleAllocID(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeGetContainerReq:
		resp.Type = rpcpb.TypeGetContainerRsp
		err := p.handleGetContainer(rc, req, resp)
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
	case rpcpb.TypeCreateResourcesReq:
		resp.Type = rpcpb.TypeCreateResourcesRsp
		err := p.handleCreateResources(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeRemoveResourcesReq:
		resp.Type = rpcpb.TypeRemoveResourcesRsp
		err := p.handleRemoveResources(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}
	case rpcpb.TypeCheckResourceStateReq:
		resp.Type = rpcpb.TypeCheckResourceStateRsp
		err := p.handleCheckResourceState(rc, req, resp)
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

func (p *defaultProphet) handlePutContainer(rc *cluster.RaftCluster, req *rpcpb.Request, resp *rpcpb.Response) error {
	meta := p.cfg.Prophet.Adapter.NewContainer()
	err := meta.Unmarshal(req.PutContainer.Container)
	if err != nil {
		return err
	}

	if err := checkContainer(rc, meta.ID()); err != nil {
		return err
	}

	if err := rc.PutContainer(meta); err != nil {
		return err
	}

	return nil
}

func (p *defaultProphet) handleResourceHeartbeat(rc *cluster.RaftCluster, req *rpcpb.Request, resp *rpcpb.Response) error {
	meta := p.cfg.Prophet.Adapter.NewResource()
	err := meta.Unmarshal(req.ResourceHeartbeat.Resource)
	if err != nil {
		return err
	}

	storeID := req.ResourceHeartbeat.GetLeader().GetContainerID()
	store := rc.GetContainer(storeID)
	if store == nil {
		return fmt.Errorf("invalid contianer ID %d, not found", storeID)
	}

	res := core.ResourceFromHeartbeat(req.ResourceHeartbeat, meta)
	if res.GetLeader() == nil {
		err := errors.New("invalid request, the leader is nil")
		p.logger.Error("invalid request, the leader is nil")
		return err
	}
	if res.Meta.ID() == 0 {
		return fmt.Errorf("invalid request resource, %v", res.Meta)
	}

	// If the resource peer count is 0, then we should not handle this.
	if len(res.Meta.Peers()) == 0 {
		err := errors.New("invalid resource, zero resource peer count")
		p.logger.Warn("invalid resource, zero resource peer count",
			zap.Uint64("resource", res.Meta.ID()))
		return err
	}

	return rc.HandleResourceHeartbeat(res)
}

func (p *defaultProphet) handleContainerHeartbeat(rc *cluster.RaftCluster, req *rpcpb.Request, resp *rpcpb.Response) error {
	if err := checkContainer(rc, req.ContainerHeartbeat.Stats.ContainerID); err != nil {
		return err
	}

	err := rc.HandleContainerHeartbeat(&req.ContainerHeartbeat.Stats)
	if err != nil {
		return err
	}

	if p.cfg.Prophet.ContainerHeartbeatDataProcessor != nil {
		data, err := p.cfg.Prophet.ContainerHeartbeatDataProcessor.HandleHeartbeatReq(req.ContainerHeartbeat.Stats.ContainerID,
			req.ContainerHeartbeat.Data, p.GetStorage())
		if err != nil {
			return err
		}
		resp.ContainerHeartbeat.Data = data
	}

	return nil
}

func (p *defaultProphet) handleCreateDestorying(rc *cluster.RaftCluster, req *rpcpb.Request, resp *rpcpb.Response) error {
	state, err := rc.HandleCreateDestorying(req.CreateDestorying)
	if err != nil {
		return err
	}
	resp.CreateDestorying.State = state
	return nil
}

func (p *defaultProphet) handleGetDestorying(rc *cluster.RaftCluster, req *rpcpb.Request, resp *rpcpb.Response) error {
	status, err := rc.HandleGetDestorying(req.GetDestorying)
	if err != nil {
		return err
	}
	resp.GetDestorying.Status = status
	return nil
}

func (p *defaultProphet) handleReportDestoryed(rc *cluster.RaftCluster, req *rpcpb.Request, resp *rpcpb.Response) error {
	state, err := rc.HandleReportDestoryed(req.ReportDestoryed)
	if err != nil {
		return err
	}
	resp.ReportDestoryed.State = state
	return nil
}

func (p *defaultProphet) handleGetContainer(rc *cluster.RaftCluster, req *rpcpb.Request, resp *rpcpb.Response) error {
	storeID := req.GetContainer.ID
	store := rc.GetContainer(storeID)
	if store == nil {
		return fmt.Errorf("invalid container ID %d, not found", storeID)
	}

	data, err := store.Meta.Marshal()
	if err != nil {
		return err
	}

	resp.GetContainer.Data = data
	resp.GetContainer.Stats = store.GetContainerStats()
	return nil
}

func (p *defaultProphet) handleAllocID(rc *cluster.RaftCluster, req *rpcpb.Request, resp *rpcpb.Response) error {
	start, end, err := p.storage.KV().AllocID(req.AllocID.Count)
	if err != nil {
		return err
	}

	resp.AllocID.Start = start
	resp.AllocID.End = end
	return nil
}

func (p *defaultProphet) handleAskBatchSplit(rc *cluster.RaftCluster, req *rpcpb.Request, resp *rpcpb.Response) error {
	split, err := rc.HandleAskBatchSplit(req)
	if err != nil {
		return err
	}

	resp.AskBatchSplit = *split
	return nil
}

func (p *defaultProphet) handleCreateResources(rc *cluster.RaftCluster, req *rpcpb.Request, resp *rpcpb.Response) error {
	rsp, err := rc.HandleCreateResources(req)
	if err != nil {
		return err
	}

	resp.CreateResources = *rsp
	return nil
}

func (p *defaultProphet) handleRemoveResources(rc *cluster.RaftCluster, req *rpcpb.Request, resp *rpcpb.Response) error {
	rsp, err := rc.HandleRemoveResources(req)
	if err != nil {
		return err
	}

	resp.RemoveResources = *rsp
	return nil
}

func (p *defaultProphet) handleCheckResourceState(rc *cluster.RaftCluster, req *rpcpb.Request, resp *rpcpb.Response) error {
	rsp, err := rc.HandleCheckResourceState(req)
	if err != nil {
		return err
	}

	resp.CheckResourceState = *rsp
	return nil
}

func (p *defaultProphet) handlePutPlacementRule(rc *cluster.RaftCluster, req *rpcpb.Request, resp *rpcpb.Response) error {
	return rc.HandlePutPlacementRule(req)
}

func (p *defaultProphet) handleGetAppliedRule(rc *cluster.RaftCluster, req *rpcpb.Request, resp *rpcpb.Response) error {
	rsp, err := rc.HandleAppliedRules(req)
	if err != nil {
		return err
	}

	resp.GetAppliedRules = *rsp
	return nil
}

// checkContainer returns an error response if the store exists and is in tombstone state.
// It returns nil if it can't get the store.
func checkContainer(rc *cluster.RaftCluster, storeID uint64) error {
	store := rc.GetContainer(storeID)
	if store != nil {
		if store.GetState() == metapb.ContainerState_Tombstone {
			return errors.New("container is tombstone")
		}
	}
	return nil
}
