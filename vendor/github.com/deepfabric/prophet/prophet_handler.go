package prophet

import (
	"errors"
	"fmt"

	"github.com/deepfabric/prophet/cluster"
	"github.com/deepfabric/prophet/core"
	"github.com/deepfabric/prophet/pb/metapb"
	"github.com/deepfabric/prophet/pb/rpcpb"
	"github.com/deepfabric/prophet/util"
	"github.com/fagongzi/goetty"
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
	if received == 1 {
		p.hbStreams.BindStream(req.ContainerID, &heartbeatStream{containerID: req.ContainerID, rs: rs})
	}

	util.GetLogger().Debugf("%s received %+v, %+v", p.cfg.Name, req, p.member)

	if p.cfg.DisableResponse {
		util.GetLogger().Debugf("%s skip response", p.cfg.Name)
		return nil
	}

	doResponse := true
	resp := &rpcpb.Response{}
	resp.ID = req.ID
	rc := p.GetRaftCluster()
	if rc == nil || !p.member.IsLeader() {
		resp.Error = util.ErrNotLeader.Error()
		resp.Leader = p.member.GetLeader().GetAddr()
		return rs.WriteAndFlush(resp)
	}

	switch req.Type {
	case rpcpb.TypeResourceHeartbeatReq:
		resp.Type = rpcpb.TypeResourceHeartbeatRsp
		err := p.handleResourceHeartbeat(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}

		break
	case rpcpb.TypeContainerHeartbeatReq:
		resp.Type = rpcpb.TypeContainerHeartbeatRsp
		err := p.handleContainerHeartbeat(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}

		break
	case rpcpb.TypeAllocIDReq:
		resp.Type = rpcpb.TypeAllocIDRsp
		err := p.handleAllocID(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}

		break
	case rpcpb.TypeGetContainerReq:
		resp.Type = rpcpb.TypeGetContainerRsp
		err := p.handleGetContainer(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}

		break
	case rpcpb.TypeAskSplitReq:
		resp.Type = rpcpb.TypeAskSplitRsp
		err := p.handleAskSplit(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}

		break
	case rpcpb.TypeReportSplitReq:
		resp.Type = rpcpb.TypeReportSplitRsp
		err := p.handleReportSplit(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}

		break
	case rpcpb.TypeAskBatchSplitReq:
		resp.Type = rpcpb.TypeAskBatchSplitRsp
		err := p.handleAskBatchSplit(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}

		break
	case rpcpb.TypeBatchReportSplitReq:
		resp.Type = rpcpb.TypeBatchReportSplitRsp
		err := p.handleReportBatchSplit(rc, req, resp)
		if err != nil {
			resp.Error = err.Error()
		}

		break
	case rpcpb.TypeCreateWatcherReq:
		doResponse = false
		if p.wn != nil {
			err := p.wn.handleCreateWatcher(req, resp, rs)
			if err != nil {
				return err
			}
		}

		break
	default:
		return fmt.Errorf("type %s not support", req.Type.String())
	}

	if doResponse {
		util.GetLogger().Debugf("%s response %+v",
			p.cfg.Name, req)
		return rs.Write(resp)
	}

	return nil
}

func (p *defaultProphet) handleResourceHeartbeat(rc *cluster.RaftCluster, req *rpcpb.Request, resp *rpcpb.Response) error {
	meta := p.cfg.Adapter.NewResource()
	err := meta.Unmarshal(req.ResourceHeartbeat.Resource)
	if err != nil {
		return err
	}

	storeID := req.ResourceHeartbeat.GetLeader().GetContainerID()
	store := rc.GetContainer(storeID)
	if store == nil {
		return fmt.Errorf("invalid contianer ID %d, not found", storeID)
	}

	region := core.ResourceFromHeartbeat(req.ResourceHeartbeat, meta)
	if region.GetLeader() == nil {
		err := errors.New("invalid request, the leader is nil")
		util.GetLogger().Errorf("invalid request, the leader is nil")
		return err
	}
	if region.Meta.ID() == 0 {
		return fmt.Errorf("invalid request resource, %v", region.Meta)
	}

	// If the resource peer count is 0, then we should not handle this.
	if len(region.Meta.Peers()) == 0 {
		err := errors.New("invalid resource, zero resource peer count")
		util.GetLogger().Warningf("invalid resource %+v, zero resource peer count", region.Meta)
		return err
	}

	return rc.HandleResourceHeartbeat(region)
}

func (p *defaultProphet) handleContainerHeartbeat(rc *cluster.RaftCluster, req *rpcpb.Request, resp *rpcpb.Response) error {
	if err := checkContainer(rc, req.ContainerHeartbeat.Stats.ContainerID); err != nil {
		return err
	}

	err := rc.HandleContainerHeartbeat(&req.ContainerHeartbeat.Stats)
	if err != nil {
		return err
	}

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
	id, err := p.storage.KV().AllocID()
	if err != nil {
		return err
	}

	resp.AllocID.ID = id
	return nil
}

func (p *defaultProphet) handleAskSplit(rc *cluster.RaftCluster, req *rpcpb.Request, resp *rpcpb.Response) error {
	split, err := rc.HandleAskSplit(req)
	if err != nil {
		return err
	}

	resp.AskSplit = *split
	return nil
}

func (p *defaultProphet) handleReportSplit(rc *cluster.RaftCluster, req *rpcpb.Request, resp *rpcpb.Response) error {
	_, err := rc.HandleReportSplit(req)
	if err != nil {
		return err
	}

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

func (p *defaultProphet) handleReportBatchSplit(rc *cluster.RaftCluster, req *rpcpb.Request, resp *rpcpb.Response) error {
	_, err := rc.HandleBatchReportSplit(req)
	if err != nil {
		return err
	}
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
	return fmt.Errorf("container %v not found", storeID)
}
