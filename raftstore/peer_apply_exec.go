package raftstore

import (
	"bytes"
	"encoding/hex"
	"errors"

	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/bhmetapb"
	"github.com/deepfabric/beehive/pb/bhraftpb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/prophet/pb/metapb"
)

func (d *applyDelegate) execAdminRequest(ctx *applyContext) (*raftcmdpb.RaftCMDResponse, *execResult, error) {
	cmdType := ctx.req.AdminRequest.CmdType
	switch cmdType {
	case raftcmdpb.AdminCmdType_ChangePeer:
		return d.doExecChangePeer(ctx)
	case raftcmdpb.AdminCmdType_BatchSplit:
		return d.doExecSplit(ctx)
	case raftcmdpb.AdminCmdType_CompactLog:
		return d.doExecCompactRaftLog(ctx)
	}

	return nil, nil, nil
}

func (d *applyDelegate) doExecChangePeer(ctx *applyContext) (*raftcmdpb.RaftCMDResponse, *execResult, error) {
	req := ctx.req.AdminRequest.ChangePeer
	logger.Infof("shard %d do apply %s peer %+v at epoch %+v",
		d.shard.ID,
		req.ChangeType.String(),
		req.Peer,
		d.shard.Epoch)

	exists := findPeer(&d.shard, req.Peer.ContainerID)
	switch req.ChangeType {
	case raftcmdpb.ConfChangeType_AddNode:
		if exists != nil {
			ctx.metrics.admin.confChangeReject++
			logger.Infof("shard %d add peer %+v skipped, already added",
				d.shard.ID,
				req.Peer)
			return nil, nil, nil
		}

		d.shard.Peers = append(d.shard.Peers, req.Peer)
		ctx.metrics.admin.addPeerSucceed++
		d.shard.Epoch.ConfVer++
		logger.Infof("shard %d added new peer %+v at epoch %+v",
			d.shard.ID,
			req.Peer,
			d.shard.Epoch)
	case raftcmdpb.ConfChangeType_RemoveNode:
		if exists == nil {
			ctx.metrics.admin.confChangeReject++
			logger.Infof("shard %d remove peer %+v skipped, already removed",
				d.shard.ID,
				req.Peer)
			return nil, nil, nil
		}

		// Remove ourself, we will destroy all shard data later.
		// So we need not to apply following logs.
		if d.peerID == req.Peer.ID {
			d.setPendingRemove()
		}

		removePeer(&d.shard, req.Peer.ContainerID)
		ctx.metrics.admin.removePeerSucceed++
		d.shard.Epoch.ConfVer++
		logger.Infof("shard %d removed a peer %+v at epoch %+v",
			d.shard.ID,
			req.Peer,
			d.shard.Epoch)
	}

	state := bhraftpb.PeerState_Normal
	if d.isPendingRemove() {
		state = bhraftpb.PeerState_Tombstone
	}

	err := d.ps.updatePeerState(d.shard, state, ctx.raftStateWB)
	if err != nil {
		logger.Fatalf("shard %d update db state failed, errors:\n %+v",
			d.shard.ID,
			err)
	}

	resp := newAdminRaftCMDResponse(raftcmdpb.AdminCmdType_ChangePeer, &raftcmdpb.ChangePeerResponse{
		Shard: d.shard,
	})

	result := &execResult{
		adminType: raftcmdpb.AdminCmdType_ChangePeer,
		// confChange set by applyConfChange
		changePeer: &changePeer{
			peer:  req.Peer,
			shard: d.shard,
		},
	}

	return resp, result, nil
}

func (d *applyDelegate) doExecSplit(ctx *applyContext) (*raftcmdpb.RaftCMDResponse, *execResult, error) {
	ctx.metrics.admin.split++
	req := ctx.req.AdminRequest.Splits

	if len(req.SplitKey) == 0 {
		logger.Errorf("shard %d missing split key",
			d.shard.ID)
		return nil, nil, errors.New("missing split key")
	}

	req.SplitKey = DecodeDataKey(req.SplitKey)

	// splitKey < shard.Startkey
	if bytes.Compare(req.SplitKey, d.shard.Start) < 0 {
		logger.Errorf("shard %d invalid split key, split=<%+v> shard-start=<%+v>",
			d.shard.ID,
			req.SplitKey,
			d.shard.Start)
		return nil, nil, nil
	}

	peer := checkKeyInShard(req.SplitKey, &d.shard)
	if peer != nil {
		logger.Errorf("shard %d split key not in shard, errors:\n %+v",
			d.shard.ID,
			peer)
		return nil, nil, nil
	}

	if len(req.NewPeerIDs) != len(d.shard.Peers) {
		logger.Errorf("shard %d invalid new peer id count, splitCount=<%d> currentCount=<%d>",
			d.shard.ID,
			len(req.NewPeerIDs),
			len(d.shard.Peers))

		return nil, nil, nil
	}

	logger.Infof("shard %d split, splitKey=<%d> shard=<%+v>",
		d.shard.ID,
		req.SplitKey,
		d.shard)

	// After split, the origin shard key range is [start_key, split_key),
	// the new split shard is [split_key, end).
	newShard := bhmetapb.Shard{
		ID:    req.NewShardID,
		Epoch: d.shard.Epoch,
		Start: req.SplitKey,
		End:   d.shard.End,
		Group: d.shard.Group,
	}
	d.shard.End = req.SplitKey

	for idx, id := range req.NewPeerIDs {
		newShard.Peers = append(newShard.Peers, metapb.Peer{
			ID:      id,
			StoreID: d.shard.Peers[idx].ContainerID,
		})
	}

	d.shard.Epoch.Version++
	newShard.Epoch.Version = d.shard.Epoch.Version

	if d.store.cfg.Customize.CustomSplitCompletedFunc != nil {
		d.store.cfg.Customize.CustomSplitCompletedFunc(&d.shard, &newShard)
	}

	err := d.ps.updatePeerState(d.shard, bhraftpb.PeerState_Normal, ctx.raftStateWB)

	d.wb.Reset()
	if err == nil {
		err = d.ps.updatePeerState(newShard, bhraftpb.PeerState_Normal, d.wb)
	}

	if err == nil {
		err = d.ps.writeInitialState(newShard.ID, d.wb)
	}
	if err != nil {
		logger.Fatalf("shard %d save split shard failed, newShard=<%+v> errors:\n %+v",
			d.shard.ID,
			newShard,
			err)
	}

	err = d.ps.store.MetadataStorage().Write(d.wb, false)
	if err != nil {
		logger.Fatalf("shard %d commit apply result failed, errors:\n %+v",
			d.shard.ID,
			err)
	}

	rsp := newAdminRaftCMDResponse(raftcmdpb.AdminCmdType_BatchSplit, &raftcmdpb.SplitResponse{
		Left:  d.shard,
		Right: newShard,
	})

	result := &execResult{
		adminType: raftcmdpb.AdminCmdType_BatchSplit,
		splitResult: &splitResult{
			left:  d.shard,
			right: newShard,
		},
	}

	ctx.metrics.admin.splitSucceed++
	return rsp, result, nil
}

func (d *applyDelegate) doExecCompactRaftLog(ctx *applyContext) (*raftcmdpb.RaftCMDResponse, *execResult, error) {
	ctx.metrics.admin.compact++

	req := ctx.req.AdminRequest.CompactLog
	compactIndex := req.CompactIndex
	firstIndex := ctx.applyState.TruncatedState.Index + 1

	if compactIndex <= firstIndex {
		return nil, nil, nil
	}

	compactTerm := req.CompactTerm
	if compactTerm == 0 {
		return nil, nil, errors.New("command format is outdated, please upgrade leader")
	}

	err := compactRaftLog(d.shard.ID, &ctx.applyState, compactIndex, compactTerm)
	if err != nil {
		return nil, nil, err
	}

	rsp := newAdminRaftCMDResponse(raftcmdpb.AdminCmdType_CompactLog, &raftcmdpb.CompactLogRequest{})
	result := &execResult{
		adminType: raftcmdpb.AdminCmdType_CompactLog,
		raftGCResult: &raftGCResult{
			state:      ctx.applyState.TruncatedState,
			firstIndex: firstIndex,
		},
	}

	ctx.metrics.admin.compactSucceed++
	return rsp, result, nil
}

func (d *applyDelegate) execWriteRequest(ctx *applyContext) (uint64, int64, *raftcmdpb.RaftCMDResponse) {
	writeBytes := uint64(0)
	diffBytes := int64(0)
	resp := pb.AcquireRaftCMDResponse()

	d.resetAttrs()
	d.buf.Clear()
	d.requests = d.requests[:0]

	for idx, req := range ctx.req.Requests {
		if logger.DebugEnabled() {
			logger.Debugf("%s exec", hex.EncodeToString(req.ID))
		}
		resp.Responses = append(resp.Responses, nil)

		ctx.metrics.writtenKeys++
		if ctx.dataWB != nil {
			addedToWB, rsp, err := ctx.dataWB.Add(d.shard.ID, req, d.attrs)
			if err != nil {
				logger.Fatalf("shard %s add %+v to write batch failed with %+v",
					d.shard.ID,
					req,
					err)
			}

			if addedToWB {
				resp.Responses[idx] = rsp
				continue
			}
		}

		d.requests = append(d.requests, idx)
	}

	if len(d.requests) > 0 {
		d.attrs[attrRequestsTotal] = len(d.requests) - 1
		for idx, which := range d.requests {
			req := ctx.req.Requests[which]
			d.attrs[attrRequestsCurrent] = idx
			if h, ok := d.store.writeHandlers[req.CustemType]; ok {
				written, diff, rsp := h(d.shard, req, d.attrs)
				if rsp.Stale {
					rsp.Error.Message = errStaleCMD.Error()
					rsp.Error.StaleCommand = infoStaleCMD
					rsp.OriginRequest = req
					rsp.OriginRequest.Key = DecodeDataKey(req.Key)
				}

				resp.Responses[which] = rsp
				writeBytes += written
				diffBytes += diff
			}
		}
	}

	return writeBytes, diffBytes, resp
}
