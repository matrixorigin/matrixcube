package raftstore

import (
	"bytes"
	"errors"

	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/pb/raftpb"
)

func (d *applyDelegate) execAdminRequest(ctx *applyContext) (*raftcmdpb.RaftCMDResponse, *execResult, error) {
	cmdType := ctx.req.AdminRequest.CmdType
	switch cmdType {
	case raftcmdpb.ChangePeer:
		return d.doExecChangePeer(ctx)
	case raftcmdpb.Split:
		return d.doExecSplit(ctx)
	case raftcmdpb.CompactRaftLog:
		return d.doExecCompactRaftLog(ctx)
	}

	return nil, nil, nil
}

func (d *applyDelegate) doExecChangePeer(ctx *applyContext) (*raftcmdpb.RaftCMDResponse, *execResult, error) {
	req := ctx.req.AdminRequest.ChangePeer
	logger.Infof("shard %d exec change conf, type=<%s> epoch=<%+v>",
		d.shard.ID,
		req.ChangeType.String(),
		d.shard.Epoch)

	exists := findPeer(&d.shard, req.Peer.StoreID)
	d.shard.Epoch.ConfVer++

	switch req.ChangeType {
	case raftcmdpb.AddNode:
		if exists != nil {
			ctx.metrics.admin.confChangeReject++
			return nil, nil, nil
		}

		d.shard.Peers = append(d.shard.Peers, req.Peer)
		ctx.metrics.admin.addPeerSucceed++
		logger.Infof("shard %d peer added, peer=<%+v>",
			d.shard.ID,
			req.Peer)
	case raftcmdpb.RemoveNode:
		if exists == nil {
			ctx.metrics.admin.confChangeReject++
			return nil, nil, nil
		}

		// Remove ourself, we will destroy all shard data later.
		// So we need not to apply following logs.
		if d.peerID == req.Peer.ID {
			d.setPendingRemove()
		}

		removePeer(&d.shard, req.Peer.StoreID)

		ctx.metrics.admin.removePeerSucceed++
		logger.Infof("shard %d peer removed, peer=<%+v>",
			d.shard.ID,
			req.Peer)
	}

	state := raftpb.PeerNormal
	if d.isPendingRemove() {
		state = raftpb.PeerTombstone
	}

	err := d.ps.updatePeerState(d.shard, state, ctx.raftStateWB)
	if err != nil {
		logger.Fatalf("shard %d update db state failed, errors:\n %+v",
			d.shard.ID,
			err)
	}

	resp := newAdminRaftCMDResponse(raftcmdpb.ChangePeer, &raftcmdpb.ChangePeerResponse{
		Shard: d.shard,
	})

	result := &execResult{
		adminType: raftcmdpb.ChangePeer,
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
	req := ctx.req.AdminRequest.Split

	if len(req.SplitKey) == 0 {
		logger.Errorf("shard %d missing split key",
			d.shard.ID)
		return nil, nil, errors.New("missing split key")
	}

	req.SplitKey = getOriginKey(req.SplitKey)

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
	newShard := metapb.Shard{
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
			StoreID: d.shard.Peers[idx].StoreID,
		})
	}

	d.shard.Epoch.ShardVer++
	newShard.Epoch.ShardVer = d.shard.Epoch.ShardVer
	err := d.ps.updatePeerState(d.shard, raftpb.PeerNormal, ctx.raftStateWB)

	d.wb.Reset()
	if err == nil {
		err = d.ps.updatePeerState(newShard, raftpb.PeerNormal, d.wb)
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

	err = d.ps.store.MetadataStorage(newShard.ID).Write(d.wb, false)
	if err != nil {
		logger.Fatalf("shard %d commit apply result failed, errors:\n %+v",
			d.shard.ID,
			err)
	}

	rsp := newAdminRaftCMDResponse(raftcmdpb.Split, &raftcmdpb.SplitResponse{
		Left:  d.shard,
		Right: newShard,
	})

	result := &execResult{
		adminType: raftcmdpb.Split,
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

	req := ctx.req.AdminRequest.Compact
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

	rsp := newAdminRaftCMDResponse(raftcmdpb.CompactRaftLog, &raftcmdpb.CompactRaftLogResponse{})
	result := &execResult{
		adminType: raftcmdpb.CompactRaftLog,
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
	d.buf.Clear()
	for _, req := range ctx.req.Requests {
		if logger.DebugEnabled() {
			logger.Debugf("exec %s", formatRequest(req))
		}

		ctx.metrics.writtenKeys++
		if ctx.dataWB != nil {
			addedToWB, rsp, err := ctx.dataWB.Add(d.shard.ID, req, d.buf)
			if err != nil {
				logger.Fatalf("shard %s add %+v to write batch failed with %+v",
					d.shard.ID,
					req,
					err)
			}

			if addedToWB {
				resp.Responses = append(resp.Responses, rsp)
				continue
			}
		}

		if h, ok := d.store.writeHandlers[req.CustemType]; ok {
			written, diff, rsp := h(d.shard, req, d.buf)
			resp.Responses = append(resp.Responses, rsp)
			writeBytes += written
			diffBytes += diff
		}
	}

	return writeBytes, diffBytes, resp
}
