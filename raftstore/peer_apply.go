package raftstore

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"time"

	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/beehive/metric"
	"github.com/deepfabric/beehive/pb"
	"github.com/deepfabric/beehive/pb/metapb"
	"github.com/deepfabric/beehive/pb/raftcmdpb"
	"github.com/deepfabric/beehive/pb/raftpb"
	"github.com/deepfabric/beehive/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/util/protoc"
)

func (pr *peerReplica) doRegistrationJob(delegate *applyDelegate) error {
	value, loaded := pr.store.delegates.LoadOrStore(delegate.shard.ID, delegate)
	if loaded {
		old := value.(*applyDelegate)
		if old.peerID != delegate.peerID {
			logger.Fatalf("shard %d delegate peer id not match, old=<%d> curr=<%d>",
				pr.shardID,
				old.peerID,
				delegate.peerID)
		}

		old.peerID = delegate.peerID
		old.shard = delegate.shard
		old.term = delegate.term
		old.applyState = delegate.applyState
		old.appliedIndexTerm = delegate.appliedIndexTerm
		old.clearAllCommandsAsStale()
	}

	return nil
}

func (s *store) doDestroy(shardID uint64, peer metapb.Peer) error {
	if value, ok := s.delegates.Load(shardID); ok {
		s.delegates.Delete(shardID)
		delegate := value.(*applyDelegate)
		delegate.destroy()
	}

	pr := s.getPR(shardID, false)
	if pr != nil {
		pr.mustDestroy()
	}

	return nil
}

func (pr *peerReplica) doCompactRaftLog(shardID, startIndex, endIndex uint64) error {
	firstIndex := startIndex

	if firstIndex == 0 {
		startKey := getRaftLogKey(shardID, 0)
		firstIndex = endIndex
		key, _, err := pr.store.MetadataStorage().Seek(startKey)
		if err != nil {
			return err
		}

		if len(key) > 0 {
			firstIndex, err = getRaftLogIndex(key)
			if err != nil {
				return err
			}
		}
	}

	if firstIndex >= endIndex {
		logger.Infof("shard %d no need to gc raft log",
			shardID)
		return nil
	}

	wb := util.NewWriteBatch()
	for index := firstIndex; index < endIndex; index++ {
		key := getRaftLogKey(shardID, index)
		err := wb.Delete(key)
		if err != nil {
			return err
		}
	}

	err := pr.store.MetadataStorage().Write(wb, false)
	if err == nil {
		logger.Debugf("shard %d raft log gc complete, entriesCount=<%d>",
			shardID,
			(endIndex - startIndex))
	}

	return err
}

func (pr *peerReplica) doApplyingSnapshotJob() error {
	logger.Infof("shard %d begin apply snapshot data", pr.shardID)
	localState, err := pr.ps.loadLocalState(pr.ps.applySnapJob)
	if err != nil {
		logger.Fatalf("shard %d apply snap load local state failed, errors:\n %+v",
			pr.shardID,
			err)
		return err
	}

	err = pr.ps.deleteAllInRange(encStartKey(&localState.Shard), encEndKey(&localState.Shard), pr.ps.applySnapJob)
	if err != nil {
		logger.Fatalf("shard %d apply snap delete range data failed, errors:\n %+v",
			pr.shardID,
			err)
		return err
	}

	err = pr.ps.applySnapshot(pr.ps.applySnapJob)
	if err != nil {
		logger.Errorf("shard %d apply snap snapshot failed, errors:\n %+v",
			pr.shardID,
			err)
		return err
	}

	err = pr.ps.updatePeerState(pr.ps.shard, raftpb.PeerNormal, nil)
	if err != nil {
		logger.Fatalf("shard %d apply snap update peer state failed, errors:\n %+v",
			pr.shardID,
			err)
		return err
	}

	pr.stopRaftTick = false
	logger.Infof("shard %d apply snapshot data complete, %+v",
		pr.shardID,
		pr.ps.raftHardState)
	return nil
}

func (pr *peerReplica) doApplyCommittedEntries(shardID uint64, term uint64, commitedEntries []etcdraftpb.Entry) error {
	logger.Debugf("shard %d async apply raft log with %d entries at term %d",
		shardID,
		len(commitedEntries),
		term)

	value, ok := pr.store.delegates.Load(shardID)
	if !ok {
		return fmt.Errorf("shard %d missing delegate", pr.shardID)
	}

	delegate := value.(*applyDelegate)
	delegate.term = term
	delegate.applyCommittedEntries(commitedEntries)

	if delegate.isPendingRemove() {
		delegate.destroy()
		pr.store.delegates.Delete(delegate.shard.ID)
	}

	return nil
}

type asyncApplyResult struct {
	shardID          uint64
	appliedIndexTerm uint64
	applyState       raftpb.RaftApplyState
	result           *execResult
	metrics          applyMetrics
}

func (res *asyncApplyResult) reset() {
	res.shardID = 0
	res.appliedIndexTerm = 0
	res.applyState = raftpb.RaftApplyState{}
	res.result = nil
	res.metrics = applyMetrics{}
}

func (res *asyncApplyResult) hasSplitExecResult() bool {
	return nil != res.result && res.result.splitResult != nil
}

type execResult struct {
	adminType    raftcmdpb.AdminCmdType
	changePeer   *changePeer
	splitResult  *splitResult
	raftGCResult *raftGCResult
}

type changePeer struct {
	confChange etcdraftpb.ConfChange
	peer       metapb.Peer
	shard      metapb.Shard
}

type splitResult struct {
	left  metapb.Shard
	right metapb.Shard
}

type raftGCResult struct {
	state      raftpb.RaftTruncatedState
	firstIndex uint64
}

type applyContext struct {
	raftStateWB *util.WriteBatch
	dataWB      CommandWriteBatch

	applyState raftpb.RaftApplyState
	req        *raftcmdpb.RaftCMDRequest
	index      uint64
	term       uint64
	metrics    applyMetrics
}

func newApplyContext(store *store) *applyContext {
	var dataWB CommandWriteBatch
	if store.opts.writeBatchFunc != nil {
		dataWB = store.opts.writeBatchFunc()
	}

	return &applyContext{
		raftStateWB: util.NewWriteBatch(),
		dataWB:      dataWB,
	}
}

func (ctx *applyContext) reset() {
	ctx.raftStateWB.Reset()
	ctx.applyState = raftpb.RaftApplyState{}
	ctx.req = nil
	ctx.index = 0
	ctx.term = 0
	ctx.metrics = applyMetrics{}

	if ctx.dataWB != nil {
		ctx.dataWB.Reset()
	}
}

type applyDelegate struct {
	store  *store
	ps     *peerStorage
	peerID uint64
	shard  metapb.Shard
	// if we remove ourself in ChangePeer remove, we should set this flag, then
	// any following committed logs in same Ready should be applied failed.
	pendingRemove        bool
	applyState           raftpb.RaftApplyState
	appliedIndexTerm     uint64
	term                 uint64
	pendingCMDs          []cmd
	pendingChangePeerCMD cmd

	// reuse
	wb  *util.WriteBatch
	ctx *applyContext

	// attrs
	buf      *goetty.ByteBuf
	attrs    map[string]interface{}
	requests []int
}

func (d *applyDelegate) clearAllCommandsAsStale() {
	for _, c := range d.pendingCMDs {
		d.notifyStaleCMD(c)
	}

	if d.pendingChangePeerCMD.req != nil {
		d.notifyStaleCMD(d.pendingChangePeerCMD)
	}

	d.pendingCMDs = d.pendingCMDs[:0]
	d.pendingChangePeerCMD = emptyCMD
}

func (d *applyDelegate) findCB(ctx *applyContext) (cmd, bool) {
	if isChangePeerCMD(ctx.req) {
		c := d.pendingChangePeerCMD
		if c.req == nil {
			return emptyCMD, false
		} else if bytes.Compare(ctx.req.Header.ID, c.getUUID()) == 0 {
			return c, true
		}

		d.notifyStaleCMD(c)
		return emptyCMD, false
	}

	for {
		head, ok := d.popPendingCMD(ctx.term)
		if !ok || head.req == nil {
			return emptyCMD, false
		}

		if bytes.Compare(head.getUUID(), ctx.req.Header.ID) == 0 {
			return head, true
		}

		// Because of the lack of original RaftCmdRequest, we skip calling
		// coprocessor here.
		d.notifyStaleCMD(head)
	}
}

func (d *applyDelegate) appendPendingCmd(c cmd) {
	d.pendingCMDs = append(d.pendingCMDs, c)
}

func (d *applyDelegate) popPendingCMD(raftLogEntryTerm uint64) (cmd, bool) {
	if len(d.pendingCMDs) == 0 {
		return emptyCMD, false
	}

	if d.pendingCMDs[0].term > raftLogEntryTerm {
		return emptyCMD, false
	}

	c := d.pendingCMDs[0]
	d.pendingCMDs[0] = emptyCMD
	d.pendingCMDs = d.pendingCMDs[1:]
	return c, true
}

func (d *applyDelegate) notifyStaleCMD(c cmd) {
	c.resp(errorStaleCMDResp(c.getUUID(), d.term))
}

func (d *applyDelegate) notifyShardRemoved(c cmd) {
	logger.Infof("shard %d cmd is removed, skip. cmd=<%+v>",
		d.shard.ID,
		c)
	c.respShardNotFound(d.shard.ID)
}

func (d *applyDelegate) applyCommittedEntries(commitedEntries []etcdraftpb.Entry) {
	if len(commitedEntries) <= 0 {
		return
	}

	start := time.Now()
	req := pb.AcquireRaftCMDRequest()

	if d.ctx.dataWB != nil {
		d.ctx.dataWB.Reset()
	}

	for idx, entry := range commitedEntries {
		if d.isPendingRemove() {
			// This peer is about to be destroyed, skip everything.
			break
		}
		expectIndex := d.applyState.AppliedIndex + 1
		if expectIndex != entry.Index {
			logger.Fatalf("shard %d index not match, expect=<%d> get=<%d> state=<%+v> entry=<%+v>",
				d.shard.ID,
				expectIndex,
				entry.Index,
				d.applyState,
				entry)
		}

		if idx > 0 {
			d.ctx.reset()
			req.Reset()
		}

		d.ctx.req = req
		d.ctx.applyState = d.applyState
		d.ctx.index = entry.Index
		d.ctx.term = entry.Term

		var result *execResult

		switch entry.Type {
		case etcdraftpb.EntryNormal:
			result = d.applyEntry(&entry)
		case etcdraftpb.EntryConfChange:
			result = d.applyConfChange(&entry)
		}

		asyncResult := asyncApplyResult{}
		asyncResult.shardID = d.shard.ID
		asyncResult.appliedIndexTerm = d.appliedIndexTerm
		asyncResult.applyState = d.applyState
		asyncResult.result = result

		if d.ctx != nil {
			asyncResult.metrics = d.ctx.metrics
		}

		pr := d.store.getPR(d.shard.ID, false)
		if pr != nil {
			pr.addApplyResult(asyncResult)
		}
	}

	// only release RaftCMDRequest. Header and Requests fields is pb created in Unmarshal
	pb.ReleaseRaftCMDRequest(req)

	metric.ObserveRaftLogApplyDuration(start)
}

func (d *applyDelegate) applyEntry(entry *etcdraftpb.Entry) *execResult {
	if len(entry.Data) > 0 {
		protoc.MustUnmarshal(d.ctx.req, entry.Data)
		return d.doApplyRaftCMD()
	}

	// when a peer become leader, it will send an empty entry.
	state := d.applyState
	state.AppliedIndex = entry.Index

	err := d.store.MetadataStorage().Set(getApplyStateKey(d.shard.ID), protoc.MustMarshal(&state))
	if err != nil {
		logger.Fatalf("shard %d apply empty entry failed, entry=<%s> errors:\n %+v",
			d.shard.ID,
			entry.String(),
			err)
	}

	d.applyState.AppliedIndex = entry.Index
	d.appliedIndexTerm = entry.Term
	if entry.Term <= 0 {
		panic("error empty entry term.")
	}

	for {
		c, ok := d.popPendingCMD(entry.Term - 1)
		if !ok {
			return nil
		}

		// apprently, all the callbacks whose term is less than entry's term are stale.
		d.notifyStaleCMD(c)
	}
}

func (d *applyDelegate) applyConfChange(entry *etcdraftpb.Entry) *execResult {
	cc := new(etcdraftpb.ConfChange)

	protoc.MustUnmarshal(cc, entry.Data)
	protoc.MustUnmarshal(d.ctx.req, cc.Context)

	result := d.doApplyRaftCMD()
	if nil == result {
		return &execResult{
			adminType:  raftcmdpb.ChangePeer,
			changePeer: &changePeer{},
		}
	}

	result.changePeer.confChange = *cc
	return result
}

func (d *applyDelegate) doApplyRaftCMD() *execResult {
	if d.ctx.index == 0 {
		logger.Fatalf("shard %d apply raft command needs a none zero index",
			d.shard.ID)
	}

	c, ok := d.findCB(d.ctx)
	if d.isPendingRemove() {
		logger.Fatalf("shard %d apply raft comand can not pending remove",
			d.shard.ID)
	}

	var err error
	var resp *raftcmdpb.RaftCMDResponse
	var result *execResult
	var writeBytes uint64
	var diffBytes int64

	if !d.checkEpoch(d.ctx.req) {
		resp = errorStaleEpochResp(d.ctx.req.Header.ID, d.term, d.shard)
	} else {
		if d.ctx.req.AdminRequest != nil {
			resp, result, err = d.execAdminRequest(d.ctx)
			if err != nil {
				resp = errorStaleEpochResp(d.ctx.req.Header.ID, d.term, d.shard)
			}
		} else {
			writeBytes, diffBytes, resp = d.execWriteRequest(d.ctx)
		}
	}

	if d.ctx.dataWB != nil {
		writeBytes, diffBytes, err = d.ctx.dataWB.Execute()
		if err != nil {
			logger.Fatalf("shard %d execute batch failed with %+v",
				d.shard.ID,
				err)
		}
	}

	if logger.DebugEnabled() {
		for _, req := range d.ctx.req.Requests {
			logger.Debugf("%s exec completed", hex.EncodeToString(req.ID))
		}
	}

	d.ctx.metrics.writtenBytes += writeBytes
	if diffBytes < 0 {
		v := uint64(math.Abs(float64(diffBytes)))
		if v >= d.ctx.metrics.sizeDiffHint {
			d.ctx.metrics.sizeDiffHint = 0
		} else {
			d.ctx.metrics.sizeDiffHint -= v
		}
	} else {
		d.ctx.metrics.sizeDiffHint += uint64(diffBytes)
	}

	d.ctx.applyState.AppliedIndex = d.ctx.index
	if !d.isPendingRemove() {
		err := d.ctx.raftStateWB.Set(getApplyStateKey(d.shard.ID), protoc.MustMarshal(&d.ctx.applyState))
		if err != nil {
			logger.Fatalf("shard %d save apply context failed, errors:\n %+v",
				d.shard.ID,
				err)
		}
	}

	err = d.store.MetadataStorage().Write(d.ctx.raftStateWB, false)
	if err != nil {
		logger.Fatalf("shard %d commit apply result failed, errors:\n %+v",
			d.shard.ID,
			err)
	}

	d.applyState = d.ctx.applyState
	d.term = d.ctx.term

	if ok {
		if resp != nil {
			buildTerm(d.term, resp)
			buildUUID(d.ctx.req.Header.ID, resp)
			// resp client
			c.resp(resp)
		}
	}

	return result
}

func (d *applyDelegate) destroy() {
	for _, c := range d.pendingCMDs {
		d.notifyShardRemoved(c)
	}

	if d.pendingChangePeerCMD.req != nil {
		d.notifyShardRemoved(d.pendingChangePeerCMD)
	}

	d.pendingCMDs = nil
	d.pendingChangePeerCMD = emptyCMD
}

func (d *applyDelegate) setPendingRemove() {
	d.pendingRemove = true
}

func (d *applyDelegate) isPendingRemove() bool {
	return d.pendingRemove
}

func (d *applyDelegate) checkEpoch(req *raftcmdpb.RaftCMDRequest) bool {
	return checkEpoch(d.shard, req)
}

func isChangePeerCMD(req *raftcmdpb.RaftCMDRequest) bool {
	return nil != req.AdminRequest &&
		req.AdminRequest.CmdType == raftcmdpb.ChangePeer
}
