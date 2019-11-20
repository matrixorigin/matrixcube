package raftstore

import (
	"bytes"
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

		old.term = delegate.term
		old.clearAllCommandsAsStale()
	}

	return nil
}

func (s *store) doDestroy(shardID uint64, peer metapb.Peer) error {
	if value, ok := s.delegates.Load(shardID); ok {
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
		key, _, err := pr.store.MetadataStorage(shardID).Seek(startKey)
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

	wb := pr.store.MetadataStorage(shardID).NewWriteBatch()
	for index := firstIndex; index < endIndex; index++ {
		key := getRaftLogKey(shardID, index)
		err := wb.Delete(key)
		if err != nil {
			return err
		}
	}

	err := pr.store.MetadataStorage(shardID).Write(wb, false)
	if err != nil {
		logger.Infof("shard %d raft log gc complete, entriesCount=<%d>",
			shardID,
			(endIndex - startIndex))
	}

	return err
}

func (pr *peerReplica) doApplyingSnapshotJob() error {
	logger.Infof("shard %d begin apply snap data", pr.shardID)
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
	logger.Infof("shard %d apply snapshot complete", pr.shardID)
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
	// raft state write batch
	wb         util.WriteBatch
	writeBatch CommandWriteBatch

	applyState raftpb.RaftApplyState
	req        *raftcmdpb.RaftCMDRequest
	index      uint64
	term       uint64
	metrics    applyMetrics
}

func newApplyContext() *applyContext {
	return &applyContext{}
}

func (ctx *applyContext) reset() {
	ctx.wb = nil
	ctx.applyState = raftpb.RaftApplyState{}
	ctx.req = nil
	ctx.index = 0
	ctx.term = 0
	ctx.metrics = applyMetrics{}

	if ctx.writeBatch != nil {
		ctx.writeBatch.Reset()
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
	pendingCMDs          []*cmd
	pendingChangePeerCMD *cmd
}

func (d *applyDelegate) clearAllCommandsAsStale() {
	for _, c := range d.pendingCMDs {
		d.notifyStaleCMD(c)
	}

	if nil != d.pendingChangePeerCMD {
		d.notifyStaleCMD(d.pendingChangePeerCMD)
	}

	d.pendingCMDs = d.pendingCMDs[:0]
	d.pendingChangePeerCMD = nil
}

func (d *applyDelegate) findCB(ctx *applyContext) *cmd {
	if isChangePeerCMD(ctx.req) {
		c := d.getPendingChangePeerCMD()
		if c == nil || c.req == nil {
			return nil
		} else if bytes.Compare(ctx.req.Header.ID, c.getUUID()) == 0 {
			return c
		}

		d.notifyStaleCMD(c)
		return nil
	}

	for {
		head := d.popPendingCMD(ctx.term)
		if head == nil {
			return nil
		}

		if bytes.Compare(head.getUUID(), ctx.req.Header.ID) == 0 {
			return head
		}

		// Because of the lack of original RaftCmdRequest, we skip calling
		// coprocessor here.
		d.notifyStaleCMD(head)
	}
}

func (d *applyDelegate) appendPendingCmd(c *cmd) {
	d.pendingCMDs = append(d.pendingCMDs, c)
}

func (d *applyDelegate) setPendingChangePeerCMD(c *cmd) {
	d.pendingChangePeerCMD = c
}

func (d *applyDelegate) getPendingChangePeerCMD() *cmd {
	return d.pendingChangePeerCMD
}

func (d *applyDelegate) popPendingCMD(raftLogEntryTerm uint64) *cmd {
	if len(d.pendingCMDs) == 0 {
		return nil
	}

	if d.pendingCMDs[0].term > raftLogEntryTerm {
		return nil
	}

	c := d.pendingCMDs[0]
	d.pendingCMDs[0] = nil
	d.pendingCMDs = d.pendingCMDs[1:]
	return c
}

func (d *applyDelegate) notifyStaleCMD(c *cmd) {
	c.resp(errorStaleCMDResp(c.getUUID(), d.term))
}

func (d *applyDelegate) notifyShardRemoved(c *cmd) {
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
	ctx := acquireApplyContext()
	req := pb.AcquireRaftCMDRequest()

	if d.store.opts.writeBatchFunc != nil {
		ctx.writeBatch = d.store.opts.writeBatchFunc()
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
			ctx.reset()
			req.Reset()
		}

		ctx.req = req
		ctx.applyState = d.applyState
		ctx.index = entry.Index
		ctx.term = entry.Term

		var result *execResult

		switch entry.Type {
		case etcdraftpb.EntryNormal:
			result = d.applyEntry(ctx, &entry)
		case etcdraftpb.EntryConfChange:
			result = d.applyConfChange(ctx, &entry)
		}

		asyncResult := acquireAsyncApplyResult()

		asyncResult.shardID = d.shard.ID
		asyncResult.appliedIndexTerm = d.appliedIndexTerm
		asyncResult.applyState = d.applyState
		asyncResult.result = result

		if ctx != nil {
			asyncResult.metrics = ctx.metrics
		}

		pr := d.store.getPR(d.shard.ID, false)
		if pr != nil {
			pr.addApplyResult(asyncResult)
		}
	}

	// only release RaftCMDRequest. Header and Requests fields is pb created in Unmarshal
	pb.ReleaseRaftCMDRequest(req)
	releaseApplyContext(ctx)

	metric.ObserveRaftLogApplyDuration(start)
}

func (d *applyDelegate) applyEntry(ctx *applyContext, entry *etcdraftpb.Entry) *execResult {
	if len(entry.Data) > 0 {
		protoc.MustUnmarshal(ctx.req, entry.Data)
		return d.doApplyRaftCMD(ctx)
	}

	// when a peer become leader, it will send an empty entry.
	state := d.applyState
	state.AppliedIndex = entry.Index

	err := d.store.MetadataStorage(d.shard.ID).Set(getApplyStateKey(d.shard.ID), protoc.MustMarshal(&state))
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
		c := d.popPendingCMD(entry.Term - 1)
		if c == nil {
			return nil
		}

		// apprently, all the callbacks whose term is less than entry's term are stale.
		d.notifyStaleCMD(c)
	}
}

func (d *applyDelegate) applyConfChange(ctx *applyContext, entry *etcdraftpb.Entry) *execResult {
	cc := new(etcdraftpb.ConfChange)

	protoc.MustUnmarshal(cc, entry.Data)
	protoc.MustUnmarshal(ctx.req, cc.Context)

	result := d.doApplyRaftCMD(ctx)
	if nil == result {
		return &execResult{
			adminType:  raftcmdpb.ChangePeer,
			changePeer: &changePeer{},
		}
	}

	result.changePeer.confChange = *cc
	return result
}

func (d *applyDelegate) doApplyRaftCMD(ctx *applyContext) *execResult {
	if ctx.index == 0 {
		logger.Fatalf("shard %d apply raft command needs a none zero index",
			d.shard.ID)
	}

	c := d.findCB(ctx)
	if d.isPendingRemove() {
		logger.Fatalf("shard %d apply raft comand can not pending remove",
			d.shard.ID)
	}

	var err error
	var resp *raftcmdpb.RaftCMDResponse
	var result *execResult
	var writeBytes uint64
	var diffBytes int64

	driver := d.store.MetadataStorage(d.shard.ID)
	ctx.wb = driver.NewWriteBatch()

	if !d.checkEpoch(ctx.req) {
		resp = errorStaleEpochResp(ctx.req.Header.ID, d.term, d.shard)
	} else {
		if ctx.req.AdminRequest != nil {
			resp, result, err = d.execAdminRequest(ctx)
			if err != nil {
				resp = errorStaleEpochResp(ctx.req.Header.ID, d.term, d.shard)
			}
		} else {
			writeBytes, diffBytes, resp = d.execWriteRequest(ctx)
		}
	}

	if ctx.writeBatch != nil {
		writeBytes, diffBytes, err = ctx.writeBatch.Execute()
		if err != nil {
			logger.Fatalf("shard %d execute batch failed with %+v",
				d.shard.ID,
				err)
		}
	}

	ctx.metrics.writtenBytes += writeBytes
	if diffBytes < 0 {
		v := uint64(math.Abs(float64(diffBytes)))
		if v >= ctx.metrics.sizeDiffHint {
			ctx.metrics.sizeDiffHint = 0
		} else {
			ctx.metrics.sizeDiffHint -= v
		}
	} else {
		ctx.metrics.sizeDiffHint += uint64(diffBytes)
	}

	ctx.applyState.AppliedIndex = ctx.index
	if !d.isPendingRemove() {
		err := ctx.wb.Set(getApplyStateKey(d.shard.ID), protoc.MustMarshal(&ctx.applyState))
		if err != nil {
			logger.Fatalf("shard %d save apply context failed, errors:\n %+v",
				d.shard.ID,
				err)
		}
	}

	err = driver.Write(ctx.wb, false)
	if err != nil {
		logger.Fatalf("shard %d commit apply result failed, errors:\n %+v",
			d.shard.ID,
			err)
	}

	d.applyState = ctx.applyState
	d.term = ctx.term

	if c != nil {
		if resp != nil {
			buildTerm(d.term, resp)
			buildUUID(ctx.req.Header.ID, resp)
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

	if d.pendingChangePeerCMD != nil && d.pendingChangePeerCMD.req != nil {
		d.notifyShardRemoved(d.pendingChangePeerCMD)
	}

	d.pendingCMDs = nil
	d.pendingChangePeerCMD = nil
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
