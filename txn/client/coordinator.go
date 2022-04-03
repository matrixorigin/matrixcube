// Copyright 2022 MatrixOrigin.
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

package client

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/fagongzi/util/hack"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/txn/util"
	"github.com/matrixorigin/matrixcube/util/hlc"
	"github.com/matrixorigin/matrixcube/util/keys"
	"github.com/matrixorigin/matrixcube/util/stop"
	"go.uber.org/zap"
)

// txnCoordinator txn coordinator
type txnCoordinator interface {
	txnMetaGetter
	send(ctx context.Context, batchRequest txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error)
	stop()
}

func newTxnCoordinator(txnMeta txnpb.TxnMeta,
	sender BatchDispatcher,
	txnClocker hlc.Clock,
	logger *zap.Logger,
	opts txnOptions) *coordinator {
	c := &coordinator{
		sender:     sender,
		logger:     logger.With(log.HexField("txn-id", txnMeta.ID)),
		txnClocker: txnClocker,
		opts:       opts,
		stopper:    stop.NewStopper("txn-"+txnMeta.Name, stop.WithLogger(logger)),
	}
	c.mu.txnMeta = txnMeta
	c.mu.status = txnpb.TxnStatus_Pending
	c.mu.infightWrites = make(map[uint64]*keys.KeyTree)
	c.mu.completedWrites = make(map[uint64]*txnpb.KeySet)
	return c
}

type coordinator struct {
	logger     *zap.Logger
	sender     BatchDispatcher
	txnClocker hlc.Clock
	opts       txnOptions
	stopper    *stop.Stopper

	mu struct {
		sync.Mutex

		// commtting or rollingback, all txn operation are disabled.
		ending       bool
		heartbeating bool

		status  txnpb.TxnStatus
		txnMeta txnpb.TxnMeta
		// infightWrites record the current transaction has not completed the consensus write
		// operation.
		infightWrites map[uint64]*keys.KeyTree
		// completedWrites record the current transaction has completed the consensus write
		// operation.
		completedWrites map[uint64]*txnpb.KeySet
		// sequence indicates the current number of write operations in the transaction.
		// Of all the operations in the transaction, only the write operation will increase
		// the value of the field.
		sequence uint32
	}
}

func (c *coordinator) send(ctx context.Context, batchRequest txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
	n := len(batchRequest.Requests)
	if n == 0 {
		c.logger.Fatal("empty batch request")
	}

	hasCommitOrRollback := batchRequest.HasCommitOrRollback()
	createTxnRecord, err := c.prepareSend(ctx, &batchRequest)
	if err != nil {
		return txnpb.TxnBatchResponse{}, err
	}

	// if with async consensus optimization on, we can only move to completedWrites after explicitly
	// querying for written temporary data
	defer c.moveToCompletedWrites(batchRequest)

	// create TxnRecord request need to be completed first, commit requests need to be completed last
	// (no parallel commit optimization), and they cannot be executed together with other write requests
	// in a Batch.
	sendDirect := n == 1 || // only one request
		batchRequest.Header.Type == txnpb.TxnRequestType_Read || // read requests
		(!createTxnRecord && !hasCommitOrRollback) // txn record created

	if sendDirect { // txn record created
		resp, err := c.sender.Send(ctx, batchRequest)
		return c.handleResponse(resp, err, createTxnRecord, createTxnRecord && !hasCommitOrRollback)
	}

	var resp txnpb.TxnBatchResponse
	requests := c.splitBatchRequests(&batchRequest, createTxnRecord, hasCommitOrRollback)
	for idx, req := range requests {
		r, err := c.sender.Send(ctx, req)
		if err != nil {
			return resp, err
		}

		// when we need to create a TxnRecord and the transaction has not yet finished, we need to ensure
		// that subsequent transaction operations must be executed after the TxnRecord is created, so we
		// execute this Batch synchronously under lock protection.
		r, err = c.handleResponse(r, err,
			createTxnRecord && idx == 0, // first write hold lock
			createTxnRecord && idx == 0 && !hasCommitOrRollback)
		if err != nil {
			return resp, err
		}

		resp.Responses = append(resp.Responses, r.Responses...)
	}

	return resp, nil
}

func (c *coordinator) prepareSend(ctx context.Context, batchRequest *txnpb.TxnBatchRequest) (createTxnRecord bool, err error) {
	c.mu.Lock()
	defer func() {
		if !createTxnRecord {
			c.mu.Unlock()
		}
	}()

	if err := c.canSendLocked(); err != nil {
		return createTxnRecord, err
	}

	if batchRequest.IsWrite() {
		c.mu.sequence++
		c.addInfightWritesLocked(batchRequest)

		if c.mu.sequence == 1 {
			if !batchRequest.Requests[0].Operation.Impacted.HasPointKeys() {
				c.logger.Fatal("first write in txn must has point key to save txn record.")
			}

			// For the first write operation, need to explicitly tell the TxnManager to create
			// the TxnRecord synchronously. We use the first Key of the first write request as
			// the TxnRecordRouteKey.
			c.mu.txnMeta.TxnRecordRouteKey = batchRequest.Requests[0].Operation.Impacted.PointKeys[0]
			c.mu.txnMeta.TxnRecordShardGroup = batchRequest.Requests[0].Operation.ShardGroup
			batchRequest.Requests[0].Options.CreateTxnRecord = true
			createTxnRecord = true
		}
	}

	// make sure all read or commit/rollback request should after infight key completed
	c.maybeInsertWaitConsensusLocked(batchRequest)

	batchRequest.Header.Txn.TxnMeta = c.mu.txnMeta
	batchRequest.Header.Txn.Sequence = c.mu.sequence
	// If the current BatchRequest contains requests for commit or rollback transactions, we need
	// to append the Key of all the operations performed and let TxnManager resolve the temporary
	// data.
	if batchRequest.HasCommitOrRollback() {
		batchRequest.Header.Txn.InfightWrites = c.inflightKeySetLocked()
		batchRequest.Header.Txn.CompletedWrites = c.completedKeySetLocked()
		c.mu.ending = true
	}

	return createTxnRecord, nil
}

func (c *coordinator) maybeInsertWaitConsensusLocked(batchRequest *txnpb.TxnBatchRequest) {
	n := c.infightKeyCountLocked()
	if !c.supportAsyncConsensus() ||
		n == 0 {
		return
	}

	// we need see has overlap with infight writes
	oldRequests := batchRequest.Requests
	// every infight key added only once
	var addedKeys map[string]struct{}
	changed := false
	for idx := range oldRequests {
		// all infight keys added
		if len(addedKeys) == n {
			if changed {
				batchRequest.AddManyRequest(oldRequests[idx:])
			}
			break
		}

		fn := func(key []byte) bool {
			if !changed {
				batchRequest.Requests = append([]txnpb.TxnRequest(nil), oldRequests[:idx]...)
				changed = true
			}
			if addedKeys == nil {
				addedKeys = make(map[string]struct{})
			}
			strKey := hack.SliceToString(key)
			if _, ok := addedKeys[strKey]; !ok {
				batchRequest.AddRequest(txnpb.TxnRequest{
					Operation: txnpb.TxnOperation{
						Op:         uint32(txnpb.InternalTxnOp_WaitConsensus),
						Impacted:   txnpb.KeySet{PointKeys: [][]byte{key}},
						ShardGroup: oldRequests[idx].Operation.ShardGroup,
					},
				})
				addedKeys[strKey] = struct{}{}
			}
			return true
		}

		if oldRequests[idx].IsCommitOrRollback() {
			// add all infight keys
			for _, tree := range c.mu.infightWrites {
				tree.Ascend(fn)
			}
		} else if oldRequests[idx].HasReadImpacted() { // normal read or readwrite operation
			if oldRequests[idx].Operation.Impacted.IsEmpty() {
				c.logger.Fatal("read operation has no impacted keys")
			}
			// add intersection of infight key
			from, to := oldRequests[idx].Operation.Impacted.GetKeyRange()
			c.mu.infightWrites[oldRequests[idx].Operation.ShardGroup].AscendRange(from, to, fn)
		}

		if changed {
			batchRequest.AddRequest(oldRequests[idx])
		}
	}
}

func (c *coordinator) canSendLocked() error {
	switch c.mu.status {
	case txnpb.TxnStatus_Aborted:
		return ErrTxnAborted
	case txnpb.TxnStatus_Committed:
		return ErrTxnCommitted
	}

	if c.mu.ending {
		return ErrTxnEnding
	}

	return nil
}

func (c *coordinator) addInfightWritesLocked(batchRequest *txnpb.TxnBatchRequest) {
	for idx := range batchRequest.Requests {
		asyncConsensus := false
		if !batchRequest.Requests[idx].IsInternal() {
			if batchRequest.Requests[idx].Operation.Impacted.IsEmpty() {
				c.logger.Fatal("write operation has no impacted keys")
			}

			if c.supportAsyncConsensus() {
				if batchRequest.Requests[idx].Operation.Impacted.HasPointKeys() {
					asyncConsensus = true
				}
				// range update can not perform async consensus, because we can not clearly
				// know which keys are in the consensus
				if batchRequest.Requests[idx].Operation.Impacted.HasKeyRanges() {
					asyncConsensus = false
				}
			}

			batchRequest.Requests[idx].Options.AsynchronousConsensus = asyncConsensus
			if asyncConsensus {
				c.addToInfightWritesLocked(batchRequest.Requests[idx].Operation)
			} else {
				c.addToCompletedWritesLocked(batchRequest.Requests[idx].Operation)
			}
		}
	}
}

func (c *coordinator) infightBytesLocked() int {
	n := 0
	for _, tree := range c.mu.infightWrites {
		n += tree.Bytes()
	}
	return n
}

func (c *coordinator) maxInfightBytesExceedLocked() bool {
	return c.opts.optimize.maxInfilghtKeysBytes <= c.infightBytesLocked()
}

func (c *coordinator) addToInfightWritesLocked(op txnpb.TxnOperation) {
	if tree, ok := c.mu.infightWrites[op.ShardGroup]; ok {
		tree.AddMany(op.Impacted.PointKeys)
		return
	}

	tree := keys.NewKeyTree(32)
	tree.AddMany(op.Impacted.PointKeys)
	c.mu.infightWrites[op.ShardGroup] = tree
}

func (c *coordinator) infightKeyCountLocked() int {
	n := 0
	for _, tree := range c.mu.infightWrites {
		n += tree.Len()
	}
	return n
}

func (c *coordinator) addToCompletedWritesLocked(op txnpb.TxnOperation) {
	if keySet, ok := c.mu.completedWrites[op.ShardGroup]; ok {
		keySet.AddPointKeys(op.Impacted.PointKeys)
		keySet.AddKeyRanges(op.Impacted.Ranges)
		return
	}

	keySet := &txnpb.KeySet{}
	keySet.AddPointKeys(op.Impacted.PointKeys)
	keySet.AddKeyRanges(op.Impacted.Ranges)
	c.mu.completedWrites[op.ShardGroup] = keySet
}

func (c *coordinator) moveToCompletedWrites(batchRequest txnpb.TxnBatchRequest) {
	if !batchRequest.HasWaitConsensus() {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for idx := range batchRequest.Requests {
		if batchRequest.Requests[idx].IsWaitConsensus() {
			g := batchRequest.Requests[idx].Operation.ShardGroup
			c.mu.infightWrites[g].DeleteMany(batchRequest.Requests[idx].Operation.Impacted.PointKeys)
			c.mu.completedWrites[g].AddPointKeys(batchRequest.Requests[idx].Operation.Impacted.PointKeys)
		}
	}
}

func (c *coordinator) stop() {
	c.mu.Lock()
	c.mu.ending = true
	c.mu.Unlock()

	c.stopper.Stop()
}

func (c *coordinator) handleResponse(resp txnpb.TxnBatchResponse, err error, locked bool, startHeartbeating bool) (txnpb.TxnBatchResponse, error) {
	if locked {
		defer c.mu.Unlock()
	}

	if err != nil {
		return txnpb.TxnBatchResponse{}, err
	}

	// TODO: Currently, we do not do any optimization, encounter any error, are Abort transaction.
	// Some special errors need to be handled:
	// 1. W/W conflict. T.ReadTS < C.CommittedTS < T.CommitTS. Forward read and write timestamp.
	// 2. WriteTimestamp forward by tscache. Forward write timestamp.
	// 3. Uncertainty error. Forward read and write timestamp.
	if resp.Header.Error != nil {
		if resp.Header.Error.AbortedError != nil {
			return txnpb.TxnBatchResponse{}, ErrTxnAborted
		} else if resp.Header.Error.UncertaintyError != nil {
			return txnpb.TxnBatchResponse{}, ErrTxnUncertainty
		}
		return txnpb.TxnBatchResponse{}, ErrTxnConflict
	}

	if startHeartbeating {
		c.startTxnHeartbeatLocked()
	}

	if !locked {
		c.mu.Lock()
		defer c.mu.Unlock()
	}
	c.updateTxnMetaLocked(resp.Header.Txn)
	return resp, nil
}

// startTxnHeartbeatLocked if a transaction has a write operation, then the transaction
// creates a TxnRecord. In order to keep the transaction alive, we need to periodically
// send heartbeats to the node where the TxnRecord is located, to update the active time
// of the transaction.
func (c *coordinator) startTxnHeartbeatLocked() {
	if c.mu.heartbeating ||
		c.mu.status != txnpb.TxnStatus_Pending {
		return
	}
	c.mu.heartbeating = true
	c.stopper.RunTask(context.Background(), func(ctx context.Context) {
		ticker := time.NewTicker(c.opts.heartbeatDuration)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				c.logger.Debug("heartbeating stopped",
					log.ReasonField("stopper closed"))
				return
			case <-ticker.C:
				if !c.doHeartbeat() {
					return
				}
			}
		}
	})
}

func (c *coordinator) doHeartbeat() bool {
	if !c.isPending() {
		c.logger.Debug("heartbeating stopped",
			log.ReasonField("not pending status"),
			zap.String("status", c.getStatus().String()))
		return false
	}

	resp, err := c.sender.Send(context.TODO(), c.getHeartbeatBatchRequest())
	if err != nil {
		c.logger.Error("send heartbeat failed",
			zap.Error(err))
		return true
	}

	if resp.Header.Error != nil {
		// heartbeat may only receive the error that the transaction has
		// been Aborted.
		if resp.Header.Error.AbortedError != nil {
			c.setAborted()
			c.startAsyncCleanTxnTask()
			return false
		}
		c.logger.Fatal("heartbeating received TxnError",
			zap.String("error", resp.Header.String()))
	}

	c.mu.Lock()
	c.updateTxnMetaLocked(resp.Header.Txn)
	switch resp.Header.Status {
	case txnpb.TxnStatus_Committed:
		c.mu.status = txnpb.TxnStatus_Committed
	case txnpb.TxnStatus_Aborted:
		c.mu.status = txnpb.TxnStatus_Aborted
	}
	status := c.mu.status
	c.mu.Unlock()

	if status.IsFinal() {
		if status == txnpb.TxnStatus_Aborted {
			c.startAsyncCleanTxnTask()
		}
		c.logger.Debug("heartbeating stopped",
			log.ReasonField("final status"),
			zap.String("status", status.String()))
		return false
	}

	return true
}

// startAsyncCleanTxnTask once the heartbeat is discovered and the state of the
// transaction is Aborted, if all temporary data of the transaction is cleaned up
// immediately.
func (c *coordinator) startAsyncCleanTxnTask() {
	c.stopper.RunTask(context.TODO(), func(ctx context.Context) {
		// It's okay if it fails, because the state of TxnRecord is Aborted, and there
		// will be opportunities to trigger the cleanup process later, such as when a
		// conflict occurs.
		_, err := c.send(ctx, endTxn(txnpb.InternalTxnOp_Rollback))
		if err != nil {
			c.logger.Error("async clean txn failed",
				zap.Error(err))
		}
	})
}

func (c *coordinator) getTxnMeta() txnpb.TxnMeta {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.txnMeta
}

func (c *coordinator) getStatus() txnpb.TxnStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.status
}

func (c *coordinator) updateTxnMetaLocked(txn txnpb.TxnMeta) {
	util.LogTxnMeta(c.logger, zap.DebugLevel, "update txn meta", txn)

	if !bytes.Equal(c.mu.txnMeta.ID, txn.ID) {
		c.logger.Fatal("invalid need update TxnMeta", log.HexField("update-txn-id", txn.ID))
	}

	// ignore epoch not match
	if c.mu.txnMeta.Epoch != txn.Epoch {
		c.logger.Debug("ignore update txn meta, epoch not match")
		return
	}

	// update write timestamp if
	// 1. server-side forward the writeTimestamp due to TSCache
	// 2. due to RW conflicts, the write timestamps of write transactions need to be
	//    forward in order to ensure that read transactions can be read without blocking.
	if c.mu.txnMeta.WriteTimestamp.Less(txn.WriteTimestamp) {
		c.mu.txnMeta.WriteTimestamp = txn.WriteTimestamp
	}
}

func (c *coordinator) setAborted() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mu.status = txnpb.TxnStatus_Aborted
}

func (c *coordinator) isPending() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.mu.status == txnpb.TxnStatus_Pending
}

func (c *coordinator) splitBatchRequests(batchRequest *txnpb.TxnBatchRequest, createTxnRecord, hasCommitOrRollback bool) []txnpb.TxnBatchRequest {
	n := len(batchRequest.Requests)
	if n == 2 {
		return c.splitBatchRequestsWithIndex(batchRequest, 1)
	}

	if createTxnRecord && hasCommitOrRollback {
		requests := make([]txnpb.TxnBatchRequest, 0, 3)
		requests = append(requests, txnpb.TxnBatchRequest{
			Header:   batchRequest.Header,
			Requests: batchRequest.Requests[0:1],
		}, txnpb.TxnBatchRequest{
			Header:   batchRequest.Header,
			Requests: batchRequest.Requests[1 : n-1],
		}, txnpb.TxnBatchRequest{
			Header:   batchRequest.Header,
			Requests: batchRequest.Requests[n-1 : n],
		})
		return requests
	}

	if createTxnRecord && !hasCommitOrRollback {
		return c.splitBatchRequestsWithIndex(batchRequest, 1)
	}

	return c.splitBatchRequestsWithIndex(batchRequest, n-1)
}

func (c *coordinator) splitBatchRequestsWithIndex(batchRequest *txnpb.TxnBatchRequest, index int) []txnpb.TxnBatchRequest {
	requests := make([]txnpb.TxnBatchRequest, 0, 2)
	requests = append(requests, txnpb.TxnBatchRequest{
		Header:   batchRequest.Header,
		Requests: batchRequest.Requests[0:index],
	}, txnpb.TxnBatchRequest{
		Header:   batchRequest.Header,
		Requests: batchRequest.Requests[index:],
	})
	return requests
}

func (c *coordinator) getHeartbeatBatchRequest() txnpb.TxnBatchRequest {
	ts := c.txnClocker.Now()
	txn := c.getTxnMeta()
	var batchRequest txnpb.TxnBatchRequest
	batchRequest.Header.Txn.TxnMeta = txn
	batchRequest.Header.Type = txnpb.TxnRequestType_Write
	batchRequest.Requests = append(batchRequest.Requests, txnpb.TxnRequest{
		Operation: txnpb.TxnOperation{
			Op:         uint32(txnpb.InternalTxnOp_Heartbeat),
			ShardGroup: txn.TxnRecordShardGroup,
			Timestamp:  ts,
		},
	})
	return batchRequest
}

func (c *coordinator) supportAsyncConsensus() bool {
	return c.opts.optimize.asynchronousConsensus &&
		!c.maxInfightBytesExceedLocked()
}

func (c *coordinator) inflightKeySetLocked() map[uint64]txnpb.KeySet {
	allInfights := make(map[uint64]txnpb.KeySet, len(c.mu.infightWrites))

	for g, v := range c.mu.infightWrites {
		set := txnpb.KeySet{
			PointKeys: make([][]byte, 0, v.Len()),
			Sorted:    true,
		}
		v.Ascend(func(key []byte) bool {
			set.PointKeys = append(set.PointKeys, key)
			return true
		})
		allInfights[g] = set
	}
	return allInfights
}

func (c *coordinator) completedKeySetLocked() map[uint64]txnpb.KeySet {
	allCompleted := make(map[uint64]txnpb.KeySet, len(c.mu.completedWrites))
	for g, v := range c.mu.completedWrites {
		allCompleted[g] = *v
	}
	return allCompleted
}
