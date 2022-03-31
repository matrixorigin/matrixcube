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

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/txn/util"
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
	txnClocker TxnClocker,
	txnHeartbeatDuration time.Duration,
	logger *zap.Logger) *coordinator {
	c := &coordinator{
		sender:               sender,
		logger:               logger.With(log.HexField("txn-id", txnMeta.ID)),
		txnClocker:           txnClocker,
		txnHeartbeatDuration: txnHeartbeatDuration,
		stopper:              stop.NewStopper("txn-"+txnMeta.Name, stop.WithLogger(logger)),
	}
	c.mu.txnMeta = txnMeta
	c.mu.status = txnpb.TxnStatus_Pending
	return c
}

type coordinator struct {
	logger               *zap.Logger
	sender               BatchDispatcher
	txnClocker           TxnClocker
	txnHeartbeatDuration time.Duration
	stopper              *stop.Stopper

	mu struct {
		sync.Mutex

		// commtting or rollingback, all txn operation are disabled.
		ending       bool
		heartbeating bool

		status  txnpb.TxnStatus
		txnMeta txnpb.TxnMeta
		// infightWrites record the current transaction has not completed the consensus write
		// operation.
		infightWrites txnpb.KeySet
		// completedWrites record the current transaction has completed the consensus write
		// operation.
		completedWrites txnpb.KeySet
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

	// TODO: with pipline optimization on, you can only move to completedWrites after explicitly
	// querying for written temporary data
	defer c.moveToCompletedWrites(&batchRequest)

	// create TxnRecord request need to be completed first, commit requests need to be completed last
	// (no parallel commit optimization), and they cannot be executed together with other write requests
	// in a Batch.
	sendDirect := n == 1 || // only one write request
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

	if batchRequest.Header.Type == txnpb.TxnRequestType_Write {
		c.mu.sequence++
		c.addInfightWritesLocked(batchRequest)

		if c.mu.sequence == 1 {
			// For the first write operation, need to explicitly tell the TxnManager to create
			// the TxnRecord synchronously. We use the first Key of the first write request as
			// the TxnRecordRouteKey.
			c.mu.txnMeta.TxnRecordRouteKey = batchRequest.Requests[0].Operation.Impacted.PointKeys[0]
			c.mu.txnMeta.TxnRecordShardGroup = batchRequest.Requests[0].Operation.ShardGroup
			batchRequest.Requests[0].Options.CreateTxnRecord = true
			createTxnRecord = true
		}
	}
	batchRequest.Header.Txn.TxnMeta = c.mu.txnMeta
	batchRequest.Header.Txn.Sequence = c.mu.sequence
	// If the current BatchRequest contains requests for commit or rollback transactions, we need
	// to append the Key of all the operations performed and let TxnManager resolve the temporary
	// data.
	if batchRequest.HasCommitOrRollback() {
		batchRequest.Header.Txn.InfightWrites = &c.mu.infightWrites
		batchRequest.Header.Txn.CompletedWrites = &c.mu.completedWrites
		c.mu.ending = true
	}

	return createTxnRecord, nil
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
		if !batchRequest.Requests[idx].IsInternal() {
			if !batchRequest.Requests[idx].Operation.Impacted.HasPointKeys() {
				c.logger.Fatal("write operation has no impacted pointKeys")
			}
			if batchRequest.Requests[idx].Operation.Impacted.HasKeyRanges() {
				c.logger.Fatal("write operation has impacted key ranges, only support pointKeys for write")
			}

			c.mu.infightWrites.PointKeys = append(c.mu.infightWrites.PointKeys,
				batchRequest.Requests[idx].Operation.Impacted.PointKeys...)
		}
	}
}

func (c *coordinator) moveToCompletedWrites(batchRequest *txnpb.TxnBatchRequest) {
	if batchRequest.Header.Type != txnpb.TxnRequestType_Write {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var completedPointKeys txnpb.KeySet
	for idx := range batchRequest.Requests {
		if !batchRequest.Requests[idx].IsInternal() {
			completedPointKeys.PointKeys = append(completedPointKeys.PointKeys,
				batchRequest.Requests[idx].Operation.Impacted.PointKeys...)
		}
	}

	// TODO: use a more efficient data structure to store infilghtWrites and facilitate
	// the movement of consensus-complete Keys to completedWrites.
	if completedPointKeys.HasPointKeys() {
		var newInfightKeys [][]byte
		for _, key := range c.mu.infightWrites.PointKeys {
			if !completedPointKeys.HasPointKey(key) {
				newInfightKeys = append(newInfightKeys, key)
			}
		}
		c.mu.infightWrites.PointKeys = newInfightKeys
		c.mu.completedWrites.PointKeys = append(c.mu.completedWrites.PointKeys, completedPointKeys.PointKeys...)
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
		ticker := time.NewTicker(c.txnHeartbeatDuration)
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
	if c.txnClocker.Compare(c.mu.txnMeta.WriteTimestamp, txn.WriteTimestamp) < 0 {
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
	ts, _ := c.txnClocker.Now()
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
