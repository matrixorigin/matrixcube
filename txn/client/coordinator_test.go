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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/util/hlc"
	"github.com/matrixorigin/matrixcube/util/keys"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestSendAfterTxnCommittedOrAbortedOrEnding(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(nil)
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	tc.mu.Lock()
	tc.mu.status = txnpb.TxnStatus_Aborted
	tc.mu.Unlock()
	_, err := tc.send(context.Background(), newTestPointReadTxnOperation("k1"))
	assert.Equal(t, ErrTxnAborted, err)

	tc.mu.Lock()
	tc.mu.status = txnpb.TxnStatus_Committed
	tc.mu.Unlock()
	_, err = tc.send(context.Background(), newTestPointReadTxnOperation("k1"))
	assert.Equal(t, ErrTxnCommitted, err)

	tc.mu.Lock()
	tc.mu.status = txnpb.TxnStatus_Pending
	tc.mu.ending = true
	tc.mu.Unlock()
	_, err = tc.send(context.Background(), newTestPointReadTxnOperation("k1"))
	assert.Equal(t, ErrTxnEnding, err)
}

func TestSendEmptyBatchRequestWillPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func() {
		if r := recover(); r == nil {
			assert.Fail(t, "must panic")
		}
	}()

	sender := newMockBatchDispatcher(nil)
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	_, err := tc.send(context.Background(), txnpb.TxnBatchRequest{})
	assert.NoError(t, err)
}

func TestSendWithoutImpactedKeyWillPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func() {
		if r := recover(); r == nil {
			assert.Fail(t, "must panic")
		}
	}()

	sender := newMockBatchDispatcher(nil)
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	r := newTestWriteTxnOperation(false, "k1")
	r.Requests[0].Operation.Impacted.PointKeys = nil
	_, err := tc.send(context.Background(), r)
	assert.NoError(t, err)
}

func TestSendWithEmptyImpactedWillPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func() {
		if r := recover(); r == nil {
			assert.Fail(t, "must panic")
		}
	}()

	sender := newMockBatchDispatcher(nil)
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	r := newTestWriteTxnOperation(false, "k1")
	r.Requests[0].Operation.Impacted = txnpb.KeySet{}
	_, err := tc.send(context.Background(), r)
	assert.NoError(t, err)
}

func TestSetTxnRecordRouteKeyByFirstWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var last txnpb.TxnBatchRequest
	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		last = req
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	_, err := tc.send(context.Background(), newTestPointReadTxnOperation("k1"))
	assert.NoError(t, err)
	assert.Empty(t, tc.getTxnMeta().TxnRecordRouteKey)

	_, err = tc.send(context.Background(), newTestWriteTxnOperation(false, "k2"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("k2"), tc.getTxnMeta().TxnRecordRouteKey)
	assert.True(t, last.Requests[0].Options.CreateTxnRecord)
}

func TestHeatbeatStartedAfterFirstWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(nil)
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	_, err := tc.send(context.Background(), newTestPointReadTxnOperation("k1"))
	assert.NoError(t, err)
	tc.mu.Lock()
	assert.False(t, tc.mu.heartbeating)
	tc.mu.Unlock()

	_, err = tc.send(context.Background(), newTestWriteTxnOperation(false, "k2"))
	assert.NoError(t, err)
	tc.mu.Lock()
	assert.True(t, tc.mu.heartbeating)
	tc.mu.Unlock()
}

func TestHeatbeatStartedAfterMultiWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(nil)
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(false, "k1", "k2"))
	assert.NoError(t, err)
	tc.mu.Lock()
	assert.True(t, tc.mu.heartbeating)
	tc.mu.Unlock()
}

func TestHeatbeatNotStartAfterFirstWriteWithCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(nil)
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(true, "k2"))
	assert.NoError(t, err)
	tc.mu.Lock()
	assert.False(t, tc.mu.heartbeating)
	tc.mu.Unlock()
}

func TestSingleWriteSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var requests []txnpb.TxnBatchRequest
	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		requests = append(requests, req)
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(false, "k1"))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(requests))
	assert.Equal(t, 1, len(requests[0].Requests))
	assert.Equal(t, "k1", string(requests[0].Requests[0].Operation.Payload))
	tc.mu.Lock()
	assert.Equal(t, 0, tc.mu.infightWrites[0].Len())
	assert.Equal(t, 1, len(tc.mu.completedWrites[0].PointKeys))
	tc.mu.Unlock()
}

func TestWriteWithAsyncConsensus(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var requests []txnpb.TxnBatchRequest
	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		requests = append(requests, req)
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	tc.opts.optimize.asynchronousConsensus = true
	tc.opts.optimize.maxInfilghtKeysBytes = 1024
	defer tc.stop()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(false, "k1"))
	assert.NoError(t, err)
	tc.mu.Lock()
	assert.Equal(t, 1, tc.mu.infightWrites[0].Len())
	assert.Equal(t, 0, len(tc.mu.completedWrites[0].PointKeys))
	tc.mu.Unlock()
}

func TestWriteWithAsyncConsensusAndMaxBytesExceed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var requests []txnpb.TxnBatchRequest
	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		requests = append(requests, req)
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	tc.opts.optimize.asynchronousConsensus = true
	tc.opts.optimize.maxInfilghtKeysBytes = 2
	defer tc.stop()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(false, "k1", "k2"))
	assert.NoError(t, err)
	tc.mu.Lock()
	assert.Equal(t, 1, tc.mu.infightWrites[0].Len())
	assert.Equal(t, 1, len(tc.mu.completedWrites[0].PointKeys))
	assert.Equal(t, 2, len(requests))
	assert.True(t, requests[0].Requests[0].Options.AsynchronousConsensus)
	assert.False(t, requests[1].Requests[0].Options.AsynchronousConsensus)
	tc.mu.Unlock()
}

func TestRangeWriteWithAsyncConsensus(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var requests []txnpb.TxnBatchRequest
	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		requests = append(requests, req)
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	tc.opts.optimize.asynchronousConsensus = true
	tc.opts.optimize.maxInfilghtKeysBytes = 1024

	req := newTestWriteTxnOperation(false, "k1")
	req.Requests[0].Operation.Impacted.Ranges = []txnpb.KeyRange{
		{
			Start: []byte("k1"),
			End:   []byte("k2"),
		},
	}
	_, err := tc.send(context.Background(), req)
	assert.NoError(t, err)
	tc.mu.Lock()
	assert.Equal(t, 0, tc.mu.infightWrites[0].Len())
	assert.Equal(t, 1, len(tc.mu.completedWrites[0].PointKeys))
	assert.Equal(t, 1, len(tc.mu.completedWrites[0].Ranges))
	tc.mu.Unlock()
}

func TestWriteWithAsyncConsensusAndWaitConsensus(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var requests []txnpb.TxnBatchRequest
	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		requests = append(requests, req)
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	tc.opts.optimize.asynchronousConsensus = true
	tc.opts.optimize.maxInfilghtKeysBytes = 1024
	defer tc.stop()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(false, "k1"))
	assert.NoError(t, err)
	tc.mu.Lock()
	assert.Equal(t, 1, tc.mu.infightWrites[0].Len())
	assert.Equal(t, 0, len(tc.mu.completedWrites[0].PointKeys))
	tc.mu.Unlock()

	req := newTestPointReadTxnOperation("k1")
	req.Requests[0].Operation.Op = uint32(txnpb.InternalTxnOp_WaitConsensus)
	_, err = tc.send(context.Background(), req)
	assert.NoError(t, err)
	tc.mu.Lock()
	assert.Equal(t, 0, tc.mu.infightWrites[0].Len())
	assert.Equal(t, 1, len(tc.mu.completedWrites[0].PointKeys))
	tc.mu.Unlock()
}

func TestMultiWriteSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var requests []txnpb.TxnBatchRequest
	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		requests = append(requests, req)
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(false, "k1", "k2", "k3"))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(requests))
	assert.Equal(t, 1, len(requests[0].Requests))
	assert.Equal(t, "k1", string(requests[0].Requests[0].Operation.Payload))
	assert.Equal(t, 2, len(requests[1].Requests))
	assert.Equal(t, "k2", string(requests[1].Requests[0].Operation.Payload))
	assert.Equal(t, "k3", string(requests[1].Requests[1].Operation.Payload))
}

func TestSingleWriteAndCommitSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var requests []txnpb.TxnBatchRequest
	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		requests = append(requests, req)
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(true, "k1"))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(requests))
	assert.Equal(t, 1, len(requests[0].Requests))
	assert.Equal(t, "k1", string(requests[0].Requests[0].Operation.Payload))
	assert.Equal(t, 1, len(requests[1].Requests))
	assert.Equal(t, uint32(txnpb.InternalTxnOp_Commit), requests[1].Requests[0].Operation.Op)
}

func TestMultiWriteAndCommitSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var requests []txnpb.TxnBatchRequest
	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		requests = append(requests, req)
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(true, "k1", "k2", "k3"))
	assert.NoError(t, err)
	assert.Equal(t, 3, len(requests))
	assert.Equal(t, 1, len(requests[0].Requests))
	assert.Equal(t, "k1", string(requests[0].Requests[0].Operation.Payload))
	assert.Equal(t, 2, len(requests[1].Requests))
	assert.Equal(t, "k2", string(requests[1].Requests[0].Operation.Payload))
	assert.Equal(t, "k3", string(requests[1].Requests[1].Operation.Payload))
	assert.Equal(t, 1, len(requests[2].Requests))
	assert.Equal(t, uint32(txnpb.InternalTxnOp_Commit), requests[2].Requests[0].Operation.Op)
}

func TestMultiWriteWithoutCreateTxnRecordAndCommitSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var requests []txnpb.TxnBatchRequest
	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		requests = append(requests, req)
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(false, "k0"))
	assert.NoError(t, err)
	_, err = tc.send(context.Background(), newTestWriteTxnOperation(true, "k1", "k2", "k3"))
	assert.NoError(t, err)
	assert.Equal(t, 3, len(requests))
	assert.Equal(t, 1, len(requests[0].Requests))
	assert.Equal(t, "k0", string(requests[0].Requests[0].Operation.Payload))
	assert.Equal(t, 3, len(requests[1].Requests))
	assert.Equal(t, "k1", string(requests[1].Requests[0].Operation.Payload))
	assert.Equal(t, "k2", string(requests[1].Requests[1].Operation.Payload))
	assert.Equal(t, "k3", string(requests[1].Requests[2].Operation.Payload))
	assert.Equal(t, 1, len(requests[2].Requests))
	assert.Equal(t, uint32(txnpb.InternalTxnOp_Commit), requests[2].Requests[0].Operation.Op)
}

func TestReceivedAbortedError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		var resp txnpb.TxnBatchResponse
		resp.Header.Error = &txnpb.TxnError{}
		resp.Header.Error.AbortedError = &txnpb.AbortedError{}
		return resp, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(false, "k0"))
	assert.Equal(t, ErrTxnAborted, err)
}

func TestReceivedConflictError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		var resp txnpb.TxnBatchResponse
		resp.Header.Error = &txnpb.TxnError{}
		resp.Header.Error.ConflictWithCommittedError = &txnpb.ConflictWithCommittedError{}
		return resp, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(false, "k0"))
	assert.Equal(t, ErrTxnConflict, err)
}

func TestReceivedUncertaintyError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		var resp txnpb.TxnBatchResponse
		resp.Header.Error = &txnpb.TxnError{}
		resp.Header.Error.UncertaintyError = &txnpb.UncertaintyError{}
		return resp, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(false, "k0"))
	assert.Equal(t, ErrTxnUncertainty, err)
}

func TestReceivedOtherError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	otherErr := errors.New("other error")
	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		var resp txnpb.TxnBatchResponse
		return resp, otherErr
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(false, "k0"))
	assert.Equal(t, otherErr, err)
}

func TestUpdateTxnWriteTimestampWithInvalidIDWillPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func() {
		if r := recover(); r == nil {
			assert.Fail(t, "must panic")
		}
	}()

	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		var resp txnpb.TxnBatchResponse
		resp.Header.Txn = req.Header.Txn.TxnMeta
		resp.Header.Txn.ID = []byte{}
		resp.Header.Txn.WriteTimestamp = resp.Header.Txn.WriteTimestamp.Next()
		return resp, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(false, "k1"))
	assert.NoError(t, err)
}

func TestUpdateTxnWriteTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		var resp txnpb.TxnBatchResponse
		resp.Header.Txn = req.Header.Txn.TxnMeta
		resp.Header.Txn.WriteTimestamp = resp.Header.Txn.WriteTimestamp.Next()
		return resp, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	tc.mu.Lock()
	ts := tc.mu.txnMeta.WriteTimestamp
	tc.mu.Unlock()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(false, "k0"))
	assert.NoError(t, err)
	tc.mu.Lock()
	assert.Equal(t, ts.Next(), tc.mu.txnMeta.WriteTimestamp)
	tc.mu.Unlock()
}

func TestUpdateTxnWriteTimestampWithLower(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		var resp txnpb.TxnBatchResponse
		resp.Header.Txn = req.Header.Txn.TxnMeta
		resp.Header.Txn.WriteTimestamp = hlc.Timestamp{
			PhysicalTime: resp.Header.Txn.WriteTimestamp.PhysicalTime - 1,
		}
		return resp, nil
	})

	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	tc.mu.Lock()
	ts := tc.mu.txnMeta.WriteTimestamp
	tc.mu.Unlock()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(false, "k0"))
	assert.NoError(t, err)
	tc.mu.Lock()
	assert.Equal(t, ts, tc.mu.txnMeta.WriteTimestamp)
	tc.mu.Unlock()
}

func TestSkipUpdateTxnWriteTimestampWithInvalidEpoch(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		var resp txnpb.TxnBatchResponse
		resp.Header.Txn = req.Header.Txn.TxnMeta
		resp.Header.Txn.Epoch = 0
		resp.Header.Txn.WriteTimestamp = resp.Header.Txn.WriteTimestamp.Next()
		return resp, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 1)
	defer tc.stop()

	tc.mu.Lock()
	ts := tc.mu.txnMeta.WriteTimestamp
	tc.mu.Unlock()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(false, "k0"))
	assert.NoError(t, err)
	tc.mu.Lock()
	assert.Equal(t, ts, tc.mu.txnMeta.WriteTimestamp)
	tc.mu.Unlock()
}

func TestStartHeartbeatTask(t *testing.T) {
	defer leaktest.AfterTest(t)()

	hb := make(chan txnpb.TxnBatchRequest)
	defer close(hb)
	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		if req.Requests[0].Operation.Op == uint32(txnpb.InternalTxnOp_Heartbeat) {
			hb <- req
		}
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(false, "k1"))
	assert.NoError(t, err)

	select {
	case req := <-hb:
		assert.False(t, req.Requests[0].Operation.Timestamp.IsEmpty())
	case <-time.After(time.Second):
		assert.Fail(t, "heartbeat timeout")
	}
}

func TestCannotStartHeartbeatTaskIfNotPendingStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	tc.mu.Lock()
	tc.mu.status = txnpb.TxnStatus_Committed
	tc.startTxnHeartbeatLocked()
	tc.mu.Unlock()
	assert.Equal(t, int64(0), tc.stopper.GetTaskCount())
}

func TestStopHeartbeatTask(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := make(chan struct{})
	defer close(c)
	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		c <- struct{}{}
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	tc.mu.Lock()
	tc.startTxnHeartbeatLocked()
	tc.mu.Unlock()
	<-c
	assert.Equal(t, int64(1), tc.stopper.GetTaskCount())

	tc.mu.Lock()
	tc.mu.status = txnpb.TxnStatus_Aborted
	tc.mu.Unlock()
	for {
		select {
		case <-time.After(time.Second):
			assert.Fail(t, "heartbeat task stop timeout")
		default:
			if tc.stopper.GetTaskCount() == 0 {
				return
			}
		}
	}
}

func TestHeartbeatTaskStopByResponseStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, status := range []txnpb.TxnStatus{txnpb.TxnStatus_Aborted, txnpb.TxnStatus_Committed} {
		func(status txnpb.TxnStatus) {
			sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
				return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Status: status, Txn: req.Header.Txn.TxnMeta}}, nil
			})
			tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
			defer tc.stop()

			assert.False(t, tc.doHeartbeat())
		}(status)
	}
}

func TestHeartbeatTaskStopByResponseTxnAbortedError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		return txnpb.TxnBatchResponse{
			Header: txnpb.TxnBatchResponseHeader{
				Txn: req.Header.Txn.TxnMeta,
				Error: &txnpb.TxnError{
					AbortedError: &txnpb.AbortedError{},
				},
			},
		}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()
	assert.False(t, tc.doHeartbeat())
}

func TestHeartbeatReceiveOtherTxnErrorWillPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var errors []*txnpb.TxnError
	errors = append(errors, &txnpb.TxnError{
		ConflictWithCommittedError: &txnpb.ConflictWithCommittedError{},
	})
	errors = append(errors, &txnpb.TxnError{
		UncertaintyError: &txnpb.UncertaintyError{},
	})

	for _, err := range errors {
		func(err *txnpb.TxnError) {
			defer func() {
				if r := recover(); r == nil {
					assert.Fail(t, "must panic")
				}
			}()

			sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
				return txnpb.TxnBatchResponse{
					Header: txnpb.TxnBatchResponseHeader{
						Txn:   req.Header.Txn.TxnMeta,
						Error: err,
					},
				}, nil
			})
			tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
			defer tc.stop()

			tc.doHeartbeat()
		}(err)
	}
}

func TestHeartbeatTaskWillContinueIfSendReturnError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		return txnpb.TxnBatchResponse{}, errors.New("error")
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	assert.True(t, tc.doHeartbeat())
}

func TestHeartbeatTaskStopByNotPendingStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, status := range []txnpb.TxnStatus{txnpb.TxnStatus_Aborted, txnpb.TxnStatus_Committed, txnpb.TxnStatus_Staging} {
		func(status txnpb.TxnStatus) {
			sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
				return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Status: status, Txn: req.Header.Txn.TxnMeta}}, nil
			})
			tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
			defer tc.stop()

			tc.mu.Lock()
			tc.mu.status = status
			tc.mu.Unlock()
			assert.False(t, tc.doHeartbeat())
		}(status)
	}
}

func TestAddNothingWithWriteImpacted(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		return txnpb.TxnBatchResponse{}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.mu.infightWrites[0].Add([]byte("k1"))

	req := newTestWriteTxnOperation(false, "k1")
	tc.maybeInsertWaitConsensusLocked(&req)
	assert.Equal(t, 1, len(req.Requests))
}

func TestAddAllInfightWithCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		return txnpb.TxnBatchResponse{}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.opts.optimize.asynchronousConsensus = true
	tc.opts.optimize.maxInfilghtKeysBytes = 1024
	tc.mu.infightWrites[0].Add([]byte("k1"))
	tc.mu.infightWrites[0].Add([]byte("k2"))

	req := newTestWriteTxnOperation(true, "k1")
	tc.maybeInsertWaitConsensusLocked(&req)
	assert.Equal(t, 4, len(req.Requests))
	assert.True(t, req.Requests[1].IsWaitConsensus())
	assert.Equal(t, []byte("k1"), req.Requests[1].Operation.Impacted.PointKeys[0])
	assert.True(t, req.Requests[2].IsWaitConsensus())
	assert.Equal(t, []byte("k2"), req.Requests[2].Operation.Impacted.PointKeys[0])
}

func TestAddInfightWithPointRead(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		return txnpb.TxnBatchResponse{}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.opts.optimize.asynchronousConsensus = true
	tc.opts.optimize.maxInfilghtKeysBytes = 1024
	tc.mu.infightWrites[0].Add([]byte("k1"))
	tc.mu.infightWrites[0].Add([]byte("k2"))
	tc.mu.infightWrites[0].Add([]byte("k3"))

	req := newTestPointReadTxnOperation("k4", "k2")
	tc.maybeInsertWaitConsensusLocked(&req)
	assert.Equal(t, 3, len(req.Requests))
	assert.True(t, req.Requests[1].IsWaitConsensus())
	assert.Equal(t, []byte("k2"), req.Requests[1].Operation.Impacted.PointKeys[0])
}

func TestAddInfightWithMultiPointRead(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		return txnpb.TxnBatchResponse{}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.opts.optimize.asynchronousConsensus = true
	tc.opts.optimize.maxInfilghtKeysBytes = 1024
	tc.mu.infightWrites[0].Add([]byte("k1"))
	tc.mu.infightWrites[0].Add([]byte("k2"))
	tc.mu.infightWrites[0].Add([]byte("k3"))

	req := newTestPointReadTxnOperation("k2", "k2")
	tc.maybeInsertWaitConsensusLocked(&req)
	assert.Equal(t, 3, len(req.Requests))
	assert.True(t, req.Requests[0].IsWaitConsensus())
	assert.Equal(t, []byte("k2"), req.Requests[0].Operation.Impacted.PointKeys[0])
}

func TestAddAllInfightsWithMultiPointRead(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		return txnpb.TxnBatchResponse{}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tc.opts.optimize.asynchronousConsensus = true
	tc.opts.optimize.maxInfilghtKeysBytes = 1024
	tc.mu.infightWrites[0].Add([]byte("k1"))
	tc.mu.infightWrites[0].Add([]byte("k2"))
	tc.mu.infightWrites[0].Add([]byte("k3"))

	req := newTestPointReadTxnOperation("k1", "k2", "k3", "k3")
	tc.maybeInsertWaitConsensusLocked(&req)
	assert.Equal(t, 7, len(req.Requests))
	assert.True(t, req.Requests[0].IsWaitConsensus())
	assert.Equal(t, []byte("k1"), req.Requests[0].Operation.Impacted.PointKeys[0])
	assert.True(t, req.Requests[2].IsWaitConsensus())
	assert.Equal(t, []byte("k2"), req.Requests[2].Operation.Impacted.PointKeys[0])
	assert.True(t, req.Requests[4].IsWaitConsensus())
	assert.Equal(t, []byte("k3"), req.Requests[4].Operation.Impacted.PointKeys[0])
}

func TestCommitWillAttachedInfightAndCompletedWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var commit txnpb.TxnBatchRequest
	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		if req.HasCommitOrRollback() {
			commit = req
		}
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	tc.mu.Lock()
	tc.opts.optimize.asynchronousConsensus = true
	tc.opts.optimize.maxInfilghtKeysBytes = 1024
	tc.mu.infightWrites[0].Add([]byte("k1"))
	tc.mu.infightWrites[0].Add([]byte("k2"))
	tc.mu.infightWrites[0].Add([]byte("k3"))
	tc.mu.completedWrites[0].AddPointKeys([][]byte{[]byte("k4"), []byte("k5")})
	tc.mu.Unlock()

	_, err := tc.send(context.Background(), newTestWriteTxnOperation(true, "k6"))
	assert.NoError(t, err)
	assert.NotNil(t, commit.Header.Txn.InfightWrites)
	assert.NotNil(t, commit.Header.Txn.InfightWrites[0].Sorted)
	assert.Equal(t, 4, len(commit.Header.Txn.InfightWrites[0].PointKeys))
	assert.NotNil(t, commit.Header.Txn.CompletedWrites)
	assert.Equal(t, 2, len(commit.Header.Txn.CompletedWrites[0].PointKeys))
}

func newTestTxnCoordinator(sender BatchDispatcher, name string, id string, epoch uint32) *coordinator {
	clock := newHLCTxnClock(time.Millisecond * 500)
	ts, max := clock.Now()
	tc := newTxnCoordinator(newTestSITxn(name, id, ts, max, epoch),
		sender,
		clock,
		log.GetPanicZapLoggerWithLevel(zap.DebugLevel),
		txnOptions{heartbeatDuration: time.Millisecond * 10})
	tc.mu.infightWrites[0] = keys.NewKeyTree(32)
	tc.mu.completedWrites[0] = &txnpb.KeySet{}
	return tc
}

func newTestSITxn(name, id string, ts, maxTS hlc.Timestamp, epoch uint32) txnpb.TxnMeta {
	return txnpb.TxnMeta{
		ID:             []byte("id"),
		Name:           name,
		IsolationLevel: txnpb.IsolationLevel_SnapshotSerializable,
		ReadTimestamp:  ts,
		WriteTimestamp: ts,
		MaxTimestamp:   maxTS,
		Epoch:          epoch,
	}
}

func newTestPointReadTxnOperation(keys ...string) txnpb.TxnBatchRequest {
	var r txnpb.TxnBatchRequest
	r.Header.Type = txnpb.TxnRequestType_Read
	for _, key := range keys {
		op := txnpb.NewReadOperation(uint32(txnpb.InternalTxnOp_Reserved)+1, []byte(key),
			txnpb.KeySet{PointKeys: [][]byte{[]byte(key)}})
		r.Requests = append(r.Requests, txnpb.TxnRequest{Operation: op})
	}

	return r
}

func newTestWriteTxnOperation(commit bool, keys ...string) txnpb.TxnBatchRequest {
	var r txnpb.TxnBatchRequest
	r.Header.Type = txnpb.TxnRequestType_Write
	for _, key := range keys {
		op := txnpb.NewWriteOnlyOperation(uint32(txnpb.InternalTxnOp_Reserved)+2, []byte(key),
			txnpb.KeySet{PointKeys: [][]byte{[]byte(key)}})
		r.Requests = append(r.Requests, txnpb.TxnRequest{Operation: op})
	}

	if commit {
		r.Requests = append(r.Requests, txnpb.TxnRequest{Operation: txnpb.TxnOperation{Op: uint32(txnpb.InternalTxnOp_Commit)}})
	}
	return r
}
