package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
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

	tc.send(context.Background(), txnpb.TxnBatchRequest{})
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
	tc.send(context.Background(), r)
}

func TestSendWithImpactedKeyRangeWillPanic(t *testing.T) {
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
	r.Requests[0].Operation.Impacted.Ranges = []txnpb.KeyRange{{}}
	tc.send(context.Background(), r)
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

	tc.send(context.Background(), newTestPointReadTxnOperation("k1"))
	tc.mu.Lock()
	assert.False(t, tc.mu.heartbeating)
	tc.mu.Unlock()

	tc.send(context.Background(), newTestWriteTxnOperation(false, "k2"))
	tc.mu.Lock()
	assert.True(t, tc.mu.heartbeating)
	tc.mu.Unlock()
}

func TestHeatbeatStartedAfterMultiWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(nil)
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	tc.send(context.Background(), newTestWriteTxnOperation(false, "k1", "k2"))
	tc.mu.Lock()
	assert.True(t, tc.mu.heartbeating)
	tc.mu.Unlock()
}

func TestHeatbeatNotStartAfterFirstWriteWithCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(nil)
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	tc.send(context.Background(), newTestWriteTxnOperation(true, "k2"))
	tc.mu.Lock()
	assert.False(t, tc.mu.heartbeating)
	tc.mu.Unlock()
}

func TestInfightWritesAndCompletedWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()

	block := false
	c := make(chan struct{})
	defer close(c)
	r := make(chan struct{})
	defer close(r)
	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		if block {
			c <- struct{}{}
			<-r
		}
		return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	tc.send(context.Background(), newTestWriteTxnOperation(false, "k1"))

	go func() {
		<-c
		tc.mu.Lock()
		defer tc.mu.Unlock()
		assert.Equal(t, "k1", string(tc.mu.completedWrites.PointKeys[0]))
		assert.Equal(t, "k2", string(tc.mu.infightWrites.PointKeys[0]))
		r <- struct{}{}
	}()
	block = true
	tc.send(context.Background(), newTestWriteTxnOperation(false, "k2"))
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

	tc.send(context.Background(), newTestWriteTxnOperation(false, "k1"))
	assert.Equal(t, 1, len(requests))
	assert.Equal(t, 1, len(requests[0].Requests))
	assert.Equal(t, "k1", string(requests[0].Requests[0].Operation.Payload))
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

	tc.send(context.Background(), newTestWriteTxnOperation(false, "k1", "k2", "k3"))
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

	tc.send(context.Background(), newTestWriteTxnOperation(true, "k1"))
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

	tc.send(context.Background(), newTestWriteTxnOperation(true, "k1", "k2", "k3"))
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

	tc.send(context.Background(), newTestWriteTxnOperation(false, "k0"))
	tc.send(context.Background(), newTestWriteTxnOperation(true, "k1", "k2", "k3"))
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
		resp.Header.Txn.WriteTimestamp = resp.Header.Txn.WriteTimestamp + 1
		return resp, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	tc.send(context.Background(), newTestWriteTxnOperation(false, "k1"))
}

func TestUpdateTxnWriteTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		var resp txnpb.TxnBatchResponse
		resp.Header.Txn = req.Header.Txn.TxnMeta
		resp.Header.Txn.WriteTimestamp = resp.Header.Txn.WriteTimestamp + 1
		return resp, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	tc.mu.Lock()
	ts := tc.mu.txnMeta.WriteTimestamp
	tc.mu.Unlock()

	tc.send(context.Background(), newTestWriteTxnOperation(false, "k0"))
	tc.mu.Lock()
	assert.Equal(t, ts+1, tc.mu.txnMeta.WriteTimestamp)
	tc.mu.Unlock()
}

func TestUpdateTxnWriteTimestampWithLower(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sender := newMockBatchDispatcher(func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
		var resp txnpb.TxnBatchResponse
		resp.Header.Txn = req.Header.Txn.TxnMeta
		resp.Header.Txn.WriteTimestamp = resp.Header.Txn.WriteTimestamp - 1
		return resp, nil
	})

	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 0)
	defer tc.stop()

	tc.mu.Lock()
	ts := tc.mu.txnMeta.WriteTimestamp
	tc.mu.Unlock()

	tc.send(context.Background(), newTestWriteTxnOperation(false, "k0"))
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
		resp.Header.Txn.WriteTimestamp = resp.Header.Txn.WriteTimestamp + 1
		return resp, nil
	})
	tc := newTestTxnCoordinator(sender, "mock-txn", "t1", 1)
	defer tc.stop()

	tc.mu.Lock()
	ts := tc.mu.txnMeta.WriteTimestamp
	tc.mu.Unlock()

	tc.send(context.Background(), newTestWriteTxnOperation(false, "k0"))
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

	tc.send(context.Background(), newTestWriteTxnOperation(false, "k1"))

	select {
	case req := <-hb:
		assert.True(t, req.Requests[0].Operation.Timestamp > 0)
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

func newTestTxnCoordinator(sender BatchDispatcher, name string, id string, epoch uint32) *coordinator {
	clocker := newMockTxnClocker(0)
	ts, skew := clocker.Now()
	return newTxnCoordinator(newTestSITxn(name, id, ts, skew, epoch),
		sender,
		clocker,
		time.Millisecond*10,
		log.GetPanicZapLoggerWithLevel(zap.DebugLevel))
}

func newTestSITxn(name, id string, ts, skew uint64, epoch uint32) txnpb.TxnMeta {
	return txnpb.TxnMeta{
		ID:             []byte("id"),
		Name:           name,
		Isolation:      txnpb.Isolation_SI,
		ReadTimestamp:  ts,
		WriteTimestamp: ts,
		MaxTimestamp:   ts + skew,
		Epoch:          epoch,
	}
}

func newTestPointReadTxnOperation(key string) txnpb.TxnBatchRequest {
	var r txnpb.TxnBatchRequest
	r.Header.Type = txnpb.TxnRequestType_Read
	r.Requests = append(r.Requests,
		txnpb.TxnRequest{Operation: txnpb.TxnOperation{
			Op:       uint32(txnpb.InternalTxnOp_Reserved) + 1,
			Payload:  []byte(key),
			Impacted: txnpb.KeySet{PointKeys: [][]byte{[]byte(key)}},
		}})
	return r
}

func newTestWriteTxnOperation(commit bool, keys ...string) txnpb.TxnBatchRequest {
	var r txnpb.TxnBatchRequest
	r.Header.Type = txnpb.TxnRequestType_Write
	for _, key := range keys {
		r.Requests = append(r.Requests,
			txnpb.TxnRequest{Operation: txnpb.TxnOperation{
				Op:       uint32(txnpb.InternalTxnOp_Reserved) + 2,
				Payload:  []byte(key),
				Impacted: txnpb.KeySet{PointKeys: [][]byte{[]byte(key)}},
			}})
	}
	if commit {
		r.Requests = append(r.Requests, txnpb.TxnRequest{Operation: txnpb.TxnOperation{Op: uint32(txnpb.InternalTxnOp_Commit)}})
	}
	return r
}
