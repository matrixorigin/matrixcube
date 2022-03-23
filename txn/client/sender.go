package client

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixcube/pb/txnpb"
)

// BatchSender BatchRequest sender.
type BatchSender interface {
	// Send based on the routing information in the BatchRequest, a BatchRequest is split
	// into multiple BatchRequests and distributed to the appropriate Shards, and the results
	// are combined and returned.
	Send(context.Context, txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error)
}

type mockBatchSender struct {
	sync.Mutex

	fn func(txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error)
}

func newMockBatchSender(fn func(txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error)) *mockBatchSender {
	if fn == nil {
		fn = func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
			return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
		}
	}
	return &mockBatchSender{fn: fn}
}

func (s *mockBatchSender) Send(ctx context.Context, req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
	s.Lock()
	defer s.Unlock()

	return s.fn(req)
}
