package client

import (
	"context"
	"sync"

	"github.com/fagongzi/util/protoc"
	raftstoreClient "github.com/matrixorigin/matrixcube/client"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/util/stop"
	"go.uber.org/zap"
)

// BatchDispatcher BatchRequest sender.
type BatchDispatcher interface {
	// Send based on the routing information in the BatchRequest, a BatchRequest is split
	// into multiple BatchRequests and distributed to the appropriate Shards, and the results
	// are combined and returned.
	Send(context.Context, txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error)
	// Close close the batch dispatcher
	Close()
}

// NewBatchDispatcher
func NewBatchDispatcher(client raftstoreClient.Client,
	replicaSelectPolicy rpcpb.ReplicaSelectPolicy,
	router TxnOperationRouter,
	txnClocker TxnClocker,
	logger *zap.Logger) BatchDispatcher {
	return &batchDispatcher{
		logger:              logger,
		router:              router,
		client:              client,
		txnClocker:          txnClocker,
		replicaSelectPolicy: replicaSelectPolicy,
		stopper:             stop.NewStopper("txn-batch-dispatcher"),
	}
}

type batchDispatcher struct {
	logger              *zap.Logger
	replicaSelectPolicy rpcpb.ReplicaSelectPolicy
	client              raftstoreClient.Client
	router              TxnOperationRouter
	txnClocker          TxnClocker
	stopper             *stop.Stopper
}

func (s *batchDispatcher) Send(ctx context.Context, request txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
	if len(request.Requests) == 0 {
		s.logger.Fatal("empty request to send to raftstore")
	}
	if _, ok := ctx.Deadline(); !ok {
		s.logger.Fatal("context must use timeout context")
	}

	if err := s.maybeTimeout(ctx); err != nil {
		return txnpb.TxnBatchResponse{}, err
	}

	result := dispatchResult{
		txnClocker: s.txnClocker,
	}
	for shard, req := range s.routeRequest(request) {
		result.wg.Add(1)
		s.doSendToShard(ctx, shard, req, &result)
	}
	return result.get()
}

func (s *batchDispatcher) Close() {
	s.stopper.Stop()
}

// routeRequest for custom read and write requests, a request may contain multiple data operations, so a request
// needs to be split into multiple requests to be sent to the corresponding Shard.
func (s *batchDispatcher) routeRequest(request txnpb.TxnBatchRequest) map[uint64]txnpb.TxnBatchRequest {
	requests := make(map[uint64]txnpb.TxnBatchRequest)
	appendRequest := func(toShard uint64, req txnpb.TxnRequest) {
		if m, ok := requests[toShard]; ok {
			m.Requests = append(m.Requests, req)
		} else {
			requests[toShard] = txnpb.TxnBatchRequest{
				Header:   request.Header,
				Requests: []txnpb.TxnRequest{req},
			}
		}
	}
	for idx := range request.Requests {
		if request.Requests[idx].IsInternal() {
			switch txnpb.InternalTxnOp(request.Requests[idx].Operation.Op) {
			case txnpb.InternalTxnOp_Heartbeat,
				txnpb.InternalTxnOp_Commit,
				txnpb.InternalTxnOp_Rollback:
				toShard := s.client.Router().SelectShardIDByKey(request.Requests[idx].Operation.ShardGroup,
					request.Header.Txn.TxnRecordRouteKey)
				appendRequest(toShard, request.Requests[idx])
				break
			}
		} else {
			routeInfos, err := s.router.Route(request.Requests[idx].Operation)
			if err != nil {
				s.logger.Fatal("split txn operation failed",
					zap.Error(err))
			}
			for i := range routeInfos {
				appendRequest(routeInfos[i].ShardID, txnpb.TxnRequest{
					Operation: routeInfos[i].Operation,
					Options:   request.Requests[idx].Options,
				})
			}
		}
	}
	return requests
}

func (s *batchDispatcher) doSendToShard(ctx context.Context, shard uint64, req txnpb.TxnBatchRequest, result *dispatchResult) {
	s.stopper.RunTask(ctx, func(ctx context.Context) {
		var resp txnpb.TxnBatchResponse
		var err error

		if err := s.maybeTimeout(ctx); err != nil {
			result.done(resp, err)
			return
		}

		fn := s.client.Write
		if req.Header.Type == txnpb.TxnRequestType_Read {
			fn = s.client.Read
		}
		options := []raftstoreClient.Option{raftstoreClient.WithShard(shard),
			raftstoreClient.WithReplicaSelectPolicy(s.replicaSelectPolicy)}
		if !req.OnlyContainsSingleKey() {
			min, max := req.GetMultiKeyRange()
			options = append(options, raftstoreClient.WithKeysRange(min, max))
		}

		// TODO: register cmd handler without consensus
		f := fn(ctx, 1, protoc.MustMarshal(&req), options...)
		v, err := f.Get()
		f.Close()
		if err != nil {
			if err == raftstore.ErrKeysNotInShard || raftstore.IsShardUnavailableErr(err) {
				resp, err = s.handleNeedReRoute(ctx, req)
				result.done(resp, err)
				return
			}

			result.done(resp, err)
			return
		}

		protoc.MustUnmarshal(&resp, v)
		result.done(resp, err)
	})
}

func (s *batchDispatcher) handleNeedReRoute(ctx context.Context, req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
	for {
		resp, err := s.Send(ctx, req)
		if err == raftstore.ErrKeysNotInShard || raftstore.IsShardUnavailableErr(err) {
			continue
		}

		return resp, err
	}
}

func (s *batchDispatcher) maybeTimeout(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

type dispatchResult struct {
	sync.Mutex
	err        error
	resp       txnpb.TxnBatchResponse
	wg         sync.WaitGroup
	txnClocker TxnClocker
}

func (dr *dispatchResult) get() (txnpb.TxnBatchResponse, error) {
	dr.wg.Wait()
	return dr.resp, dr.err
}

func (dr *dispatchResult) done(res txnpb.TxnBatchResponse, err error) {
	defer dr.wg.Done()
	dr.Lock()
	defer dr.Unlock()

	if err != nil {
		dr.err = err
		return
	}
	if dr.err != nil {
		return
	}

	if res.Header.Error != nil {
		if dr.resp.Header.Error == nil {
			dr.resp.Header.Error = res.Header.Error
			return
		}

		dr.updateTxnErrorLocked(res.Header.Error)
		return
	}

	// Previous has txn error, ignore normal response. The original BatchRequest will
	// retry as a whole.
	if dr.resp.Header.Error != nil {
		return
	}

	dr.resp.Responses = append(dr.resp.Responses, res.Responses...)
	if dr.resp.Header.Txn.IsEmpty() {
		dr.resp.Header.Txn = res.Header.Txn
	} else if dr.txnClocker.Compare(dr.resp.Header.Txn.WriteTimestamp, res.Header.Txn.WriteTimestamp) < 0 {
		// Determine the maximum write timestamp due to TSCache (RW Conflict)
		dr.resp.Header.Txn.WriteTimestamp = res.Header.Txn.WriteTimestamp
	}
}

func (dr *dispatchResult) updateTxnErrorLocked(currentTxnErr *txnpb.TxnError) {
	previousTxnErr := dr.resp.Header.Error

	// the transaction has been aborted and does not care about other errors, either the
	// transaction is restarted or a client error is returned.
	if previousTxnErr.Aborted() {
		return
	}
	if currentTxnErr.Aborted() {
		dr.resp.Header.Error = currentTxnErr
		return
	}

	// 1. Previous is UncertaintyError => Use max(err.MinTimestamp) to update UncertaintyError.MinTimestamp
	// 2. Previous is ConflictWithCommittedError, current is UncertaintyError => Use UncertaintyError instead,
	//    and use max(err.MinTimestamp) to update UncertaintyError.MinTimestamp
	// 3. Previous is ConflictWithCommittedError, current is ConflictWithCommittedError, use max(err.MinTimestamp)
	//    to update ConflictWithCommittedError.MinTimestamp
	if previousTxnErr.UncertaintyError != nil {
		if currentTxnErr.UncertaintyError != nil {
			if dr.txnClocker.Compare(previousTxnErr.UncertaintyError.MinTimestamp,
				currentTxnErr.UncertaintyError.MinTimestamp) < 0 {
				previousTxnErr.UncertaintyError.MinTimestamp = currentTxnErr.UncertaintyError.MinTimestamp
			}
		} else if currentTxnErr.ConflictWithCommittedError != nil {
			if dr.txnClocker.Compare(previousTxnErr.UncertaintyError.MinTimestamp,
				currentTxnErr.ConflictWithCommittedError.MinTimestamp) < 0 {
				previousTxnErr.UncertaintyError.MinTimestamp = currentTxnErr.ConflictWithCommittedError.MinTimestamp
			}
		}
	} else if previousTxnErr.ConflictWithCommittedError != nil {
		if currentTxnErr.UncertaintyError != nil {
			if dr.txnClocker.Compare(previousTxnErr.ConflictWithCommittedError.MinTimestamp,
				currentTxnErr.UncertaintyError.MinTimestamp) > 0 {
				currentTxnErr.UncertaintyError.MinTimestamp = previousTxnErr.ConflictWithCommittedError.MinTimestamp
			}
			previousTxnErr.ConflictWithCommittedError = nil
			previousTxnErr.UncertaintyError = currentTxnErr.UncertaintyError
		} else if currentTxnErr.ConflictWithCommittedError != nil {
			if dr.txnClocker.Compare(previousTxnErr.ConflictWithCommittedError.MinTimestamp,
				currentTxnErr.ConflictWithCommittedError.MinTimestamp) < 0 {
				previousTxnErr.ConflictWithCommittedError.MinTimestamp = currentTxnErr.ConflictWithCommittedError.MinTimestamp
			}
		}
	}
}

type mockBatchDispatcher struct {
	sync.Mutex

	fn func(txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error)
}

func newMockBatchDispatcher(fn func(txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error)) *mockBatchDispatcher {
	if fn == nil {
		fn = func(req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
			return txnpb.TxnBatchResponse{Header: txnpb.TxnBatchResponseHeader{Txn: req.Header.Txn.TxnMeta}}, nil
		}
	}
	return &mockBatchDispatcher{fn: fn}
}

func (s *mockBatchDispatcher) Send(ctx context.Context, req txnpb.TxnBatchRequest) (txnpb.TxnBatchResponse, error) {
	s.Lock()
	defer s.Unlock()

	return s.fn(req)
}

func (s *mockBatchDispatcher) Close() {

}
