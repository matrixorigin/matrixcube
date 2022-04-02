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
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/protoc"
	raftstoreClient "github.com/matrixorigin/matrixcube/client"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/errorpb"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/raftstore"
	"github.com/matrixorigin/matrixcube/util/keys"
	"github.com/matrixorigin/matrixcube/util/leaktest"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestRouteRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()

	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "10/11,20/21,30/31")
	addTestShard(router, 2, "100/101,200/201,300/301")
	addTestShard(router, 3, "1000/1001,2000/2001,3000/3001")

	client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) { return rpcpb.ResponseBatch{}, nil })
	defer client.Stop()

	bd := newTestBatchDispatcher(client)
	defer bd.Close()

	req := newTestBatchRequest(1)
	s, m := bd.routeRequest(req)
	assert.Equal(t, s, uint64(0))
	assert.Equal(t, 1, len(m))
	assert.Equal(t, 1, len(m[1].Requests))
	assert.Equal(t, req.Requests[0].Operation.Impacted.PointKeys, m[1].Requests[0].Operation.Impacted.PointKeys)

	req = newTestBatchRequest(1)
	req.Header.Type = txnpb.TxnRequestType_Write
	s, m = bd.routeRequest(req)
	assert.Equal(t, s, uint64(0))
	assert.Equal(t, 1, len(m))
	assert.Equal(t, 1, len(m[1].Requests))
	assert.Equal(t, req.Requests[0].Operation.Impacted.PointKeys, m[1].Requests[0].Operation.Impacted.PointKeys)

	req = newTestBatchRequest(1)
	req.Header.Type = txnpb.TxnRequestType_Write
	req.Header.Options.CreateTxnRecord = true
	s, m = bd.routeRequest(req)
	assert.Equal(t, s, uint64(1))
	assert.Equal(t, 1, len(m))
	assert.Equal(t, 1, len(m[1].Requests))
	assert.Equal(t, req.Requests[0].Operation.Impacted.PointKeys, m[1].Requests[0].Operation.Impacted.PointKeys)

	req = newTestBatchRequest(1, 2)
	s, m = bd.routeRequest(req)
	assert.Equal(t, s, uint64(0))
	assert.Equal(t, 2, len(m))
	assert.Equal(t, 1, len(m[1].Requests))
	assert.Equal(t, 1, len(m[2].Requests))
	assert.Equal(t, req.Requests[0].Operation.Impacted.PointKeys[:1], m[1].Requests[0].Operation.Impacted.PointKeys)
	assert.Equal(t, req.Requests[0].Operation.Impacted.PointKeys[1:], m[2].Requests[0].Operation.Impacted.PointKeys)

	req = newTestBatchRequest(1, 2)
	req.Header.Type = txnpb.TxnRequestType_Write
	req.Header.Options.CreateTxnRecord = true
	s, m = bd.routeRequest(req)
	assert.Equal(t, s, uint64(1))
	assert.Equal(t, 2, len(m))
	assert.Equal(t, 1, len(m[1].Requests))
	assert.Equal(t, 1, len(m[2].Requests))
	assert.True(t, m[1].Header.Options.CreateTxnRecord)
	assert.False(t, m[2].Header.Options.CreateTxnRecord)
	assert.Equal(t, req.Requests[0].Operation.Impacted.PointKeys[:1], m[1].Requests[0].Operation.Impacted.PointKeys)
	assert.Equal(t, req.Requests[0].Operation.Impacted.PointKeys[1:], m[2].Requests[0].Operation.Impacted.PointKeys)

}

func TestDispatcherSend(t *testing.T) {
	defer leaktest.AfterTest(t)()

	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "10/11,20/21,30/31")

	client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
		return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
			Responses: []txnpb.TxnResponse{{Data: []byte("ok")}},
		}}}}, nil
	})
	defer client.Stop()

	bd := newTestBatchDispatcher(client)
	defer bd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	resp, err := bd.Send(ctx, newTestBatchRequest(1))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp.Responses))
	assert.Equal(t, "ok", string(resp.Responses[0].Data))
}

func TestDispatcherSendWithCreateTxnRecordToMultiShards(t *testing.T) {
	defer leaktest.AfterTest(t)()

	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "10/11,20/21,30/31")
	addTestShard(router, 2, "100/101,200/201,300/301")
	addTestShard(router, 3, "1000/1001,2000/2001,3000/3001")

	client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
		return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
			Responses: []txnpb.TxnResponse{{Data: []byte("ok")}},
		}}}}, nil
	})
	defer client.Stop()

	bd := newTestBatchDispatcher(client)
	defer bd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	req := newTestBatchRequest(1, 2, 3)
	req.Header.Type = txnpb.TxnRequestType_Write
	req.Header.Options.CreateTxnRecord = true
	resp, err := bd.Send(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(resp.Responses))
	for i := 0; i < 3; i++ {
		assert.Equal(t, "ok", string(resp.Responses[0].Data))
	}
}

func TestDispatcherSendWithInternal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "10/11,20/21,30/31")

	client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
		return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
			Responses: []txnpb.TxnResponse{{Data: []byte("ok")}},
		}}}}, nil
	})
	defer client.Stop()

	bd := newTestBatchDispatcher(client)
	defer bd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	resp, err := bd.Send(ctx, newTestBatchInternalRequest(1))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp.Responses))
	assert.Equal(t, "ok", string(resp.Responses[0].Data))
}

func TestDispatcherSendWithTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()

	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "10/11,20/21,30/31")

	client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
		return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
			Responses: []txnpb.TxnResponse{{Data: []byte("ok")}},
		}}}}, nil
	})
	defer client.Stop()

	bd := newTestBatchDispatcher(client)
	defer bd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	time.Sleep(time.Millisecond * 10)
	_, err := bd.Send(ctx, newTestBatchRequest(1))
	assert.Error(t, err)
}

func TestDispatcherDoSendWithTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()

	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "10/11,20/21,30/31")

	client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
		return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
			Responses: []txnpb.TxnResponse{{Data: []byte("ok")}},
		}}}}, nil
	})
	defer client.Stop()

	bd := newTestBatchDispatcher(client)
	defer bd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	time.Sleep(time.Millisecond * 10)
	var result dispatchResult
	result.wg.Add(1)
	bd.doSendToShard(ctx, 1, newTestBatchRequest(1), &result)
	_, err := result.get()
	assert.Error(t, err)
}

func TestDispatcherDoSendWithMultiKeysAndReroute(t *testing.T) {
	defer leaktest.AfterTest(t)()

	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "10/11,20/21,30/31")
	addTestShard(router, 2, "100/101,200/201,300/301")
	addTestShard(router, 3, "1000/1001,2000/2001,3000/3001")

	client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
		return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
			Responses: []txnpb.TxnResponse{{Data: []byte("ok")}},
		}}}}, nil
	})
	defer client.Stop()

	bd := newTestBatchDispatcher(client)
	defer bd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	var result dispatchResult
	result.wg.Add(1)
	bd.doSendToShard(ctx, 1, newTestBatchRequest(1, 2, 3), &result)
	resp, err := result.get()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(resp.Responses))
}

func TestDispatchEmptyRequestsWillPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func() {
		if err := recover(); err == nil {
			assert.Fail(t, "must panic")
		}
	}()

	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "10/11,20/21,30/31")

	client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
		return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, Value: protoc.MustMarshal(&txnpb.TxnBatchResponse{
			Responses: []txnpb.TxnResponse{{Data: []byte("ok")}},
		})}}}, nil
	})
	defer client.Stop()

	bd := newTestBatchDispatcher(client)
	defer bd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	bd.Send(ctx, txnpb.TxnBatchRequest{})
}

func TestDispatchNonTimeoutContextRequestsWillPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	defer func() {
		if err := recover(); err == nil {
			assert.Fail(t, "must panic")
		}
	}()

	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "10/11,20/21,30/31")

	client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
		return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, Value: protoc.MustMarshal(&txnpb.TxnBatchResponse{
			Responses: []txnpb.TxnResponse{{Data: []byte("ok")}},
		})}}}, nil
	})
	defer client.Stop()

	bd := newTestBatchDispatcher(client)
	defer bd.Close()

	bd.Send(context.Background(), newTestBatchRequest(1))
}

func TestDispatcherSendWithRetryableErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	retryableErrors := []errorpb.Error{
		{
			Message:       "retryable error",
			ShardNotFound: &errorpb.ShardNotFound{},
		},
		{
			Message:   "retryable error",
			NotLeader: &errorpb.NotLeader{},
		},
		{
			Message:       "retryable error",
			KeyNotInShard: &errorpb.KeyNotInShard{},
		},
		{
			Message:    "retryable error",
			StaleEpoch: &errorpb.StaleEpoch{},
		},
		{
			Message:      "retryable error",
			ServerIsBusy: &errorpb.ServerIsBusy{},
		},
		{
			Message:      "retryable error",
			StaleCommand: &errorpb.StaleCommand{},
		},
		{
			Message:       "retryable error",
			StoreMismatch: &errorpb.StoreMismatch{},
		},
	}

	for _, rerr := range retryableErrors {
		func(rerr errorpb.Error) {
			router := raftstore.NewMockRouter()
			addTestShard(router, 1, "10/11,20/21,30/31")
			var lock sync.Mutex
			idx := 0
			client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
				lock.Lock()
				defer lock.Unlock()

				if idx == 0 {
					idx++
					return rpcpb.ResponseBatch{Header: rpcpb.ResponseBatchHeader{
						Error: rerr,
					}, Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
						Responses: []txnpb.TxnResponse{{Data: []byte("ok")}},
					}}}}, nil
				} else {
					return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
						Responses: []txnpb.TxnResponse{{Data: []byte("ok")}},
					}}}}, nil
				}
			})
			defer client.Stop()

			bd := newTestBatchDispatcher(client)
			defer bd.Close()

			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			resp, err := bd.Send(ctx, newTestBatchRequest(1))
			assert.NoError(t, err)
			assert.Equal(t, 1, len(resp.Responses))
			assert.Equal(t, "ok", string(resp.Responses[0].Data))
			assert.Equal(t, 1, idx)
		}(rerr)
	}
}

func TestDispatcherSendWithShardUnavailable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "10/11,20/21,30/31")

	idx := 0
	client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
		if idx == 0 {
			idx++
			return rpcpb.ResponseBatch{Header: rpcpb.ResponseBatchHeader{
				Error: errorpb.Error{
					Message:          "retryable error",
					ShardUnavailable: &errorpb.ShardUnavailable{},
				},
			}, Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
				Responses: []txnpb.TxnResponse{{Data: []byte("ok")}},
			}}}}, nil
		} else {
			return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
				Responses: []txnpb.TxnResponse{{Data: []byte("ok")}},
			}}}}, nil
		}
	})
	defer client.Stop()

	bd := newTestBatchDispatcher(client)
	defer bd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	resp, err := bd.Send(ctx, newTestBatchRequest(1))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp.Responses))
	assert.Equal(t, "ok", string(resp.Responses[0].Data))
	assert.Equal(t, 1, idx)
}

func TestDispatcherSendWithOtherError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "10/11,20/21,30/31")

	client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
		return rpcpb.ResponseBatch{}, errors.New("others")
	})
	defer client.Stop()

	bd := newTestBatchDispatcher(client)
	defer bd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	_, err := bd.Send(ctx, newTestBatchRequest(1))
	assert.Equal(t, "others", err.Error())
}

func TestDispatcherSendWithRouteToMoreShard(t *testing.T) {
	defer leaktest.AfterTest(t)()

	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "10/11,20/21,30/31")
	addTestShard(router, 2, "100/101,200/201,300/301")
	addTestShard(router, 3, "100/101,200/201,300/301")

	client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
		data := format.Uint64ToBytes(r.ToShard)
		return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
			Responses: []txnpb.TxnResponse{{Data: data}},
		}}}}, nil
	})
	defer client.Stop()

	bd := newTestBatchDispatcher(client)
	defer bd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	resp, err := bd.Send(ctx, newTestBatchRequest(1, 2, 3))
	assert.NoError(t, err)
	assert.Equal(t, 3, len(resp.Responses))
	sort.Slice(resp.Responses, func(i, j int) bool {
		return bytes.Compare(resp.Responses[i].Data, resp.Responses[j].Data) < 0
	})
	assert.Equal(t, format.Uint64ToBytes(1), resp.Responses[0].Data)
	assert.Equal(t, format.Uint64ToBytes(2), resp.Responses[1].Data)
	assert.Equal(t, format.Uint64ToBytes(3), resp.Responses[2].Data)
}

func TestDispatcherSendWithUpdateWriteTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "10/11,20/21,30/31")
	addTestShard(router, 2, "100/101,200/201,300/301")
	addTestShard(router, 3, "100/101,200/201,300/301")

	client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
		data := format.Uint64ToBytes(r.ToShard)
		return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
			Header: txnpb.TxnBatchResponseHeader{
				Txn: txnpb.TxnMeta{
					ID:             []byte("txn-1"),
					Name:           "txn-1",
					WriteTimestamp: r.ToShard,
				},
			},
			Responses: []txnpb.TxnResponse{{Data: data}},
		}}}}, nil
	})
	defer client.Stop()

	bd := newTestBatchDispatcher(client)
	defer bd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	resp, err := bd.Send(ctx, newTestBatchRequest(1, 2))
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), resp.Header.Txn.WriteTimestamp)
}

func TestUpdateTxnErrorsWithAbortedError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "10/11,20/21,30/31")

	client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
		return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
			Header: txnpb.TxnBatchResponseHeader{
				Error: &txnpb.TxnError{
					AbortedError: &txnpb.AbortedError{},
				},
			},
			Responses: []txnpb.TxnResponse{{}},
		}}}}, nil
	})
	defer client.Stop()

	bd := newTestBatchDispatcher(client)
	defer bd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	resp, err := bd.Send(ctx, newTestBatchRequest(1))
	assert.NoError(t, err)
	assert.NotNil(t, resp.Header.Error.AbortedError)
	assert.Nil(t, resp.Header.Error.ConflictWithCommittedError)
	assert.Nil(t, resp.Header.Error.UncertaintyError)
}

func TestUpdateTxnErrorsWithMergeAbortedErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "10/11,20/21,30/31")
	addTestShard(router, 2, "100/101,200/201,300/301")

	client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
		return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
			Header: txnpb.TxnBatchResponseHeader{
				Error: &txnpb.TxnError{
					AbortedError: &txnpb.AbortedError{},
				},
			},
			Responses: []txnpb.TxnResponse{{}},
		}}}}, nil
	})
	defer client.Stop()

	bd := newTestBatchDispatcher(client)
	defer bd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	resp, err := bd.Send(ctx, newTestBatchRequest(1, 2))
	assert.NoError(t, err)
	assert.NotNil(t, resp.Header.Error.AbortedError)
	assert.Nil(t, resp.Header.Error.ConflictWithCommittedError)
	assert.Nil(t, resp.Header.Error.UncertaintyError)
}

func TestUpdateTxnErrorsWithMergeAbortedAndConflictWithCommittedErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	errors := [][]*txnpb.TxnError{
		{
			&txnpb.TxnError{
				AbortedError: &txnpb.AbortedError{},
			},
			&txnpb.TxnError{
				ConflictWithCommittedError: &txnpb.ConflictWithCommittedError{MinTimestamp: 1},
			},
		},
		{
			&txnpb.TxnError{
				ConflictWithCommittedError: &txnpb.ConflictWithCommittedError{MinTimestamp: 1},
			},
			&txnpb.TxnError{
				AbortedError: &txnpb.AbortedError{},
			},
		},
	}

	for _, errs := range errors {
		func(errs []*txnpb.TxnError) {
			router := raftstore.NewMockRouter()
			addTestShard(router, 1, "10/11,20/21,30/31")
			addTestShard(router, 2, "100/101,200/201,300/301")

			var lock sync.Mutex
			idx := 0
			client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
				lock.Lock()
				defer lock.Unlock()

				err := errs[idx]
				idx++

				return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
					Header: txnpb.TxnBatchResponseHeader{
						Error: err,
					},
					Responses: []txnpb.TxnResponse{{}},
				}}}}, nil
			})
			defer client.Stop()

			bd := newTestBatchDispatcher(client)
			defer bd.Close()

			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			resp, err := bd.Send(ctx, newTestBatchRequest(1, 2))
			assert.NoError(t, err)
			assert.NotNil(t, resp.Header.Error.AbortedError)
			assert.Nil(t, resp.Header.Error.ConflictWithCommittedError)
			assert.Nil(t, resp.Header.Error.UncertaintyError)
		}(errs)
	}
}

func TestUpdateTxnErrorsWithMergeAbortedAndUncertaintyErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	errors := [][]*txnpb.TxnError{
		{
			&txnpb.TxnError{
				AbortedError: &txnpb.AbortedError{},
			},
			&txnpb.TxnError{
				UncertaintyError: &txnpb.UncertaintyError{MinTimestamp: 1},
			},
		},
		{
			&txnpb.TxnError{
				UncertaintyError: &txnpb.UncertaintyError{MinTimestamp: 1},
			},
			&txnpb.TxnError{
				AbortedError: &txnpb.AbortedError{},
			},
		},
	}

	for _, errs := range errors {
		func(errs []*txnpb.TxnError) {
			router := raftstore.NewMockRouter()
			addTestShard(router, 1, "10/11,20/21,30/31")
			addTestShard(router, 2, "100/101,200/201,300/301")

			var lock sync.Mutex
			idx := 0
			client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
				lock.Lock()
				defer lock.Unlock()

				err := errs[idx]
				idx++

				return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
					Header: txnpb.TxnBatchResponseHeader{
						Error: err,
					},
					Responses: []txnpb.TxnResponse{{}},
				}}}}, nil
			})
			defer client.Stop()

			bd := newTestBatchDispatcher(client)
			defer bd.Close()

			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			resp, err := bd.Send(ctx, newTestBatchRequest(1, 2))
			assert.NoError(t, err)
			assert.NotNil(t, resp.Header.Error.AbortedError)
			assert.Nil(t, resp.Header.Error.ConflictWithCommittedError)
			assert.Nil(t, resp.Header.Error.UncertaintyError)
		}(errs)
	}
}

func TestUpdateTxnErrorsWithMergeConflictWithCommittedErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	errors := [][]*txnpb.TxnError{
		{
			&txnpb.TxnError{
				ConflictWithCommittedError: &txnpb.ConflictWithCommittedError{MinTimestamp: 2},
			},
			&txnpb.TxnError{
				ConflictWithCommittedError: &txnpb.ConflictWithCommittedError{MinTimestamp: 1},
			},
		},
		{
			&txnpb.TxnError{
				ConflictWithCommittedError: &txnpb.ConflictWithCommittedError{MinTimestamp: 1},
			},
			&txnpb.TxnError{
				ConflictWithCommittedError: &txnpb.ConflictWithCommittedError{MinTimestamp: 2},
			},
		},
	}

	for _, errs := range errors {
		func(errs []*txnpb.TxnError) {
			router := raftstore.NewMockRouter()
			addTestShard(router, 1, "10/11,20/21,30/31")
			addTestShard(router, 2, "100/101,200/201,300/301")

			var lock sync.Mutex
			idx := 0
			client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
				lock.Lock()
				defer lock.Unlock()

				err := errs[idx]
				idx++

				return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
					Header: txnpb.TxnBatchResponseHeader{
						Error: err,
					},
					Responses: []txnpb.TxnResponse{{}},
				}}}}, nil
			})
			defer client.Stop()

			bd := newTestBatchDispatcher(client)
			defer bd.Close()

			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			resp, err := bd.Send(ctx, newTestBatchRequest(1, 2))
			assert.NoError(t, err)
			assert.NotNil(t, resp.Header.Error.ConflictWithCommittedError)
			assert.Equal(t, uint64(2), resp.Header.Error.ConflictWithCommittedError.MinTimestamp)
			assert.Nil(t, resp.Header.Error.AbortedError)
			assert.Nil(t, resp.Header.Error.UncertaintyError)
		}(errs)
	}
}

func TestUpdateTxnErrorsWithMergeUncertaintyErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	errors := [][]*txnpb.TxnError{
		{
			&txnpb.TxnError{
				UncertaintyError: &txnpb.UncertaintyError{MinTimestamp: 2},
			},
			&txnpb.TxnError{
				UncertaintyError: &txnpb.UncertaintyError{MinTimestamp: 1},
			},
		},
		{
			&txnpb.TxnError{
				UncertaintyError: &txnpb.UncertaintyError{MinTimestamp: 1},
			},
			&txnpb.TxnError{
				UncertaintyError: &txnpb.UncertaintyError{MinTimestamp: 2},
			},
		},
	}

	for _, errs := range errors {
		func(errs []*txnpb.TxnError) {
			router := raftstore.NewMockRouter()
			addTestShard(router, 1, "10/11,20/21,30/31")
			addTestShard(router, 2, "100/101,200/201,300/301")

			var lock sync.Mutex
			idx := 0
			client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
				lock.Lock()
				defer lock.Unlock()

				err := errs[idx]
				idx++

				return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
					Header: txnpb.TxnBatchResponseHeader{
						Error: err,
					},
					Responses: []txnpb.TxnResponse{{}},
				}}}}, nil
			})
			defer client.Stop()

			bd := newTestBatchDispatcher(client)
			defer bd.Close()

			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			resp, err := bd.Send(ctx, newTestBatchRequest(1, 2))
			assert.NoError(t, err)
			assert.NotNil(t, resp.Header.Error.UncertaintyError)
			assert.Equal(t, uint64(2), resp.Header.Error.UncertaintyError.MinTimestamp)
			assert.Nil(t, resp.Header.Error.AbortedError)
			assert.Nil(t, resp.Header.Error.ConflictWithCommittedError)
		}(errs)
	}
}

func TestUpdateTxnErrorsWithMergeConflictWithCommittedAndUncertaintyError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	errors := [][]*txnpb.TxnError{
		{
			&txnpb.TxnError{
				ConflictWithCommittedError: &txnpb.ConflictWithCommittedError{MinTimestamp: 2},
			},
			&txnpb.TxnError{
				UncertaintyError: &txnpb.UncertaintyError{MinTimestamp: 1},
			},
		},
		{
			&txnpb.TxnError{
				ConflictWithCommittedError: &txnpb.ConflictWithCommittedError{MinTimestamp: 1},
			},
			&txnpb.TxnError{
				UncertaintyError: &txnpb.UncertaintyError{MinTimestamp: 2},
			},
		},
		{
			&txnpb.TxnError{
				UncertaintyError: &txnpb.UncertaintyError{MinTimestamp: 1},
			},
			&txnpb.TxnError{
				ConflictWithCommittedError: &txnpb.ConflictWithCommittedError{MinTimestamp: 2},
			},
		},
		{
			&txnpb.TxnError{
				UncertaintyError: &txnpb.UncertaintyError{MinTimestamp: 2},
			},
			&txnpb.TxnError{
				ConflictWithCommittedError: &txnpb.ConflictWithCommittedError{MinTimestamp: 1},
			},
		},
	}

	for _, errs := range errors {
		func(errs []*txnpb.TxnError) {
			router := raftstore.NewMockRouter()
			addTestShard(router, 1, "10/11,20/21,30/31")
			addTestShard(router, 2, "100/101,200/201,300/301")

			var lock sync.Mutex
			idx := 0
			client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
				lock.Lock()
				defer lock.Unlock()

				err := errs[idx]
				idx++

				return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
					Header: txnpb.TxnBatchResponseHeader{
						Error: err,
					},
					Responses: []txnpb.TxnResponse{{}},
				}}}}, nil
			})
			defer client.Stop()

			bd := newTestBatchDispatcher(client)
			defer bd.Close()

			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			resp, err := bd.Send(ctx, newTestBatchRequest(1, 2))
			assert.NoError(t, err)
			assert.NotNil(t, resp.Header.Error.UncertaintyError)
			assert.Equal(t, uint64(2), resp.Header.Error.UncertaintyError.MinTimestamp)
			assert.Nil(t, resp.Header.Error.AbortedError)
			assert.Nil(t, resp.Header.Error.ConflictWithCommittedError)
		}(errs)
	}
}

func TestDispatcherSendOnlyCommitWithNoSwitchWaitConsensus(t *testing.T) {
	defer leaktest.AfterTest(t)()

	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "10/11,20/21,30/31")

	client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
		return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
			Responses: []txnpb.TxnResponse{{Data: []byte("ok")}},
		}}}}, nil
	})
	defer client.Stop()

	bd := newTestBatchDispatcher(client)
	defer bd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	resp, err := bd.Send(ctx, newTestBatchInternalRequest(1))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp.Responses))
	assert.Equal(t, "ok", string(resp.Responses[0].Data))
}

func TestDispatcherSendCommitAndNonWaitConsensusWithNoSwitchWaitConsensus(t *testing.T) {
	defer leaktest.AfterTest(t)()

	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "10/11,20/21,30/31")

	client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
		return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
			Responses: []txnpb.TxnResponse{{Data: []byte("ok")}},
		}}}}, nil
	})
	defer client.Stop()

	bd := newTestBatchDispatcher(client)
	defer bd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	req := newTestBatchRequest(1)
	appendTestCommitRequest(&req, 1)
	resp, err := bd.Send(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(resp.Responses))
	assert.Equal(t, "ok", string(resp.Responses[0].Data))
}

func TestDispatcherSendCommitAndWithSwitchWaitConsensus(t *testing.T) {
	defer leaktest.AfterTest(t)()

	router := raftstore.NewMockRouter()
	addTestShard(router, 1, "10/11,20/21,30/31")

	client := newTestRaftstoreClient(router, func(r rpcpb.Request) (rpcpb.ResponseBatch, error) {
		data := []byte("ok")
		if r.TxnBatchRequest.Requests[0].IsWaitConsensus() {
			data = []byte("wait")
		}
		return rpcpb.ResponseBatch{Responses: []rpcpb.Response{{ID: r.ID, TxnBatchResponse: &txnpb.TxnBatchResponse{
			Responses: []txnpb.TxnResponse{{Data: data}},
		}}}}, nil
	})
	defer client.Stop()

	bd := newTestBatchDispatcher(client)
	defer bd.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	req := newTestBatchRequest(1)
	appendTestInternalRequest(&req, 1, txnpb.InternalTxnOp_WaitConsensus)
	appendTestCommitRequest(&req, 1)
	resp, err := bd.Send(ctx, req)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(resp.Responses))
	assert.Equal(t, "wait", string(resp.Responses[0].Data))
	assert.Equal(t, "ok", string(resp.Responses[1].Data))
}

func newTestBatchRequest(ids ...uint64) txnpb.TxnBatchRequest {
	keys := make([][]byte, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, format.Uint64ToBytes(id))
	}
	p := &payload{Keys: keys}
	var req txnpb.TxnBatchRequest
	req.Requests = append(req.Requests, txnpb.TxnRequest{
		Operation: txnpb.TxnOperation{
			Op:       uint32(txnpb.InternalTxnOp_Reserved) + 1,
			Payload:  p.marshal(),
			Impacted: txnpb.KeySet{PointKeys: keys},
		},
	})
	return req
}

func newTestBatchInternalRequest(id uint64) txnpb.TxnBatchRequest {
	keys := [][]byte{format.Uint64ToBytes(id)}
	p := &payload{Keys: keys}
	var req txnpb.TxnBatchRequest
	req.Header.Txn.TxnRecordRouteKey = keys[0]
	req.Requests = append(req.Requests, txnpb.TxnRequest{
		Operation: txnpb.TxnOperation{
			Op:       uint32(txnpb.InternalTxnOp_Commit),
			Payload:  p.marshal(),
			Impacted: txnpb.KeySet{PointKeys: keys},
		},
	})
	return req
}

func appendTestCommitRequest(req *txnpb.TxnBatchRequest, id uint64) {
	req.Header.Txn.TxnRecordRouteKey = format.Uint64ToBytes(id)
	req.Requests = append(req.Requests, txnpb.TxnRequest{
		Operation: txnpb.TxnOperation{
			Op:       uint32(txnpb.InternalTxnOp_Commit),
			Impacted: txnpb.KeySet{PointKeys: [][]byte{format.Uint64ToBytes(id)}},
		},
	})
}

func appendTestInternalRequest(req *txnpb.TxnBatchRequest, id uint64, op txnpb.InternalTxnOp) {
	req.Requests = append(req.Requests, txnpb.TxnRequest{
		Operation: txnpb.TxnOperation{
			Op:       uint32(op),
			Impacted: txnpb.KeySet{PointKeys: [][]byte{format.Uint64ToBytes(id)}},
		},
	})
}

func addTestShard(router raftstore.Router, shardID uint64, shardInfo string) {
	b := raftstore.NewTestDataBuilder()
	s := b.CreateShard(shardID, shardInfo)
	for _, r := range s.Replicas {
		router.UpdateStore(metapb.Store{ID: r.StoreID, ClientAddress: "test-cli"})
	}
	router.UpdateShard(s)
	router.UpdateLeader(s.ID, s.Replicas[0].ID)
}

func newTestBatchDispatcher(client raftstoreClient.Client) *batchDispatcher {
	batch := NewBatchDispatcher(client, rpcpb.SelectLeader,
		newTestTxnOperationRouter(client.Router()),
		newMockTxnClocker(0),
		log.GetPanicZapLoggerWithLevel(zap.DebugLevel))
	return batch.(*batchDispatcher)
}

func newTestRaftstoreClient(router raftstore.Router, handler func(rpcpb.Request) (rpcpb.ResponseBatch, error)) raftstoreClient.Client {
	sp, _ := raftstore.NewMockShardsProxy(router, handler)
	c := raftstoreClient.NewClientWithOptions(raftstoreClient.CreateWithShardsProxy(sp))
	c.Start()
	return c
}

func newMockRoute() raftstore.Router {
	return raftstore.NewMockRouter()
}

type testTxnOperationRouter struct {
	router raftstore.Router
}

func newTestTxnOperationRouter(router raftstore.Router) *testTxnOperationRouter {
	return &testTxnOperationRouter{
		router: router,
	}
}

func (tr *testTxnOperationRouter) Route(request txnpb.TxnOperation) ([]RouteInfo, error) {
	p := &payload{}
	p.unmarshal(request.Payload)
	var routes []RouteInfo
	tr.router.AscendRange(request.ShardGroup, p.firstKey(), keys.NextKey(p.lastKey()), rpcpb.SelectLeader, func(shard raftstore.Shard, replicaStore metapb.Store) bool {
		keys := p.getKeysInShard(shard)
		if len(keys) > 0 {
			sp := &payload{Keys: keys}
			routes = append(routes, RouteInfo{
				ShardID: shard.ID,
				Operation: txnpb.TxnOperation{
					Op:         request.Op,
					Payload:    sp.marshal(),
					Impacted:   txnpb.KeySet{PointKeys: keys},
					ShardGroup: shard.Group,
				},
			})
		}
		return true
	})
	return routes, nil
}

type payload struct {
	Keys [][]byte `json:"keys"`
	idx  int
}

func (p *payload) getKeysInShard(shard raftstore.Shard) [][]byte {
	if p.idx >= len(p.Keys) {
		return nil
	}

	var keys [][]byte
	for p.idx < len(p.Keys) {
		if !shard.ContainsKey(p.Keys[p.idx]) {
			break
		}
		keys = append(keys, p.Keys[p.idx])
		p.idx++
	}

	return keys
}

func (p *payload) firstKey() []byte {
	return p.Keys[0]
}

func (p *payload) lastKey() []byte {
	return p.Keys[len(p.Keys)-1]
}

func (p *payload) marshal() []byte {
	v, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	return v
}

func (p *payload) unmarshal(v []byte) {
	if err := json.Unmarshal(v, p); err != nil {
		panic(err)
	}
	sort.Slice(p.Keys, func(i, j int) bool {
		return bytes.Compare(p.Keys[i], p.Keys[j]) < 0
	})
}
