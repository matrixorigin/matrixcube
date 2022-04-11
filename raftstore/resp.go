package raftstore

import (
	"github.com/matrixorigin/matrixcube/pb/errorpb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
)

// TODO: move all response method to here

func requestDone(req rpcpb.Request, cb func(rpcpb.ResponseBatch), data []byte) {
	r := getResponse(req)
	r.Value = data
	cb(rpcpb.ResponseBatch{Responses: []rpcpb.Response{r}})
}

func requestDoneWithReplicaRemoved(req rpcpb.Request, cb func(rpcpb.ResponseBatch), id uint64) {
	r := getResponse(req)
	cb(rpcpb.ResponseBatch{Responses: []rpcpb.Response{r}, Header: rpcpb.ResponseBatchHeader{Error: errorpb.Error{
		Message: errShardNotFound.Error(),
		ShardNotFound: &errorpb.ShardNotFound{
			ShardID: id,
		},
	}}})
}

func getResponse(req rpcpb.Request) rpcpb.Response {
	return rpcpb.Response{
		Type:       req.Type,
		CustomType: req.CustomType,
		ID:         req.ID,
		PID:        req.PID,
	}
}
