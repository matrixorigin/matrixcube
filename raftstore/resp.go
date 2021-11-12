package raftstore

import (
	"github.com/matrixorigin/matrixcube/pb/errorpb"
	"github.com/matrixorigin/matrixcube/pb/rpc"
)

// TODO: move all reponse method to here

func requestDone(req rpc.Request, cb func(rpc.ResponseBatch), data []byte) {
	r := getResponse(req)
	r.Value = data
	cb(rpc.ResponseBatch{Responses: []rpc.Response{r}})
}

func requestDoneWithReplicaRemoved(req rpc.Request, cb func(rpc.ResponseBatch), id uint64) {
	r := getResponse(req)
	// FIXME: delete Request field
	r.Request = &req
	cb(rpc.ResponseBatch{Responses: []rpc.Response{r}, Header: rpc.ResponseBatchHeader{Error: errorpb.Error{
		Message: errShardNotFound.Error(),
		ShardNotFound: &errorpb.ShardNotFound{
			ShardID: id,
		},
	}}})
}

func getResponse(req rpc.Request) rpc.Response {
	return rpc.Response{
		Type: req.Type,
		ID:   req.ID,
		PID:  req.PID,
		Key:  req.Key,
	}
}
