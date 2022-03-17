package client

import (
	"context"

	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"go.uber.org/zap"
)

// TxnOperator txn operator
type TxnOperator interface {
	// Write write operation, operation type, data and context are stored in the TxnOperation.
	// Each TxnOperation can be split into multiple `TxnOperations` by the `Splitter` and
	// the framework will send these split `TxnOperations` to the corresponding Shard for
	// execution. The Write method will only return when all TxnOperations have been executed.
	Write(ctx context.Context, requests []txnpb.TxnOperation) error
	// WriteAndCommit similar to `Wirte`, but commit the transaction after completing the write.
	WriteAndCommit(ctx context.Context, requests []txnpb.TxnOperation) error
	// Read 事务读操作，一个读操作的Payload有可能被`Splitter`拆分成多个`Payload`发送到不同的Shard，
	// 返回的[][]byte结果与拆分后的`Payload`顺序一致。
	Read(ctx context.Context, requests []txnpb.TxnOperation) ([][]byte, error)
	// Rollback 回滚事务
	Rollback(ctx context.Context) error
	// Commit 提交事务
	Commit(ctx context.Context) error
}

var _ TxnOperator = (*txnOperator)(nil)

// NewTxnOperator create txn operator, a txn corresponds to a TxnOperator instance.
func NewTxnOperator(txnID []byte, name string, isolation txnpb.Isolation, logger *zap.Logger) TxnOperator {
	return nil
}

type txnOperator struct {
	txn txnpb.Txn
}
