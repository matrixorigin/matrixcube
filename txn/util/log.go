package util

import (
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// LogTxnMeta log txn meta
func LogTxnMeta(logger *zap.Logger, level zapcore.Level, msg string, txn txnpb.TxnMeta) {
	if ce := logger.Check(level, msg); ce != nil {
		ce.Write(log.TxnIDField(txn.ID),
			zap.String("txn-name", txn.Name),
			zap.Uint32("txn-epoch", txn.Epoch),
			log.HexField("txn-record-route-key", txn.TxnRecordRouteKey),
			zap.Uint32("txn-priority", txn.Priority),
			zap.String("txn-read-ts", txn.ReadTimestamp.String()),
			zap.String("txn-write-ts", txn.WriteTimestamp.String()),
			zap.String("txn-max-ts", txn.MaxTimestamp.String()),
			zap.String("txn-isolation", txn.IsolationLevel.String()))
	}
}
