package client

import "errors"

var (
	// ErrTxnEnding no operations can be performed during ending transaction
	ErrTxnEnding = errors.New("no operations can be performed during ending transaction")
	// ErrTxnCommitted no operation can be executed after the transaction is committed
	ErrTxnCommitted = errors.New("no operation can be executed after the transaction is committed")
	// ErrTxnAborted no operation can be executed after the transaction is aborted
	ErrTxnAborted = errors.New("no operation can be executed after the transaction is aborted")
	// ErrTxnConflict transaction operations encounter conflicts and the client needs to rollback
	// the transaction
	ErrTxnConflict = errors.New("transaction operations encounter conflicts")
	// ErrTxnUncertainty uncertain clock error encountered
	ErrTxnUncertainty = errors.New("uncertain clock error encountered")
)
