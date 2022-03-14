package client

// TxnClient distributed transaction client
type TxnClient interface {
	// NewTxn create transaction operation handles to perform transaction operations.
	NewTxn(name string, isolation IsolationLevel) TxnOperator
}
