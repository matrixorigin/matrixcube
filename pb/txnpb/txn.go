package txnpb

import "bytes"

// IsEmpty returns true if KeySet no pointKeys or KeyRanges
func (m *KeySet) IsEmpty() bool {
	return len(m.PointKeys) == 0 || len(m.Ranges) == 0
}

// HasPointKeys returns true if KeySet any pointKeys
func (m *KeySet) HasPointKeys() bool {
	return len(m.PointKeys) > 0
}

// HasKeyRanges returns true if KeySet any key ranges
func (m *KeySet) HasKeyRanges() bool {
	return len(m.Ranges) > 0
}

// HasPointKey returns true if the key in pointKeys
func (m *KeySet) HasPointKey(key []byte) bool {
	for _, k := range m.PointKeys {
		if bytes.Equal(k, key) {
			return true
		}
	}
	return false
}

// HasCommitOrRollback returns true if the last request is commit or rollback operation
func (m *TxnBatchRequest) HasCommitOrRollback() bool {
	n := len(m.Requests)
	return n > 0 && (m.Requests[n-1].Operation.Op == uint32(InternalTxnOp_Commit) ||
		m.Requests[n-1].Operation.Op == uint32(InternalTxnOp_Rollback))
}

// HasCommit returns true if the last request is commit
func (m *TxnBatchRequest) HasCommit() bool {
	return m.Requests[len(m.Requests)-1].Operation.Op == uint32(InternalTxnOp_Commit)
}

// IsInternal is internal request
func (m *TxnRequest) IsInternal() bool {
	return m.Operation.Op < uint32(InternalTxnOp_Reserved)
}

// IsFinal is final status
func (x TxnStatus) IsFinal() bool {
	return x == TxnStatus_Aborted || x == TxnStatus_Committed
}
