package txnpb

import (
	"bytes"
	"sort"

	"github.com/matrixorigin/matrixcube/util/keys"
)

// IsRead return true if is read batch request
func (m TxnBatchRequest) IsRead() bool {
	return m.Header.Type == TxnRequestType_Read
}

// IsWrite return true if is write batch request
func (m TxnBatchRequest) IsWrite() bool {
	return m.Header.Type == TxnRequestType_Write
}

// HasCommitOrRollback returns true if the last request is commit or rollback operation
func (m TxnBatchRequest) HasCommitOrRollback() bool {
	n := len(m.Requests)
	return n > 0 && m.Requests[n-1].IsCommitOrRollback()
}

// HasCommit returns true if the last request is commit
func (m TxnBatchRequest) HasCommit() bool {
	return m.Requests[len(m.Requests)-1].IsCommit()
}

// OnlyContainsSingleKey returns true if only contains single impacted key
func (m TxnBatchRequest) OnlyContainsSingleKey() bool {
	return len(m.Requests) == 1 &&
		len(m.Requests[0].Operation.Impacted.PointKeys) == 1 &&
		len(m.Requests[0].Operation.Impacted.Ranges) == 0
}

// GetMultiKeyRange returns the impacted key range
func (m TxnBatchRequest) GetMultiKeyRange() ([]byte, []byte) {
	var min, max []byte
	for idx := range m.Requests {
		m.Requests[idx].Operation.Impacted.Sort()
		v1, v2 := m.Requests[idx].Operation.Impacted.getKeyRange()
		if len(min) == 0 && len(max) == 0 {
			min, max = v1, v2
			continue
		}

		if bytes.Compare(min, v1) > 0 {
			min = v1
		}

		if bytes.Compare(max, v2) < 0 {
			max = v2
		}
	}
	return min, max
}

// HasWaitConsensus returns true if has WaitConsensus request in batch
func (m TxnBatchRequest) HasWaitConsensus() bool {
	for idx := range m.Requests {
		if m.Requests[idx].Operation.Op == uint32(InternalTxnOp_WaitConsensus) {
			return true
		}
	}
	return false
}

// IsInternal is internal request
func (m TxnRequest) IsInternal() bool {
	return m.Operation.Op < uint32(InternalTxnOp_Reserved)
}

// IsCommitOrRollback is a commit or rollback request
func (m TxnRequest) IsCommitOrRollback() bool {
	return m.IsCommit() || m.IsInternal()
}

// IsCommit is commit request
func (m TxnRequest) IsCommit() bool {
	return m.Operation.Op == uint32(InternalTxnOp_Commit)
}

// IsRollback is rollback request
func (m TxnRequest) IsRollback() bool {
	return m.Operation.Op == uint32(InternalTxnOp_Rollback)
}

// IsWaitConsensus is wait consensus request
func (m TxnRequest) IsWaitConsensus() bool {
	return m.Operation.Op < uint32(InternalTxnOp_Reserved)
}

// IsFinal is final status
func (x TxnStatus) IsFinal() bool {
	return x == TxnStatus_Aborted || x == TxnStatus_Committed
}

// Aborted return true if has an AbortedError
func (m TxnError) Aborted() bool {
	return m.AbortedError != nil
}

// IsEmpty returns true if is a empty TxnMeta
func (m TxnMeta) IsEmpty() bool {
	return len(m.ID) == 0 && len(m.Name) == 0
}

// IsEmpty returns true if KeySet no pointKeys or KeyRanges
func (m KeySet) IsEmpty() bool {
	return len(m.PointKeys) == 0 || len(m.Ranges) == 0
}

// HasPointKeys returns true if KeySet any pointKeys
func (m KeySet) HasPointKeys() bool {
	return len(m.PointKeys) > 0
}

// HasKeyRanges returns true if KeySet any key ranges
func (m KeySet) HasKeyRanges() bool {
	return len(m.Ranges) > 0
}

// HasPointKey returns true if the key in pointKeys
func (m KeySet) HasPointKey(key []byte) bool {
	for _, k := range m.PointKeys {
		if bytes.Equal(k, key) {
			return true
		}
	}
	return false
}

// AddPointKeys add point keys
func (m *KeySet) AddPointKeys(keys [][]byte) {
	m.PointKeys = append(m.PointKeys, keys...)
	m.Sorted = false
}

// ResetPointKeys reset point keys
func (m *KeySet) ResetPointKeys(keys [][]byte) {
	m.PointKeys = keys
	m.Sorted = false
}

// Sort sort point keys and all key ranges
func (m *KeySet) Sort() {
	if m.Sorted {
		return
	}
	sort.Slice(m.PointKeys, func(i, j int) bool {
		return bytes.Compare(m.PointKeys[i], m.PointKeys[j]) < 0
	})

	sort.Slice(m.Ranges, func(i, j int) bool {
		return bytes.Compare(m.Ranges[i].Start, m.Ranges[j].Start) < 0
	})
	m.Sorted = true
}

func (m KeySet) getKeyRange() ([]byte, []byte) {
	var min, max []byte
	if len(m.PointKeys) > 0 {
		min = m.PointKeys[0]
		max = keys.NextKey(m.PointKeys[len(m.PointKeys)-1])
	}

	if len(m.Ranges) > 0 {
		if len(min) == 0 || bytes.Compare(min, m.Ranges[0].Start) > 0 {
			min = m.Ranges[0].Start
		}

		if len(max) == 0 || bytes.Compare(max, m.Ranges[len(m.Ranges)-1].End) < 0 {
			max = m.Ranges[len(m.Ranges)-1].End
		}
	}
	return min, max
}
