package client

import (
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/matrixorigin/matrixcube/pb/txnpb"
	"github.com/matrixorigin/matrixcube/util/uuid"
	"go.uber.org/zap"
)

// Option the option create txn client
type Option func(*txnClient)

// WithLogger set logger for txn client
func WithLogger(logger *zap.Logger) Option {
	return func(tc *txnClient) {
		tc.logger = logger.Named("txn")
	}
}

// WithTxnIDGenerator set TxnIDGenerator for txn client
func WithTxnIDGenerator(txnIDGenerator TxnIDGenerator) Option {
	return func(tc *txnClient) {
		tc.txnIDGenerator = txnIDGenerator
	}
}

// WithTxnPriorityGenerator set TxnIDGenerator for txn client
func WithTxnPriorityGenerator(txnPriorityGenerator TxnPriorityGenerator) Option {
	return func(tc *txnClient) {
		tc.txnPriorityGenerator = txnPriorityGenerator
	}
}

// WithTxnClocker set TxnClocker for txn client
func WithTxnClocker(txnClocker TxnClocker) Option {
	return func(tc *txnClient) {
		tc.txnClocker = txnClocker
	}
}

// WithTxnHeartbeatDuration set txn heartbeat duration
func WithTxnHeartbeatDuration(value time.Duration) Option {
	return func(tc *txnClient) {
		tc.txnHeartbeatDuration = value
	}
}

// TxnIDGenerator generate a unique transaction ID for the cluster
type TxnIDGenerator interface {
	// Generate returns a unique transaction ID
	Generate() []byte
}

// TxnOperationSplitter used to split TxnOperation, as the transaction framework does not know how the
// TxnOperation data is organized, the caller needs to split the data managed in a TxnOperation
// into multiple TxnOperations according to the Shard.
type TxnOperationSplitter interface {
	// Split according to the TxnOperation internal management of data split into multiple
	// TxnOperation, split each TxnOperation with a Shard correspondence. The transaction
	// framework will concurrently send the split TxnOperations to the corresponding Shard for
	// execution.
	Split(request txnpb.TxnOperation) (payloads []txnpb.TxnOperation, shards []uint64, err error)
}

// TxnPriorityGenerator transaction priority generator, when a conflict occurs, decide which
// transaction to Abort based on priority.
type TxnPriorityGenerator interface {
	// Generate generator the transaction priority, the higher the number, the higher the
	// priority.
	Generate() uint32
}

// TxnClocker transaction clock solution abstraction
type TxnClocker interface {
	// Now return current clock
	Now() (current uint64, maxSkew uint64)
	// Compare return 0 if ts1 == ts2, positive if ts1 > ts2, negative if ts1 < ts2
	Compare(ts1, ts2 uint64) int
	// Next returns the next timestamp of ts
	Next(ts uint64) uint64
}

var _ TxnIDGenerator = (*uuidTxnIDGenerator)(nil)

type uuidTxnIDGenerator struct {
}

func newUUIDTxnIDGenerator() TxnIDGenerator {
	return &uuidTxnIDGenerator{}
}

func (gen *uuidTxnIDGenerator) Generate() []byte {
	return uuid.NewV4().Bytes()
}

type txnPriorityGenerator struct {
}

func newTxnPriorityGenerator() TxnPriorityGenerator {
	return &txnPriorityGenerator{}
}

func (p *txnPriorityGenerator) Generate() uint32 {
	return uint32(rand.Int63n(math.MaxUint32))
}

type mockTxnClocker struct {
	sync.Mutex
	ts      uint64
	maxSkew uint64
}

func newMockTxnClocker(maxSkew uint64) TxnClocker {
	return &mockTxnClocker{maxSkew: maxSkew}
}

func (tc *mockTxnClocker) Now() (current uint64, maxSkew uint64) {
	tc.Lock()
	defer tc.Unlock()

	tc.ts++
	return tc.ts, tc.maxSkew
}

func (tc *mockTxnClocker) Compare(ts1, ts2 uint64) int {
	return int(ts1 - ts2)
}

func (tc *mockTxnClocker) Next(ts uint64) uint64 {
	return ts + 1
}
