package executor

import (
	"bytes"
	"math"

	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/storage"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/util/buf"
)

var emptyFilterFunc = func(v []byte) bool {
	return true
}

// ScanOption scan option func
type ScanOption func(*scanOptions)

type scanOptions struct {
	startKey   []byte
	endKey     []byte
	countLimit uint64
	bytesLimit uint64
	withValue  bool
	buffer     *buf.ByteBuf
	filterFunc func([]byte) bool
}

func (opts *scanOptions) adjust(shard metapb.Shard) {
	// opts.startKey < shard.Start, only scan the data in current shard
	if len(opts.startKey) == 0 ||
		bytes.Compare(opts.startKey, shard.Start) < 0 {
		opts.startKey = shard.Start
	}
	// opts.endKey > shard.End, only scan the data in current shard
	if len(opts.endKey) == 0 ||
		(len(shard.End) > 0 && bytes.Compare(opts.endKey, shard.End) > 0) {
		opts.endKey = shard.End
	}

	if opts.filterFunc == nil {
		opts.filterFunc = emptyFilterFunc
	}

	if opts.countLimit == 0 {
		opts.countLimit = math.MaxUint64
	}

	if opts.bytesLimit == 0 {
		opts.bytesLimit = math.MaxUint64
	}
}

// WithValue set whether the return result of scan contains value
func WithValue() ScanOption {
	return func(opts *scanOptions) {
		opts.withValue = true
	}
}

// WithScanBuffer set the buffer of scan to avoid memory overhead
func WithScanBuffer(buffer *buf.ByteBuf) ScanOption {
	return func(opts *scanOptions) {
		opts.buffer = buffer
	}
}

// WithScanEndKey set the endKey of scan
func WithScanEndKey(endKey []byte) ScanOption {
	return func(opts *scanOptions) {
		opts.endKey = endKey
	}
}

// WithScanStartKey set the startKey of scan
func WithScanStartKey(startKey []byte) ScanOption {
	return func(opts *scanOptions) {
		opts.startKey = startKey
	}
}

// WithScanCountLimit set the maximum number of data to be included in the result
func WithScanCountLimit(value uint64) ScanOption {
	return func(opts *scanOptions) {
		opts.countLimit = value
	}
}

// WithScanBytesLimit set the maximum number of bytes of data to be included in the result
func WithScanBytesLimit(value uint64) ScanOption {
	return func(opts *scanOptions) {
		opts.bytesLimit = value
	}
}

// WithScanFilterFunc set the key filter and return true to indicate that the record meets the condition
func WithScanFilterFunc(filterFunc func([]byte) bool) ScanOption {
	return func(opts *scanOptions) {
		opts.filterFunc = filterFunc
	}
}

var _ DataStorageScanner = (*kvBasedDataStorageScanner)(nil)

// ScanStartKeyPolicy calculation strategy for the startKey of next scan
type ScanStartKeyPolicy int

var (
	// None no need to next scan
	None = ScanStartKeyPolicy(0)
	// GenWithResultLastKey use `kv.NextKey(last result key)` as startKey for next scan
	GenWithResultLastKey = ScanStartKeyPolicy(1)
	// UseShardEnd use the endKey of the shard as startKey for next scan
	UseShardEnd = ScanStartKeyPolicy(2)
)

// DataStorageScanner
type DataStorageScanner interface {
	// Scan Scan the data in the specified shard to satisfy the conditions,
	// the qualifying conditions can be specified using options.
	//
	// When a key satisfies the condition, the handler will be called.
	// The key and value passed to the handler are not safe and will be reused
	// between multiple handler calls, and need to be copied if they need to be saved.
	//
	// completed to true means that the scan is completed in all shards, otherwise the
	// client needs nextKeyPolicy to create the startKey for the next scan.
	Scan(shard metapb.Shard, handler func(key, value []byte) error, options ...ScanOption) (completed bool, nextKeyPolicy ScanStartKeyPolicy, err error)
}

type kvBasedDataStorageScanner struct {
	kv storage.KVStorage
}

// NewKVBasedDataStorageScanner create a kv based DataStorageScanner
func NewKVBasedDataStorageScanner(kv storage.KVStorage) DataStorageScanner {
	return &kvBasedDataStorageScanner{
		kv: kv,
	}
}

func (s *kvBasedDataStorageScanner) Scan(shard metapb.Shard, handler func(key, value []byte) error, options ...ScanOption) (bool, ScanStartKeyPolicy, error) {
	var opts scanOptions
	for _, opt := range options {
		opt(&opts)
	}
	opts.adjust(shard)

	view := s.kv.GetView()
	defer view.Close()

	buffer := opts.buffer
	if buffer == nil {
		buffer = buf.NewByteBuf(32)
		defer buffer.Release()
	}

	start := kv.EncodeShardStart(opts.startKey, buffer)
	end := kv.EncodeShardEnd(opts.endKey, buffer)
	n := uint64(0)
	bytes := uint64(0)
	skipByLimit := false
	err := s.kv.ScanInView(view, start, end, func(key, value []byte) (bool, error) {
		originKey := kv.DecodeDataKey(key)
		if opts.filterFunc(originKey) {
			err := handler(originKey, value)
			if err != nil {
				return false, err
			}

			n++
			bytes += uint64(len(originKey))
			if opts.withValue {
				bytes += uint64(len(value))
			}
			if n >= opts.countLimit ||
				bytes >= opts.bytesLimit {
				skipByLimit = true
				return false, nil
			}
		}
		return true, nil
	}, false)

	if err != nil {
		return false, None, err
	}

	if n == 0 {
		// last shard scan completed
		if len(shard.End) == 0 {
			return true, None, nil
		}
		// current shard scan completed, using shard.end as next scan key
		return false, UseShardEnd, nil
	}

	// use kv.NextKey(keys[len(keys-1)]) as next scan startKey
	if skipByLimit {
		return false, GenWithResultLastKey, nil
	}

	// current shard is last, all data scan completed
	if len(shard.End) == 0 {
		return true, None, nil
	}

	// current shard scan completed, using shard.end as next scan key
	return false, UseShardEnd, nil
}
