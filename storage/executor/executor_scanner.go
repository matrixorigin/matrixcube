package executor

import (
	"bytes"

	"github.com/matrixorigin/matrixcube/pb/meta"
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
	buffer     *buf.ByteBuf
	filterFunc func([]byte) bool
}

func (opts *scanOptions) adjust(shard meta.Shard) {
	if len(opts.startKey) == 0 ||
		bytes.Compare(opts.startKey, shard.Start) < 0 { // opts.startKey < shard.Start
		opts.startKey = shard.Start
	}
	if len(opts.endKey) == 0 ||
		bytes.Compare(opts.endKey, shard.End) < 0 { // opts.endKey > shard.End
		opts.endKey = shard.End
	}

	if opts.filterFunc == nil {
		opts.filterFunc = emptyFilterFunc
	}
}

// WithScanBuffer 设置scan的buffer，避免内存开销
func WithScanBuffer(buffer *buf.ByteBuf) ScanOption {
	return func(opts *scanOptions) {
		opts.buffer = buffer
	}
}

// WithScanEndKey 设置scan的endKey
func WithScanEndKey(endKey []byte) ScanOption {
	return func(opts *scanOptions) {
		opts.endKey = endKey
	}
}

// WithScanStartKey 设置scan的startKey
func WithScanStartKey(startKey []byte) ScanOption {
	return func(opts *scanOptions) {
		opts.startKey = startKey
	}
}

// WithScanCountLimit 最多scan多少条数据
func WithScanCountLimit(value uint64) ScanOption {
	return func(opts *scanOptions) {
		opts.countLimit = value
	}
}

// WithScanBytesLimit 最多scan多少字节的数据
func WithScanBytesLimit(value uint64) ScanOption {
	return func(opts *scanOptions) {
		opts.bytesLimit = value
	}
}

// WithScanFilterFunc scan过滤器
func WithScanFilterFunc(filterFunc func([]byte) bool) ScanOption {
	return func(opts *scanOptions) {
		opts.filterFunc = filterFunc
	}
}

var _ DataStorageScanner = (*kvBasedDataStorageScanner)(nil)

// DataStorageScanner
type DataStorageScanner interface {
	// Scan 在指定的shard里scan满足条件数据，限定条件可以使用options来指定。当一个key满足条件时，handler会被调用.
	// needScanNext为false是，表示数据已经完全scan结束，不需要继续尝试了。
	// 当needScanNext是true的时候，nextScanKey是下一次scan的起始key
	Scan(shard meta.Shard, handler func(key, value []byte), options ...ScanOption) (needScanNext bool, nextScanKey []byte, err error)
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

func (s *kvBasedDataStorageScanner) Scan(shard meta.Shard, handler func(key, value []byte), options ...ScanOption) (bool, []byte, error) {
	var opts scanOptions
	for _, opt := range options {
		opt(&opts)
	}
	opts.adjust(shard)

	start := kv.EncodeShardStart(opts.startKey, opts.buffer)
	end := kv.EncodeShardEnd(opts.endKey, opts.buffer)

	err := s.kv.Scan(start, end, func(key, value []byte) (bool, error) {
		if opts.filterFunc(kv.DecodeDataKey(key)) {

		}

		return false, nil
	}, false)

	if err != nil {
		return false, nil, err
	}
	return false, nil, nil
}
