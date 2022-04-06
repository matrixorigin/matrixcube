package executor

import (
	"testing"

	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/storage/kv"
	"github.com/matrixorigin/matrixcube/storage/kv/mem"
	"github.com/matrixorigin/matrixcube/vfs"
	"github.com/stretchr/testify/assert"
)

func TestScanner(t *testing.T) {
	fs := vfs.GetTestFS()
	defer vfs.ReportLeakedFD(fs, t)

	s := mem.NewStorage()
	defer s.Close()

	scanner := NewKVBasedDataStorageScanner(s)
	assert.NoError(t, s.Set(kv.EncodeDataKey([]byte("a"), nil), []byte("a"), false))
	assert.NoError(t, s.Set(kv.EncodeDataKey([]byte("b"), nil), []byte("b"), false))
	assert.NoError(t, s.Set(kv.EncodeDataKey([]byte("c"), nil), []byte("c"), false))
	assert.NoError(t, s.Set(kv.EncodeDataKey([]byte("d"), nil), []byte("d"), false))

	cases := []struct {
		shard           metapb.Shard
		options         []ScanOption
		expectCompleted bool
		expectKeyPolicy ScanStartKeyPolicy
		expectKeys      [][]byte
	}{
		{
			shard:           metapb.Shard{},
			options:         []ScanOption{WithScanStartKey(nil), WithScanEndKey(nil)},
			expectCompleted: true,
			expectKeyPolicy: None,
			expectKeys:      [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
		},
		{
			shard:           metapb.Shard{},
			options:         []ScanOption{WithScanStartKey(nil), WithScanEndKey(nil), WithScanFilterFunc(func(b []byte) bool { return string(b) == "c" })},
			expectCompleted: true,
			expectKeyPolicy: None,
			expectKeys:      [][]byte{[]byte("c")},
		},
		{
			shard:           metapb.Shard{},
			options:         []ScanOption{WithScanStartKey(nil), WithScanEndKey(nil), WithScanCountLimit(1)},
			expectCompleted: false,
			expectKeyPolicy: GenWithResultLastKey,
			expectKeys:      [][]byte{[]byte("a")},
		},
		{
			shard:           metapb.Shard{},
			options:         []ScanOption{WithScanStartKey(nil), WithScanEndKey(nil), WithScanBytesLimit(2)},
			expectCompleted: false,
			expectKeyPolicy: GenWithResultLastKey,
			expectKeys:      [][]byte{[]byte("a"), []byte("b")},
		},
		{
			shard:           metapb.Shard{},
			options:         []ScanOption{WithScanStartKey([]byte("a")), WithScanEndKey([]byte("c"))},
			expectCompleted: true,
			expectKeyPolicy: None,
			expectKeys:      [][]byte{[]byte("a"), []byte("b")},
		},
		{
			shard:           metapb.Shard{Start: []byte("a"), End: []byte("c")},
			options:         []ScanOption{WithScanStartKey(nil), WithScanEndKey(nil)},
			expectCompleted: false,
			expectKeyPolicy: UseShardEnd,
			expectKeys:      [][]byte{[]byte("a"), []byte("b")},
		},
		{
			shard:           metapb.Shard{Start: []byte("a"), End: []byte("c")},
			options:         []ScanOption{WithScanStartKey([]byte("c")), WithScanEndKey(nil)},
			expectCompleted: false,
			expectKeyPolicy: UseShardEnd,
		},
	}

	for i, c := range cases {
		var keys [][]byte
		completed, policy, err := scanner.Scan(c.shard, func(key, value []byte) error {
			keys = append(keys, copyBytes(key))
			return nil
		}, c.options...)
		assert.NoError(t, err, "index %d", i)
		assert.Equal(t, c.expectCompleted, completed, "index %d", i)
		assert.Equal(t, c.expectKeyPolicy, policy, "index %d", i)
		assert.Equal(t, c.expectKeys, keys, "index %d", i)
	}
}

func copyBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
