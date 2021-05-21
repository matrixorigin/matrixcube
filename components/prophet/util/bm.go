package util

import (
	"bytes"
	"runtime"

	roaring64 "github.com/pilosa/pilosa/roaring"
)

const (
	// UseSliceCount encode bitmap as Slice
	UseSliceCount = uint64(5)
)

// MustUnmarshalBM64 parse a bitmap
func MustUnmarshalBM64(data []byte) *roaring64.Bitmap {
	bm := roaring64.NewBitmap()
	MustUnmarshalBM64To(data, bm)
	return bm
}

// MustUnmarshalBM64To parse a bitmap
func MustUnmarshalBM64To(data []byte, bm *roaring64.Bitmap) {
	if len(data) == 0 {
		return
	}

	err := bm.UnmarshalBinary(data)
	if err != nil {
		buf := make([]byte, 4096)
		n := runtime.Stack(buf, true)
		GetLogger().Fatalf("BUG: parse bm %+v failed with %+v \n %s", data, err, string(buf[:n]))
	}
}

// MustMarshalBM64 must marshal BM
func MustMarshalBM64(bm *roaring64.Bitmap) []byte {
	buf := bytes.NewBuffer(nil)
	MustMarshalBM64To(bm, buf)
	return buf.Bytes()
}

// MustMarshalBM64To must marshal BM
func MustMarshalBM64To(bm *roaring64.Bitmap, buf *bytes.Buffer) {
	n, err := bm.WriteTo(buf)
	if err != nil {
		log.Fatalf("BUG: write bm failed with %+v", err)
	}

	if n != int64(len(buf.Bytes())) {
		GetLogger().Fatalf("BUG: write bm failed with %d != %d, %+v", n, len(buf.Bytes()), bm.Slice())
	}
}
