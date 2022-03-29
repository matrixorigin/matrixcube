package txnpb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSortPointKeys(t *testing.T) {
	var m KeySet
	m.PointKeys = append(m.PointKeys, []byte("k1"))
	m.PointKeys = append(m.PointKeys, []byte("k3"))
	m.PointKeys = append(m.PointKeys, []byte("k2"))
	m.sort()
	assert.Equal(t, [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")}, m.PointKeys)
}

func TestSortRanges(t *testing.T) {
	var m KeySet
	m.Ranges = append(m.Ranges, KeyRange{Start: []byte("k1"), End: []byte("k2")})
	m.Ranges = append(m.Ranges, KeyRange{Start: []byte("k3"), End: []byte("k4")})
	m.Ranges = append(m.Ranges, KeyRange{Start: []byte("k2"), End: []byte("k3")})
	m.sort()
	assert.Equal(t, KeyRange{Start: []byte("k1"), End: []byte("k2")}, m.Ranges[0])
	assert.Equal(t, KeyRange{Start: []byte("k2"), End: []byte("k3")}, m.Ranges[1])
	assert.Equal(t, KeyRange{Start: []byte("k3"), End: []byte("k4")}, m.Ranges[2])
}

func TestGetMultiKeyRange(t *testing.T) {
	var req TxnBatchRequest
	req.Requests = append(req.Requests, TxnRequest{
		Operation: TxnOperation{
			Impacted: KeySet{
				PointKeys: [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")},
				Ranges: []KeyRange{
					{
						Start: []byte("k1"), End: []byte("k2"),
					},
					{
						Start: []byte("k3"), End: []byte("k4"),
					},
					{
						Start: []byte("k2"), End: []byte("k3"),
					},
				},
			},
		},
	})

	req.Requests = append(req.Requests, TxnRequest{
		Operation: TxnOperation{
			Impacted: KeySet{
				PointKeys: [][]byte{[]byte("k5"), []byte("k4"), []byte("k2")},
				Ranges: []KeyRange{
					{
						Start: []byte("k8"), End: []byte("k9"),
					},
					{
						Start: []byte("k5"), End: []byte("k6"),
					},
					{
						Start: []byte("k2"), End: []byte("k3"),
					},
				},
			},
		},
	})

	min, max := req.GetMultiKeyRange()
	assert.Equal(t, []byte("k1"), min)
	assert.Equal(t, []byte("k9"), max)
}
