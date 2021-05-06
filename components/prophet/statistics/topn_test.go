package statistics

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type item struct {
	id     uint64
	values []float64
}

func (it *item) ID() uint64 {
	return it.id
}

func (it *item) Less(k int, than TopNItem) bool {
	return it.values[k] < than.(*item).values[k]
}

func TestPut(t *testing.T) {
	const Total = 10000
	const K = 3
	const N = 50
	tn := NewTopN(K, N, 1*time.Hour)

	putPerm(t, tn, K, Total, func(x int) float64 {
		return float64(-x) + 1
	}, false /*insert*/)

	putPerm(t, tn, K, Total, func(x int) float64 {
		return float64(-x)
	}, true /*update*/)

	// check GetTopNMin
	for k := 0; k < K; k++ {
		assert.Equal(t, float64(1-N), tn.GetTopNMin(k).(*item).values[k])
	}

	{
		topns := make([]float64, N)
		// check GetAllTopN
		for _, it := range tn.GetAllTopN(0) {
			it := it.(*item)
			topns[it.id] = it.values[0]
		}
		// check update worked
		for i, v := range topns {
			assert.Equal(t, v, float64(-i))
		}
	}

	{
		all := make([]float64, Total)
		// check GetAll
		for _, it := range tn.GetAll() {
			it := it.(*item)
			all[it.id] = it.values[0]
		}
		// check update worked
		for i, v := range all {
			assert.Equal(t, v, float64(-i))
		}
	}

	{ // check all dimensions
		for k := 1; k < K; k++ {
			topn := make([]float64, 0, N)
			for _, it := range tn.GetAllTopN(k) {
				topn = append(topn, it.(*item).values[k])
			}
			sort.Sort(sort.Reverse(sort.Float64Slice(topn)))

			all := make([]float64, 0, Total)
			for _, it := range tn.GetAll() {
				all = append(all, it.(*item).values[k])
			}
			sort.Sort(sort.Reverse(sort.Float64Slice(all)))
			assert.True(t, reflect.DeepEqual(topn, all[:N]))
		}
	}

	// check Get
	for i := uint64(0); i < Total; i++ {
		it := tn.Get(i).(*item)
		assert.Equal(t, it.id, i)
		assert.Equal(t, it.values[0], -float64(i))
	}
}

func putPerm(t *testing.T, tn *TopN, dimNum, total int, f func(x int) float64, isUpdate bool) {
	{ // insert
		dims := make([][]int, dimNum)
		for k := 0; k < dimNum; k++ {
			dims[k] = rand.Perm(total)
		}
		for i := 0; i < total; i++ {
			item := &item{
				id:     uint64(dims[0][i]),
				values: make([]float64, dimNum),
			}
			for k := 0; k < dimNum; k++ {
				item.values[k] = f(dims[k][i])
			}
			assert.Equal(t, tn.Put(item), isUpdate)
		}
	}
}

func TestRemove(t *testing.T) {
	const Total = 10000
	const K = 3
	const N = 50
	tn := NewTopN(K, N, 1*time.Hour)

	putPerm(t, tn, K, Total, func(x int) float64 {
		return float64(-x)
	}, false /*insert*/)

	// check Remove
	for i := 0; i < Total; i++ {
		if i%3 != 0 {
			it := tn.Remove(uint64(i)).(*item)
			assert.Equal(t, it.id, uint64(i))
		}
	}

	// check Remove worked
	for i := 0; i < Total; i++ {
		if i%3 != 0 {
			assert.Nil(t, tn.Remove(uint64(i)))
		}
	}

	assert.Equal(t, tn.GetTopNMin(0).(*item).id, uint64(3*(N-1)))

	{
		topns := make([]float64, N)
		for _, it := range tn.GetAllTopN(0) {
			it := it.(*item)
			topns[it.id/3] = it.values[0]
			assert.Equal(t, it.id%3, uint64(0))
		}
		for i, v := range topns {
			assert.Equal(t, v, float64(-i*3))
		}
	}

	{
		all := make([]float64, Total/3+1)
		for _, it := range tn.GetAll() {
			it := it.(*item)
			all[it.id/3] = it.values[0]
			assert.Equal(t, it.id%3, uint64(0))
		}
		for i, v := range all {
			assert.Equal(t, v, float64(-i*3))
		}
	}

	{ // check all dimensions
		for k := 1; k < K; k++ {
			topn := make([]float64, 0, N)
			for _, it := range tn.GetAllTopN(k) {
				topn = append(topn, it.(*item).values[k])
			}
			sort.Sort(sort.Reverse(sort.Float64Slice(topn)))

			all := make([]float64, 0, Total/3+1)
			for _, it := range tn.GetAll() {
				all = append(all, it.(*item).values[k])
			}
			sort.Sort(sort.Reverse(sort.Float64Slice(all)))
			assert.True(t, reflect.DeepEqual(topn, all[:N]))
		}
	}

	for i := uint64(0); i < Total; i += 3 {
		it := tn.Get(i).(*item)
		assert.Equal(t, it.id, i)
		assert.Equal(t, it.values[0], -float64(i))
	}
}

func TestTTL(t *testing.T) {
	const Total = 1000
	const K = 3
	const N = 50
	tn := NewTopN(K, 50, 900*time.Millisecond)

	putPerm(t, tn, K, Total, func(x int) float64 {
		return float64(-x)
	}, false /*insert*/)

	time.Sleep(900 * time.Millisecond)
	{
		item := &item{id: 0, values: []float64{100}}
		for k := 1; k < K; k++ {
			item.values = append(item.values, rand.NormFloat64())
		}
		assert.True(t, tn.Put(item))
	}
	for i := 3; i < Total; i += 3 {
		item := &item{id: uint64(i), values: []float64{float64(-i) + 100}}
		for k := 1; k < K; k++ {
			item.values = append(item.values, rand.NormFloat64())
		}
		assert.False(t, tn.Put(item))
	}
	tn.RemoveExpired()

	assert.Equal(t, tn.Len(), Total/3+1)
	items := tn.GetAllTopN(0)
	v := make([]float64, N)
	for _, it := range items {
		it := it.(*item)
		assert.Equal(t, it.id%3, uint64(0))
		v[it.id/3] = it.values[0]
	}
	for i, x := range v {
		assert.Equal(t, x, float64(-i*3)+100)
	}

	{ // check all dimensions
		for k := 1; k < K; k++ {
			topn := make([]float64, 0, N)
			for _, it := range tn.GetAllTopN(k) {
				topn = append(topn, it.(*item).values[k])
			}
			sort.Sort(sort.Reverse(sort.Float64Slice(topn)))

			all := make([]float64, 0, Total/3+1)
			for _, it := range tn.GetAll() {
				all = append(all, it.(*item).values[k])
			}
			sort.Sort(sort.Reverse(sort.Float64Slice(all)))
			assert.True(t, reflect.DeepEqual(topn, all[:N]))
		}
	}
}
