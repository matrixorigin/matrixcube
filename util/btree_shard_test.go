package util

import (
	"testing"

	"github.com/deepfabric/beehive/pb/bhmetapb"
)

func TestTree(t *testing.T) {
	tree := NewShardTree()

	tree.Update(bhmetapb.Shard{
		ID:    1,
		Start: []byte{0},
		End:   []byte{1},
	})

	tree.Update(bhmetapb.Shard{
		ID:    2,
		Start: []byte{2},
		End:   []byte{3},
	})

	tree.Update(bhmetapb.Shard{
		ID:    3,
		Start: []byte{4},
		End:   []byte{5},
	})

	if tree.tree.Len() != 3 {
		t.Errorf("tree failed, insert 3 elements, but only %d", tree.tree.Len())
	}

	expect := []byte{0, 2, 4}
	count := 0
	tree.Ascend(func(Shard *bhmetapb.Shard) bool {
		if expect[count] != Shard.Start[0] {
			t.Error("tree failed, asc order is error")
		}
		count++

		return true
	})

	Shard := tree.Search([]byte{2})
	if len(Shard.Start) == 0 || Shard.Start[0] != 2 {
		t.Error("tree failed, search failed")
	}

	c := tree.NextShard(nil)
	if c == nil || len(c.Start) == 0 || c.Start[0] != 0 {
		t.Error("tree failed, search next failed")
	}

	count = 0
	tree.AscendRange(nil, []byte{4}, func(Shard *bhmetapb.Shard) bool {
		count++
		return true
	})

	if count != 2 {
		t.Error("tree failed, asc range failed")
	}

	count = 0
	tree.AscendRange(nil, []byte{5}, func(Shard *bhmetapb.Shard) bool {
		count++
		return true
	})

	if count != 3 {
		t.Error("tree failed, asc range failed")
	}

	// it will replace with 0,1 Shard
	tree.Update(bhmetapb.Shard{
		ID:    10,
		Start: nil,
		End:   []byte{1},
	})
	Shard = tree.Search([]byte{0})
	if len(Shard.Start) != 0 && Shard.Start[0] == 0 {
		t.Error("tree failed, update overlaps failed")
	}

	tree.Remove(bhmetapb.Shard{
		ID:    2,
		Start: []byte{2},
		End:   []byte{3},
	})
	if tree.length() != 2 {
		t.Error("tree failed, Remove failed")
	}
}
