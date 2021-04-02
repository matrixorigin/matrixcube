package storage

import (
	"sync"

	"github.com/google/btree"
)

type memoryKVItem struct {
	key, value string
}

func (s memoryKVItem) Less(than btree.Item) bool {
	return s.key < than.(memoryKVItem).key
}

// just for test
type memStorage struct {
	sync.RWMutex

	id           uint64
	bootstrapped bool
	tree         *btree.BTree
}

func newMemKV() KV {
	return &memStorage{
		tree: btree.New(2),
	}
}

func (s *memStorage) Batch(batch *Batch) error {
	s.Lock()
	defer s.Unlock()

	for i := range batch.SaveKeys {
		s.doSave(batch.SaveKeys[i], batch.SaveValues[i])
	}

	for _, k := range batch.RemoveKeys {
		s.doRemove(k)
	}
	return nil
}

func (s *memStorage) Save(key, value string) error {
	s.Lock()
	defer s.Unlock()

	return s.doSave(key, value)
}

func (s *memStorage) Load(key string) (string, error) {
	s.Lock()
	defer s.Unlock()

	return s.doLoad(key)
}

func (s *memStorage) Remove(key string) error {
	s.Lock()
	defer s.Unlock()

	return s.doRemove(key)
}

func (s *memStorage) LoadRange(key, endKey string, limit int64) ([]string, []string, error) {
	s.RLock()
	defer s.RUnlock()

	keys := make([]string, 0, limit)
	values := make([]string, 0, limit)
	s.tree.AscendRange(memoryKVItem{key, ""}, memoryKVItem{endKey, ""}, func(item btree.Item) bool {
		keys = append(keys, item.(memoryKVItem).key)
		values = append(values, item.(memoryKVItem).value)
		if limit > 0 {
			return int64(len(keys)) < limit
		}
		return true
	})
	return keys, values, nil
}

func (s *memStorage) CountRange(key, endKey string) (uint64, error) {
	s.RLock()
	defer s.RUnlock()

	cnt := uint64(0)
	s.tree.AscendRange(memoryKVItem{key, ""}, memoryKVItem{endKey, ""}, func(item btree.Item) bool {
		cnt++
		return true
	})

	return cnt, nil
}

func (s *memStorage) SaveIfNotExists(key string, value string, batch *Batch) (bool, string, error) {
	s.Lock()
	defer s.Unlock()

	old, err := s.doLoad(key)
	if err != nil {
		return false, "", err
	}

	if len(old) > 0 {
		return false, old, nil
	}

	err = s.doSave(key, value)
	if err != nil {
		return false, "", err
	}

	if batch != nil {
		for i := range batch.SaveKeys {
			s.doSave(batch.SaveKeys[i], batch.SaveValues[i])
		}

		for _, k := range batch.RemoveKeys {
			s.doRemove(k)
		}
	}

	return true, "", err
}

func (s *memStorage) RemoveIfValueMatched(key string, expect string) (bool, error) {
	s.Lock()
	defer s.Unlock()

	old, err := s.doLoad(key)
	if err != nil {
		return false, err
	}

	if old != expect {
		return false, nil
	}

	err = s.doRemove(key)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (s *memStorage) AllocID() (uint64, error) {
	s.Lock()
	defer s.Unlock()

	s.id++
	return s.id, nil
}

func (s *memStorage) doLoad(key string) (string, error) {
	item := s.tree.Get(memoryKVItem{key, ""})
	if item == nil {
		return "", nil
	}
	return item.(memoryKVItem).value, nil
}

func (s *memStorage) doSave(key, value string) error {
	s.tree.ReplaceOrInsert(memoryKVItem{key, value})
	return nil
}

func (s *memStorage) doRemove(key string) error {
	s.tree.Delete(memoryKVItem{key, ""})
	return nil
}
