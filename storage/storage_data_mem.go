package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/deepfabric/beehive/util"
)

type memDataStorage struct {
	kv *util.KVTree
}

// NewMemDataStorage returns a mem data storage
func NewMemDataStorage() DataStorage {
	return &memDataStorage{
		kv: util.NewKVTree(),
	}
}

func (s *memDataStorage) RangeDelete(start, end []byte) error {
	s.kv.RangeDelete(start, end)
	return nil
}

func (s *memDataStorage) SplitCheck(start []byte, end []byte, size uint64) (uint64, []byte, error) {
	total := uint64(0)
	found := false
	var splitKey []byte
	s.kv.Scan(start, end, func(key, value []byte) (bool, error) {
		total += uint64(len(key) + len(value))
		if !found && total > size {
			found = true
			splitKey = key
		}
		return true, nil
	})

	return total, splitKey, nil
}

func (s *memDataStorage) CreateSnapshot(path string, start, end []byte) error {
	f, err := os.Create(filepath.Join(path, "db.data"))
	if err != nil {
		return err
	}
	defer f.Close()

	err = writeBytes(f, start)
	if err != nil {
		return err
	}

	err = writeBytes(f, end)
	if err != nil {
		return err
	}

	return s.kv.Scan(start, end, func(key, value []byte) (bool, error) {
		err := writeBytes(f, key)
		if err != nil {
			return false, err
		}

		err = writeBytes(f, value)
		if err != nil {
			return false, err
		}

		return true, nil
	})
}

func (s *memDataStorage) ApplySnapshot(path string) error {
	f, err := os.Open(filepath.Join(path, "db.data"))
	if err != nil {
		return err
	}
	defer f.Close()

	start, err := readBytes(f)
	if err != nil {
		return err
	}
	if len(start) == 0 {
		return fmt.Errorf("error format, missing start field")
	}

	end, err := readBytes(f)
	if err != nil {
		return err
	}
	if len(end) == 0 {
		return fmt.Errorf("error format, missing end field")
	}

	s.kv.RangeDelete(start, end)
	for {
		key, err := readBytes(f)
		if err != nil {
			return err
		}
		if len(key) == 0 {
			break
		}

		value, err := readBytes(f)
		if err != nil {
			return err
		}
		if len(value) == 0 {
			return fmt.Errorf("error format, missing value field")
		}

		s.kv.Put(key, value)
	}

	return nil
}

func writeBytes(f *os.File, data []byte) error {
	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, uint32(len(data)))
	_, err := f.Write(size)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err != nil {
		return err
	}

	return nil
}

func readBytes(f *os.File) ([]byte, error) {
	size := make([]byte, 4)
	n, err := f.Read(size)
	if n == 0 && err == io.EOF {
		return nil, nil
	}

	total := int(binary.BigEndian.Uint32(size))
	written := 0
	data := make([]byte, total, total)
	for {
		n, err = f.Read(data[written:])
		if err != nil && err != io.EOF {
			return nil, err
		}
		written += n
		if written == total {
			return data, nil
		}
	}
}
