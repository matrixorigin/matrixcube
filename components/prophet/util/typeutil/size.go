package typeutil

import (
	"strconv"

	"github.com/docker/go-units"
)

// ByteSize is a retype uint64 for TOML and JSON.
type ByteSize uint64

// MarshalJSON returns the size as a JSON string.
func (b ByteSize) MarshalJSON() ([]byte, error) {
	return []byte(`"` + units.BytesSize(float64(b)) + `"`), nil
}

// UnmarshalJSON parses a JSON string into the byte size.
func (b *ByteSize) UnmarshalJSON(text []byte) error {
	s, err := strconv.Unquote(string(text))
	if err != nil {
		return err
	}
	v, err := units.RAMInBytes(s)
	if err != nil {
		return err
	}
	*b = ByteSize(v)
	return nil
}

// UnmarshalText parses a Toml string into the byte size.
func (b *ByteSize) UnmarshalText(text []byte) error {
	v, err := units.RAMInBytes(string(text))
	if err != nil {
		return err
	}
	*b = ByteSize(v)
	return nil
}
