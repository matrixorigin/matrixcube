package typeutil

import (
	"strconv"
	"strings"
)

// StringSlice is more friendly to json encode/decode
type StringSlice []string

// MarshalJSON returns the size as a JSON string.
func (s StringSlice) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(strings.Join(s, ","))), nil
}

// UnmarshalJSON parses a JSON string into the byte size.
func (s *StringSlice) UnmarshalJSON(text []byte) error {
	data, err := strconv.Unquote(string(text))
	if err != nil {
		return err
	}
	if len(data) == 0 {
		*s = []string{}
		return nil
	}
	*s = strings.Split(data, ",")
	return nil
}
