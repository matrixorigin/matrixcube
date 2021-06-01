package keyutil

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/fagongzi/util/format"
)

// BuildKeyRangeKey build key for a keyRange
func BuildKeyRangeKey(group uint64, startKey, endKey []byte) string {
	return fmt.Sprintf("%d-%s-%s", group, hex.EncodeToString(startKey), hex.EncodeToString(endKey))
}

// GetGroupFromRangeKey return group from range key
func GetGroupFromRangeKey(key string) uint64 {
	return format.MustParseStrUInt64(strings.Split(key, "-")[0])
}
