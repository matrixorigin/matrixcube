package keyutil

import (
	"encoding/hex"
	"fmt"
)

// BuildKeyRangeKey build key for a keyRange
func BuildKeyRangeKey(startKey, endKey []byte) string {
	return fmt.Sprintf("%s-%s", hex.EncodeToString(startKey), hex.EncodeToString(endKey))
}
