package pb

import (
	"github.com/fagongzi/util/protoc"
)

// Clone clone pb
func Clone(src, dst protoc.PB) {
	protoc.MustUnmarshal(dst, protoc.MustMarshal(src))
}
