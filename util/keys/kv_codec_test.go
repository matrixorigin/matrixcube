// Copyright 2022 MatrixOrigin.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License

package keys

import (
	"testing"

	"github.com/matrixorigin/matrixcube/util/buf"
	"github.com/stretchr/testify/assert"
)

func TestEncodeDataKey(t *testing.T) {
	assert.Equal(t, []byte{dataPrefix, 1}, EncodeDataKey([]byte{1}, nil))
	assert.Equal(t, []byte{dataPrefix, 1}, EncodeDataKey([]byte{1}, buf.NewByteBuf(12)))
}

func TestDecodeDataKey(t *testing.T) {
	assert.Equal(t, []byte{1}, DecodeDataKey([]byte{dataPrefix, 1}))
	assert.Equal(t, []byte{1}, DecodeDataKey([]byte{dataPrefix, 1}))
}

func TestNextKey(t *testing.T) {
	assert.Equal(t, []byte("a\x00"), NextKey([]byte("a"), nil))
	assert.Equal(t, []byte("\x00"), NextKey(nil, nil))

	buffer := buf.NewByteBuf(32)
	defer buffer.Release()

	assert.Equal(t, []byte("a\x00"), NextKey([]byte("a"), buffer))
	assert.Equal(t, []byte("\x00"), NextKey(nil, buffer))
}
