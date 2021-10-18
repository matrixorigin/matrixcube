// Copyright 2021 MatrixOrigin.
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
// limitations under the License.

package buf

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewWrap(t *testing.T) {
	value := []byte{1, 2, 3}
	buf := WrapBytes(value)
	assert.Equal(t, 3, buf.Readable())

	v, err := buf.ReadByte()
	assert.NoError(t, err)
	assert.Equal(t, byte(1), v)

	v, err = buf.ReadByte()
	assert.NoError(t, err)
	assert.Equal(t, byte(2), v)

	v, err = buf.ReadByte()
	assert.NoError(t, err)
	assert.Equal(t, byte(3), v)
}

func TestWrap(t *testing.T) {
	buf := NewByteBuf(4)
	buf.Write([]byte{5, 6, 7})

	value := []byte{1, 2, 3}
	buf.Wrap(value)
	assert.Equal(t, 3, buf.Readable())

	v, err := buf.ReadByte()
	assert.NoError(t, err)
	assert.Equal(t, byte(1), v)

	v, err = buf.ReadByte()
	assert.NoError(t, err)
	assert.Equal(t, byte(2), v)

	v, err = buf.ReadByte()
	assert.NoError(t, err)
	assert.Equal(t, byte(3), v)
}

func TestSlice(t *testing.T) {
	buf := NewByteBuf(32)
	buf.Write([]byte("hello"))
	s := buf.Slice(0, 5)
	assert.Equal(t, "hello", string(s.Data()))
}

func TestWrittenDataAfterMark(t *testing.T) {
	buf := NewByteBuf(32)
	buf.MarkWrite()
	buf.Write([]byte("hello"))
	s := buf.WrittenDataAfterMark()
	assert.Equal(t, "hello", string(s.Data()))
}

func TestExpansion(t *testing.T) {
	buf := NewByteBuf(256)
	data := make([]byte, 257, 257)
	buf.Write(data)
	assert.Equal(t, 512, cap(buf.buf))
}
