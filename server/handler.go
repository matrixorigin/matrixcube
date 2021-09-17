// Copyright 2020 MatrixOrigin.
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

package server

import (
	"github.com/fagongzi/goetty/codec"
	"github.com/matrixorigin/matrixcube/pb/raftcmdpb"
)

// Handler is the request handler
type Handler interface {
	// BuildRequest build the request, fill the key, cmd, type,
	// and the custom type
	BuildRequest(*raftcmdpb.Request, interface{}) error
	// Codec returns the decoder and encoder to transfer request and response
	Codec() (codec.Encoder, codec.Decoder)
}
