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

package raftstore

type workReady struct {
	channels []chan struct{}
	w        uint64
}

func newWorkReady(g uint64, w uint64) *workReady {
	total := g * w
	wr := &workReady{
		w:        w,
		channels: make([]chan struct{}, total),
	}
	for i := uint64(0); i < total; i++ {
		wr.channels[i] = make(chan struct{}, 1)
	}
	return wr
}

func (wr *workReady) waitC(g uint64, w uint64) <-chan struct{} {
	idx := g*wr.w + w
	return wr.channels[idx]
}

func (wr *workReady) notify(g uint64, w uint64) {
	idx := g*wr.w + w
	select {
	case wr.channels[idx] <- struct{}{}:
	default:
	}
}
