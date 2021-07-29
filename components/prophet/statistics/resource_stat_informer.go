// Copyright 2020 PingCAP, Inc.
// Modifications copyright (C) 2021 MatrixOrigin.
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

package statistics

import (
	"github.com/matrixorigin/matrixcube/components/prophet/core"
)

// ResourceStatInformer provides access to a shared informer of statistics.
type ResourceStatInformer interface {
	IsResourceHot(res *core.CachedResource) bool
	// ResourceWriteStats return the containerID -> write stat of peers on this container
	// The result only includes peers that are hot enough.
	ResourceWriteStats() map[uint64][]*HotPeerStat
	// ResourceReadStats return the containerID -> read stat of peers on this container
	// The result only includes peers that are hot enough.
	ResourceReadStats() map[uint64][]*HotPeerStat
	RandHotResourceFromContainer(container uint64, kind FlowKind) *core.CachedResource
}
