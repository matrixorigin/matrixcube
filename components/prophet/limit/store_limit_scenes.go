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

package limit

// Scene defines the container limitation on difference
// scenes
// Idle/Low/Normal/High indicates the load of the cluster, it is defined
// in cluster.State. See the details there for how to calculate the
// load.
// The values here defines the container-limit for each load. For example:
// Idle = 60, means that change the container-limit to 60 when the cluster is
// idle.
type Scene struct {
	Idle   int
	Low    int
	Normal int
	High   int
}

// DefaultScene returns Scene object with default values
func DefaultScene(limitType Type) *Scene {
	defaultScene := &Scene{
		Idle:   100,
		Low:    50,
		Normal: 32,
		High:   12,
	}

	// change this if different type rate limit has different default scene
	switch limitType {
	case AddPeer:
		return defaultScene
	case RemovePeer:
		return defaultScene
	default:
		return nil
	}
}
