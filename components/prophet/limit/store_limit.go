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

import (
	"time"

	"github.com/juju/ratelimit"
)

const (
	// SmallResourceThreshold is used to represent a resource which can be regarded as a small resource once the size is small than it.
	SmallResourceThreshold int64 = 20
	// Unlimited is used to control the container limit. Here uses a big enough number to represent unlimited.
	Unlimited = float64(100000000)
)

// ResourceInfluence represents the influence of a operator step, which is used by container limit.
var ResourceInfluence = map[Type]int64{
	AddPeer:    1000,
	RemovePeer: 1000,
}

// SmallResourceInfluence represents the influence of a operator step
// when the resource size is smaller than smallResourceThreshold, which is used by container limit.
var SmallResourceInfluence = map[Type]int64{
	AddPeer:    200,
	RemovePeer: 200,
}

// Type indicates the type of container limit
type Type int

const (
	// AddPeer indicates the type of container limit that limits the adding peer rate
	AddPeer Type = iota
	// RemovePeer indicates the type of container limit that limits the removing peer rate
	RemovePeer
)

// TypeNameValue indicates the name of container limit type and the enum value
var TypeNameValue = map[string]Type{
	"add-peer":    AddPeer,
	"remove-peer": RemovePeer,
}

// String returns the representation of the Type
func (t Type) String() string {
	for n, v := range TypeNameValue {
		if v == t {
			return n
		}
	}
	return ""
}

// ContainerLimit limits the operators of a container
type ContainerLimit struct {
	bucket            *ratelimit.Bucket
	resourceInfluence int64
	ratePerSec        float64
}

// NewContainerLimit returns a ContainerLimit object
func NewContainerLimit(ratePerSec float64, resourceInfluence int64) *ContainerLimit {
	capacity := resourceInfluence
	rate := ratePerSec
	// unlimited
	if rate >= Unlimited {
		capacity = int64(Unlimited)
	} else if ratePerSec > 1 {
		capacity = int64(ratePerSec * float64(resourceInfluence))
		ratePerSec *= float64(resourceInfluence)
	} else {
		ratePerSec *= float64(resourceInfluence)
	}
	return &ContainerLimit{
		bucket:            ratelimit.NewBucketWithRate(ratePerSec, capacity),
		resourceInfluence: resourceInfluence,
		ratePerSec:        rate,
	}
}

// Available returns the number of available tokens
func (l *ContainerLimit) Available() int64 {
	return l.bucket.Available()
}

// Rate returns the fill rate of the bucket, in tokens per second.
func (l *ContainerLimit) Rate() float64 {
	return l.ratePerSec
}

// Take takes count tokens from the bucket without blocking.
func (l *ContainerLimit) Take(count int64) time.Duration {
	return l.bucket.Take(count)
}
