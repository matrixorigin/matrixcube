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

package hbstream

import (
	"context"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockcluster"
	"github.com/matrixorigin/matrixcube/components/prophet/mock/mockhbstream"
	"github.com/matrixorigin/matrixcube/components/prophet/testutil"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/pb/rpcpb"
)

func TestActivity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cluster := mockcluster.NewCluster(config.NewTestOptions())
	cluster.AddResourceContainer(1, 1)
	cluster.AddResourceContainer(2, 0)
	cluster.AddLeaderResource(1, 1)
	resource := cluster.GetResource(1)
	msg := &rpcpb.ResourceHeartbeatRsp{
		ConfigChange: &rpcpb.ConfigChange{
			Replica:    metapb.Replica{ID: 2, ContainerID: 2},
			ChangeType: metapb.ConfigChangeType_AddLearnerNode,
		},
	}

	hbs := NewTestHeartbeatStreams(ctx, cluster.ID, cluster, true, nil)
	stream1, stream2 := mockhbstream.NewHeartbeatStream(), mockhbstream.NewHeartbeatStream()

	// Active stream is stream1.
	hbs.BindStream(1, stream1)
	testutil.WaitUntil(t, func(t *testing.T) bool {
		hbs.SendMsg(resource, proto.Clone(msg).(*rpcpb.ResourceHeartbeatRsp))
		return stream1.Recv() != nil && stream2.Recv() == nil
	})
	// Rebind to stream2.
	hbs.BindStream(1, stream2)
	testutil.WaitUntil(t, func(t *testing.T) bool {
		hbs.SendMsg(resource, proto.Clone(msg).(*rpcpb.ResourceHeartbeatRsp))
		return stream1.Recv() == nil && stream2.Recv() != nil
	})

	// Switch back to 1 again.
	hbs.BindStream(1, stream1)
	testutil.WaitUntil(t, func(t *testing.T) bool {
		hbs.SendMsg(resource, proto.Clone(msg).(*rpcpb.ResourceHeartbeatRsp))
		return stream1.Recv() != nil && stream2.Recv() == nil
	})
}
