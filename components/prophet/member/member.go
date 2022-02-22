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

package member

import (
	"sync/atomic"

	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/election"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

// Member is used for the election related logic.
type Member struct {
	isProphet                            bool
	etcd                                 *embed.Etcd
	client                               *clientv3.Client
	elector                              election.Elector
	leadership                           *election.Leadership
	leader                               atomic.Value   // stored as *metapb.Member
	member                               *metapb.Member // current prophet's info.
	memberValue                          string
	id                                   uint64 //etcd server id
	becomeLeaderFunc, becomeFollowerFunc func() error
	logger                               *zap.Logger
}

// NewMember create a new Member.
func NewMember(
	client *clientv3.Client,
	etcd *embed.Etcd,
	elector election.Elector,
	isProphet bool,
	becomeLeaderFunc, becomeFollowerFunc func() error,
	logger *zap.Logger,
) *Member {
	id := uint64(0)
	if etcd != nil {
		id = uint64(etcd.Server.ID())
	}

	return &Member{
		client:             client,
		elector:            elector,
		isProphet:          isProphet,
		becomeLeaderFunc:   becomeLeaderFunc,
		becomeFollowerFunc: becomeFollowerFunc,
		etcd:               etcd,
		id:                 id,
		logger:             log.Adjust(logger).Named("member"),
	}
}

// ID returns the unique etcd ID for this server in etcd cluster.
func (m *Member) ID() uint64 {
	return m.id
}

// MemberValue returns the member value.
func (m *Member) MemberValue() string {
	return m.memberValue
}

// Member returns the member.
func (m *Member) Member() *metapb.Member {
	return m.member
}

// Stop loop
func (m *Member) Stop() {
	if m.leadership != nil {
		m.leadership.Stop()
	}
}

func (m *Member) becomeFollower(newLeader string) bool {
	if newLeader == "" {
		m.leader.Store(&metapb.Member{})
		if err := m.becomeFollowerFunc(); err != nil {
			return false
		}
		return true
	}

	v := &metapb.Member{}
	err := v.Unmarshal([]byte(newLeader))
	if err != nil {
		return false
	}

	m.leader.Store(v)
	if err := m.becomeFollowerFunc(); err != nil {
		return false
	}
	return true
}

func (m *Member) becomeLeader(newLeader string) bool {
	m.leader.Store(m.member)
	if err := m.becomeLeaderFunc(); err != nil {
		return false
	}

	return true
}

// GetLeadership returns the leadership of the prophet member.
func (m *Member) GetLeadership() *election.Leadership {
	return m.leadership
}

// GetLeader returns current PD leader of PD cluster.
func (m *Member) GetLeader() *metapb.Member {
	leader := m.leader.Load()
	if leader == nil {
		return nil
	}
	member := leader.(*metapb.Member)
	if member.GetID() == 0 {
		return nil
	}
	return member
}

// ElectionLoop start leader election loop
func (m *Member) ElectionLoop() {
	m.leadership.ElectionLoop()
}

// MemberInfo initializes the member info.
func (m *Member) MemberInfo(nodeName, addr string) {
	member := &metapb.Member{
		ID:   m.id,
		Name: nodeName,
		Addr: addr,
	}

	data, err := member.Marshal()
	if err != nil {
		// can't fail, so panic here.
		m.logger.Fatal("fail to marshal prophet member info", zap.Error(err))
	}

	m.member = member
	m.memberValue = string(data)
	m.leadership = m.elector.CreateLeadship(
		"prophet-leader", nodeName, m.memberValue,
		m.isProphet, m.becomeLeader, m.becomeFollower,
	)
}

// IsLeader returns whether the server is prophet leader or not by checking its leadership's lease and leader info.
func (m *Member) IsLeader() bool {
	return m.leadership.Check() && m.GetLeader().GetName() == m.member.Name
}

// Client etcd client
func (m *Member) Client() *clientv3.Client {
	return m.client
}

// GetEtcdLeader returns the etcd leader ID.
func (m *Member) GetEtcdLeader() uint64 {
	return m.etcd.Server.Lead()
}
