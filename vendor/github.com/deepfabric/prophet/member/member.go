package member

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/deepfabric/prophet/codec"
	"github.com/deepfabric/prophet/election"
	"github.com/deepfabric/prophet/pb/metapb"
	"github.com/deepfabric/prophet/util"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/goetty/buf"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

// Member is used for the election related logic.
type Member struct {
	candidate   bool
	etcd        *embed.Etcd
	client      *clientv3.Client
	elector     election.Elector
	leadership  *election.Leadership
	leader      atomic.Value   // stored as *metapb.Member
	member      *metapb.Member // current prophet's info.
	memberValue string
	id          uint64 //etcd server id

	becomeLeaderFunc, becomeFollowerFunc func() error
}

// NewMember create a new Member.
func NewMember(client *clientv3.Client, etcd *embed.Etcd, elector election.Elector, candidate bool, becomeLeaderFunc, becomeFollowerFunc func() error) *Member {
	id := uint64(0)
	if etcd != nil {
		id = uint64(etcd.Server.ID())
	}

	return &Member{
		client:             client,
		elector:            elector,
		candidate:          candidate,
		becomeLeaderFunc:   becomeLeaderFunc,
		becomeFollowerFunc: becomeFollowerFunc,
		etcd:               etcd,
		id:                 id,
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

func (m *Member) disableLeader(newLeader string) bool {
	if newLeader == "" {
		m.leader.Store(&metapb.Member{})
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

func (m *Member) enableLeader(newLeader string) bool {
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
func (m *Member) ElectionLoop(ctx context.Context) {
	m.leadership.ElectionLoop(ctx)
}

// MemberInfo initializes the member info.
func (m *Member) MemberInfo(name, addr string) {
	leader := &metapb.Member{
		ID:   m.id,
		Name: name,
		Addr: addr,
	}

	data, err := leader.Marshal()
	if err != nil {
		// can't fail, so panic here.
		util.GetLogger().Fatalf("marshal prophet leader failed with %+v", err)
	}
	m.member = leader
	m.memberValue = string(data)
	m.leadership = m.elector.CreateLeadship("prophet-leader", name, m.memberValue, m.candidate, m.enableLeader, m.disableLeader)
}

// IsLeader returns whether the server is prophet leader or not by checking its leadership's lease and leader info.
func (m *Member) IsLeader() bool {
	return m.leadership.Check() && m.GetLeader().Name == m.member.Name
}

// Client etcd client
func (m *Member) Client() *clientv3.Client {
	return m.client
}

// GetEtcdLeader returns the etcd leader ID.
func (m *Member) GetEtcdLeader() uint64 {
	return m.etcd.Server.Lead()
}

func (m *Member) getLeaderClient(addr string) goetty.IOSession {
	for {
		leader := m.GetLeader()
		if leader != nil {
			conn, err := m.createLeaderClient(leader.Addr)
			if err == nil {
				util.GetLogger().Infof("create leader connection to %+v", leader)
				return conn
			}

			util.GetLogger().Errorf("create leader connection failed, errors: %+v", err)
		}

		time.Sleep(time.Second)
	}
}

func (m *Member) createLeaderClient(leader string) (goetty.IOSession, error) {
	encoder, decoder := codec.NewClientCodec(10 * buf.MB)
	conn := goetty.NewIOSession(goetty.WithCodec(encoder, decoder),
		goetty.WithEnableAsyncWrite(16))
	_, err := conn.Connect(leader, time.Second*3)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
