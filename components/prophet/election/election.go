package election

import (
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/clientv3"
)

var (
	leaderTickInterval = 50 * time.Millisecond
	loopInterval       = 100 * time.Millisecond
)

// Elector a leader elector
type Elector interface {
	// CreateLeadship create a leadership
	CreateLeadship(purpose string, nodeName, nodeValue string, allowCampaign bool, becomeLeader, becomeFollower func(string) bool) *Leadership
	// Client etcd clientv3
	Client() *clientv3.Client
}

type elector struct {
	sync.RWMutex
	options electorOptions
	client  *clientv3.Client
	lessor  clientv3.Lease
}

// NewElector create a elector
func NewElector(client *clientv3.Client, options ...ElectorOption) (Elector, error) {
	e := &elector{
		client: client,
		lessor: clientv3.NewLease(client),
	}

	for _, opt := range options {
		opt(&e.options)
	}

	e.options.adjust()

	return e, nil
}

func (e *elector) CreateLeadship(purpose string, nodeName, nodeValue string, allowCampaign bool, becomeLeader, becomeFollower func(string) bool) *Leadership {
	return newLeadership(e, purpose, nodeName, nodeValue, allowCampaign, becomeLeader, becomeFollower)
}

func (e *elector) Client() *clientv3.Client {
	return e.client
}

func getPurposePath(prefix string, purpose string) string {
	return fmt.Sprintf("%s/%s", prefix, purpose)
}

func getPurposeExpectPath(prefix string, purpose string) string {
	return fmt.Sprintf("%s/expect-%s", prefix, purpose)
}
