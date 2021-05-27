package movingaverage

import (
	"sync"

	"github.com/phf/go-queue/queue"
)

// SafeQueue is a concurrency safe queue
type SafeQueue struct {
	mu  sync.Mutex
	que *queue.Queue
}

// NewSafeQueue return a SafeQueue
func NewSafeQueue() *SafeQueue {
	sq := &SafeQueue{}
	sq.que = queue.New()
	return sq
}

// Init implement init
func (sq *SafeQueue) Init() {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	sq.que.Init()
}

// PushBack implement PushBack
func (sq *SafeQueue) PushBack(v interface{}) {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	sq.que.PushBack(v)
}

// PopFront implement PopFront
func (sq *SafeQueue) PopFront() interface{} {
	sq.mu.Lock()
	defer sq.mu.Unlock()
	return sq.que.PopFront()
}
