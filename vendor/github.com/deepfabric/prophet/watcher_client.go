package prophet

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty"
)

// Watcher watcher client
type Watcher struct {
	sync.RWMutex

	ops           uint64
	addrs         []string
	p             *defaultProphet
	conn          goetty.IOSession
	resetConnFunc func(int)

	eventC chan *EventNotify
	stopC  chan struct{}
}

// NewWatcher returns a watcher for watch
func NewWatcher(addrs ...string) *Watcher {
	w := &Watcher{
		addrs: addrs,
		stopC: make(chan struct{}, 1),
	}

	w.resetConnFunc = w.resetConn
	return w
}

// NewWatcherWithProphet returns a watcher for watch
func NewWatcherWithProphet(p Prophet) *Watcher {
	w := &Watcher{
		p:     p.(*defaultProphet),
		stopC: make(chan struct{}, 1),
	}

	w.resetConnFunc = w.resetConnWithProphet
	return w
}

// Watch watch event
func (w *Watcher) Watch(flag int) chan *EventNotify {
	w.Lock()
	defer w.Unlock()

	go w.watchDog(flag)
	w.eventC = make(chan *EventNotify)
	return w.eventC
}

// Stop stop watch
func (w *Watcher) Stop() {
	w.Lock()
	defer w.Unlock()

	w.stopC <- struct{}{}
	w.conn.Close()
	close(w.eventC)
}

// Reset reset and refresh
func (w *Watcher) Reset() {
	w.Lock()
	defer w.Unlock()

	w.conn.Close()
}

func (w *Watcher) resetConn(flag int) {
	for {
		addr := w.next()
		if addr == "" {
			continue
		}

		c := &codec{}
		conn := goetty.NewConnector(addr,
			goetty.WithClientDecoder(goetty.NewIntLengthFieldBasedDecoder(c)),
			goetty.WithClientEncoder(goetty.NewIntLengthFieldBasedEncoder(c)),
			goetty.WithClientConnectTimeout(time.Second*10))
		_, err := conn.Connect()
		if err != nil {
			time.Sleep(time.Millisecond * 200)
			continue
		}
		err = conn.WriteAndFlush(&InitWatcher{
			Flag: flag,
		})
		if err != nil {
			time.Sleep(time.Millisecond * 200)
			continue
		}

		w.conn = conn
		return
	}
}

func (w *Watcher) resetConnWithProphet(flag int) {
	for {
		conn := w.p.getLeaderClient()
		err := conn.WriteAndFlush(&InitWatcher{
			Flag: flag,
		})
		if err != nil {
			conn.Close()
			time.Sleep(time.Millisecond * 200)
			continue
		}

		w.conn = conn
		return
	}
}

func (w *Watcher) watchDog(flag int) {
	defer func() {
		if r := recover(); r != nil {
			go w.watchDog(flag)
		}
	}()

	for {
		select {
		case <-w.stopC:
			w.conn.Close()
			return
		default:
			w.resetConnFunc(flag)
			w.startReadLoop()
		}
	}
}

func (w *Watcher) startReadLoop() {
	expectSeq := uint64(0)

	for {
		msg, err := w.conn.Read()
		if err != nil {
			w.conn.Close()
			return
		}

		if evt, ok := msg.(*EventNotify); ok {
			// we lost some event notify, close the conection, and retry
			if expectSeq != evt.Seq {
				log.Warningf("watch lost some event notify, expect seq %d, but %d, close and retry",
					expectSeq,
					evt.Seq)
				w.conn.Close()
				return
			}

			expectSeq = evt.Seq + 1
			w.eventC <- evt
		} else {
			w.conn.Close()
			time.Sleep(time.Second * 5)
			return
		}
	}
}

func (w *Watcher) next() string {
	l := uint64(len(w.addrs))
	if 0 >= l {
		return ""
	}

	return w.addrs[int(atomic.AddUint64(&w.ops, 1)%l)]
}
