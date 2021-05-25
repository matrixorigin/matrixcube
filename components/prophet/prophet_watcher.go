package prophet

import (
	"sync"
	"sync/atomic"

	"github.com/fagongzi/goetty"
	"github.com/matrixorigin/matrixcube/components/prophet/cluster"
	"github.com/matrixorigin/matrixcube/components/prophet/event"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
)

type watcherSession struct {
	seq     uint64
	flag    uint32
	session goetty.IOSession
}

func (wt *watcherSession) notify(evt rpcpb.EventNotify) error {
	if event.MatchEvent(evt.Type, wt.flag) {
		resp := &rpcpb.Response{}
		resp.Type = rpcpb.TypeEventNotify
		resp.Event = evt
		resp.Event.Seq = atomic.AddUint64(&wt.seq, 1)
		util.GetLogger().Debugf("write notify event %+v", resp)
		return wt.session.WriteAndFlush(resp)
	}
	return nil
}

type watcherNotifier struct {
	watchers sync.Map
	cluster  *cluster.RaftCluster
}

func newWatcherNotifier(cluster *cluster.RaftCluster) *watcherNotifier {
	return &watcherNotifier{
		cluster: cluster,
	}
}

func (wn *watcherNotifier) handleCreateWatcher(req *rpcpb.Request, resp *rpcpb.Response, session goetty.IOSession) error {
	util.GetLogger().Infof("new watcher %s added",
		session.RemoteAddr())

	if wn != nil {
		if event.MatchEvent(event.EventInit, req.CreateWatcher.Flag) {
			snap := event.Snapshot{
				Leaders: make(map[uint64]uint64),
			}
			for _, c := range wn.cluster.GetContainers() {
				snap.Containers = append(snap.Containers, c.Meta.Clone())
			}
			for _, res := range wn.cluster.GetResources() {
				snap.Resources = append(snap.Resources, res.Meta.Clone())
				leader := res.GetLeader()
				if leader != nil {
					snap.Leaders[res.Meta.ID()] = leader.ContainerID
				}
			}

			rsp, err := event.NewInitEvent(snap)
			if err != nil {
				return err
			}

			resp.Event.Type = event.EventInit
			resp.Event.InitEvent = rsp
		}

		wn.watchers.Store(session.ID(), &watcherSession{
			flag:    req.CreateWatcher.Flag,
			session: session,
		})
	}

	return nil
}

func (wn *watcherNotifier) clearWatcher(w *watcherSession) {
	wn.watchers.Delete(w.session.ID())
	util.GetLogger().Infof("watcher %s removed",
		w.session.RemoteAddr())
}

func (wn *watcherNotifier) start() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				util.GetLogger().Errorf("watcher notify failed with %+v, restart later", err)
				wn.start()
			}
		}()

		var closed []*watcherSession
		for {
			evt, ok := <-wn.cluster.ChangedEventNotifier()
			if !ok {
				util.GetLogger().Infof("watcher notifer exited")
				return
			}

			if len(closed) > 0 {
				closed = closed[:0]
			}

			wn.watchers.Range(func(key, value interface{}) bool {
				wt := value.(*watcherSession)
				err := wt.notify(evt)
				if err != nil {
					closed = append(closed, wt)
				}
				return true
			})

			for _, w := range closed {
				wn.clearWatcher(w)
			}
		}
	}()
}

func (wn *watcherNotifier) stop() {
	wn.watchers.Range(func(key, value interface{}) bool {
		wn.watchers.Delete(key)
		value.(*watcherSession).session.Close()
		return true
	})

	util.GetLogger().Infof("watcher notifier stopped")
}
