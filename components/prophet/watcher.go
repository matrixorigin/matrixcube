// Copyright 2020 MatrixOrigin.
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

package prophet

import (
	"context"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
)

// Watcher watcher
type Watcher interface {
	// GetNotify returns event notify channel
	GetNotify() chan rpcpb.EventNotify
	// Close close watcher
	Close()
}

type watcher struct {
	ctx    context.Context
	cancel context.CancelFunc
	flag   uint32
	client *asyncClient
	eventC chan rpcpb.EventNotify
	conn   goetty.IOSession
}

func newWatcher(flag uint32, client *asyncClient) Watcher {
	ctx, cancel := context.WithCancel(context.Background())
	w := &watcher{
		ctx:    ctx,
		cancel: cancel,
		flag:   flag,
		client: client,
		eventC: make(chan rpcpb.EventNotify, 128),
		conn:   createConn(),
	}

	go w.watchDog()
	return w
}

func (w *watcher) Close() {
	w.cancel()
}

func (w *watcher) GetNotify() chan rpcpb.EventNotify {
	return w.eventC
}

func (w *watcher) doClose() {
	close(w.eventC)
	w.conn.Close()
}

func (w *watcher) watchDog() {
	defer func() {
		if err := recover(); err != nil {
			util.GetLogger().Errorf("client watcher failed with %+v, restart later", err)
			go w.watchDog()
		}
	}()
	for {
		select {
		case <-w.ctx.Done():
			w.doClose()
			return
		case <-w.client.ctx.Done():
			w.doClose()
			return
		default:
			err := w.resetConn()
			if err == nil {
				w.startReadLoop()
			}

			util.GetLogger().Errorf("reset watcher conn failed with %+v, retry later",
				err)
			time.Sleep(time.Second)
		}
	}
}

func (w *watcher) resetConn() error {
	err := w.client.initLeaderConn(w.conn, w.client.opts.rpcTimeout, false)
	if err != nil {
		return err
	}
	util.GetLogger().Infof("watcher init leader connection %s succeed",
		w.conn.RemoteAddr())
	return w.conn.WriteAndFlush(&rpcpb.Request{
		Type: rpcpb.TypeCreateWatcherReq,
		CreateWatcher: rpcpb.CreateWatcherReq{
			Flag: w.flag,
		},
	})
}

func (w *watcher) startReadLoop() {
	expectSeq := uint64(0)

	for {
		data, err := w.conn.Read()
		if err != nil {
			util.GetLogger().Errorf("watcher read events failed with %+v",
				err)
			return
		}

		resp := data.(*rpcpb.Response)
		if resp.Error != "" {
			util.GetLogger().Errorf("watcher read events failed with %+v", resp.Error)
			return
		}

		if resp.Type != rpcpb.TypeEventNotify {
			return
		}

		// we lost some event notify, close the conection, and retry
		if expectSeq != resp.Event.Seq {
			util.GetLogger().Errorf("watch lost some event notify, expect seq %d, but %d, close and retry",
				expectSeq,
				resp.Event.Seq)
			return
		}

		util.GetLogger().Debugf("watcher read event %+v", resp.Event)
		expectSeq = resp.Event.Seq + 1
		w.eventC <- resp.Event
	}
}
