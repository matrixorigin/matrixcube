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
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet/pb/rpcpb"
	"go.uber.org/zap"
)

// Watcher watcher
type Watcher interface {
	// GetNotify returns event notify channel
	GetNotify() chan rpcpb.EventNotify
	// Close close watcher
	Close()
}

type watcher struct {
	logger *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
	flag   uint32
	client *asyncClient
	eventC chan rpcpb.EventNotify
	conn   goetty.IOSession
}

func newWatcher(flag uint32, client *asyncClient, logger *zap.Logger) Watcher {
	ctx, cancel := context.WithCancel(context.Background())
	w := &watcher{
		logger: log.Adjust(logger).Named("watcher"),
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
			w.logger.Error("fail to read, restart later",
				zap.Any("error", err))
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

			w.logger.Error("fail to reset connection, retry later",
				zap.Error(err))
			time.Sleep(time.Second)
		}
	}
}

func (w *watcher) resetConn() error {
	err := w.client.initLeaderConn(w.conn, w.client.opts.rpcTimeout, false)
	if err != nil {
		return err
	}
	w.logger.Info("connect to leader succeed",
		zap.String("leader-address", w.conn.RemoteAddr()))
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
			w.logger.Error("fail to read events",
				zap.Error(err))
			return
		}

		resp := data.(*rpcpb.Response)
		if resp.Error != "" {
			w.logger.Error("error response",
				zap.String("error", resp.Error))
			return
		}

		if resp.Type != rpcpb.TypeEventNotify {
			w.logger.Error("read invalid type response",
				zap.String("type", resp.Type.String()))
			return
		}

		// we lost some event notify, close the conection, and retry
		if expectSeq != resp.Event.Seq {
			w.logger.Error("lost some event notify, close and retry",
				zap.Uint64("expect", expectSeq),
				zap.Uint64("actual", resp.Event.Seq))
			return
		}

		w.logger.Debug("read event",
			zap.Uint64("seq", resp.Event.Seq),
			zap.Uint32("type", resp.Event.Type))
		expectSeq = resp.Event.Seq + 1
		w.eventC <- resp.Event
	}
}
