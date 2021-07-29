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

package mock

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixcube/components/prophet/option"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/embed"
)

var (
	mutex sync.Mutex
	ports = 10000
)

func nextTestPorts() int {
	mutex.Lock()
	defer mutex.Unlock()

	ports++
	return ports
}

// NewEtcdClient create a etcd client
func NewEtcdClient(t *testing.T, port int) *clientv3.Client {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(fmt.Sprintf("http://127.0.0.1:%d", port), ","),
		DialTimeout: option.DefaultTimeout,
	})
	if err != nil {
		assert.FailNowf(t, "create etcd client failed", "error: %+v", err)
	}

	return client
}

// StartTestSingleEtcd start a single etcd server
func StartTestSingleEtcd(t *testing.T) (chan interface{}, int) {
	port := nextTestPorts()
	peerPort := nextTestPorts()

	now := time.Now().UnixNano()

	cfg := embed.NewConfig()
	cfg.Name = "p1"
	cfg.Dir = fmt.Sprintf("%s/prophet/test-%d", os.TempDir(), now)
	cfg.WalDir = ""
	cfg.InitialCluster = fmt.Sprintf("p1=http://127.0.0.1:%d", peerPort)
	cfg.ClusterState = embed.ClusterStateFlagNew
	cfg.EnablePprof = false
	cfg.Debug = false
	cfg.LPUrls, _ = util.ParseUrls(fmt.Sprintf("http://127.0.0.1:%d", peerPort))
	cfg.APUrls = cfg.LPUrls
	cfg.LCUrls, _ = util.ParseUrls(fmt.Sprintf("http://127.0.0.1:%d", port))
	cfg.ACUrls = cfg.LCUrls

	etcd, err := embed.StartEtcd(cfg)
	if err != nil {
		assert.FailNowf(t, "start embed etcd failed", "error: %+v", err)
	}

	select {
	case <-etcd.Server.ReadyNotify():
		time.Sleep(time.Millisecond * 100)
		stopC := make(chan interface{})
		go func() {
			<-stopC
			etcd.Server.Stop()
			os.RemoveAll(cfg.Dir)
		}()

		return stopC, port
	case <-time.After(time.Minute * 5):
		assert.FailNowf(t, "start embed etcd failed", "error: timeout", err)
	}

	return nil, 0
}
