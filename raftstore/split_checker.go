// Copyright 2021 MatrixOrigin.
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

package raftstore

import (
	"context"
	"sync"

	"github.com/matrixorigin/matrixcube/components/log"
	"go.uber.org/zap"
)

type splitCheckFunc func(start, end []byte, size uint64) (currentSize uint64,
	currentKeys uint64, splitKeys [][]byte, err error)

type splitChecker struct {
	shardCapacityBytes uint64
	replicaGetter      replicaGetter
	checkFuncFactory   func(group uint64) splitCheckFunc
	ctx                context.Context
	cancel             context.CancelFunc
	shardsC            chan Shard

	mu struct {
		sync.Mutex
		running bool
	}
}

func newSplitChecker(maxWaitToCheck int,
	shardCapacityBytes uint64,
	replicaGetter replicaGetter,
	checkFuncFactory func(group uint64) splitCheckFunc) *splitChecker {
	ctx, cancel := context.WithCancel(context.Background())
	return &splitChecker{
		replicaGetter:      replicaGetter,
		checkFuncFactory:   checkFuncFactory,
		shardCapacityBytes: shardCapacityBytes,
		ctx:                ctx,
		cancel:             cancel,
		shardsC:            make(chan Shard, maxWaitToCheck),
	}
}

func (sc *splitChecker) start() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.mu.running {
		return
	}

	sc.mu.running = true
	go func() {
		for {
			select {
			case <-sc.ctx.Done():
				close(sc.shardsC)
				return
			case shard := <-sc.shardsC:
				sc.doChecker(shard)
			}
		}
	}()
}

func (sc *splitChecker) doChecker(shard Shard) bool {
	pr, ok := sc.replicaGetter.getReplica(shard.ID)
	if !ok {
		return false
	}

	pr.logger.Info("start split check job")

	epoch := shard.Epoch
	current := pr.getShard()
	if current.Epoch.Version != epoch.Version {
		pr.logger.Info("epoch changed, need re-check later",
			log.EpochField("current-epoch", current.Epoch),
			log.EpochField("check-epoch", epoch))
		return false
	}

	fn := sc.checkFuncFactory(shard.Group)
	size, keys, splitKeys, err := fn(shard.Start, shard.End,
		sc.shardCapacityBytes)
	if err != nil {
		pr.logger.Fatal("fail to scan split key",
			zap.Error(err))
	}

	pr.logger.Debug("split check result",
		zap.Uint64("size", size),
		zap.Uint64("capacity", sc.shardCapacityBytes),
		zap.Uint64("keys", keys),
		zap.ByteStrings("split-keys", splitKeys))

	act := action{
		actionType: splitAction,
		epoch:      current.Epoch,
	}
	act.splitCheckData.keys = keys
	act.splitCheckData.size = size
	act.splitCheckData.splitKeys = splitKeys

	// need to exec split request
	if len(splitKeys) > 0 {
		newIDs, err := pr.prophetClient.AskBatchSplit(NewResourceAdapterWithShard(current), uint32(len(splitKeys)))
		if err != nil {
			pr.logger.Fatal("fail to ask batch split",
				zap.Error(err))

		}
		act.splitCheckData.splitIDs = newIDs
	}

	pr.addAction(act)
	return true
}

func (sc *splitChecker) close() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if !sc.mu.running {
		return
	}

	sc.cancel()
	sc.mu.running = false
}

func (sc *splitChecker) add(shard Shard) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if !sc.mu.running {
		return
	}

	select {
	case sc.shardsC <- shard:
	default:
	}
}
