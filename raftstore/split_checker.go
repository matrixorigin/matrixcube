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
	"bytes"
	"fmt"
	"sync"

	"github.com/lni/goutils/syncutil"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"go.uber.org/zap"
)

type splitCheckFunc func(shard Shard, size uint64) (currentApproximateSize uint64,
	currentApproximateKeys uint64, splitKeys [][]byte, ctx []byte, err error)

type splitChecker struct {
	shardCapacityBytes uint64
	replicaGetter      replicaGetter
	checkFuncFactory   func(group uint64) splitCheckFunc
	stopper            *syncutil.Stopper
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
	return &splitChecker{
		stopper:            syncutil.NewStopper(),
		replicaGetter:      replicaGetter,
		checkFuncFactory:   checkFuncFactory,
		shardCapacityBytes: shardCapacityBytes,
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
			case <-sc.stopper.ShouldStop():
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
	if current.Epoch.Generation != epoch.Generation {
		pr.logger.Info("epoch changed, need re-check later",
			log.EpochField("current-epoch", current.Epoch),
			log.EpochField("check-epoch", epoch))
		return false
	}

	fn := sc.checkFuncFactory(shard.Group)
	size, keys, splitKeys, ctx, err := fn(shard, sc.shardCapacityBytes)
	if err != nil {
		pr.logger.Fatal("fail to scan split key",
			zap.Error(err))
	}

	pr.logger.Debug("split check result",
		log.ShardField("metadata", shard),
		zap.Uint64("size", size),
		zap.Uint64("capacity", sc.shardCapacityBytes),
		zap.Uint64("keys", keys),
		zap.ByteStrings("split-keys", splitKeys))

	if len(splitKeys) > 0 {
		for idx, key := range splitKeys {
			if checkKeyInShard(key, shard) != nil {
				pr.logger.Fatal("invalid split key",
					log.HexField("key", key),
					log.HexField("shard-start", shard.Start),
					log.HexField("shard-end", shard.End))
			}

			if idx > 0 && bytes.Compare(key, splitKeys[idx-1]) <= 0 {
				pr.logger.Fatal("invalid split key",
					log.HexField("key", key),
					log.HexField("prev-split-key", splitKeys[idx-1]))
			}
		}
	}

	act := action{
		actionType: splitAction,
		epoch:      current.Epoch,
	}
	act.splitCheckData.ctx = ctx
	act.splitCheckData.keys = keys
	act.splitCheckData.size = size
	act.splitCheckData.splitKeys = splitKeys

	// need to exec split request
	if len(splitKeys) > 0 {
		// Suppose we have a shard A with range [0,10), after checking, we need to split Shard A into 2 Shards B and C
		// in the range of [0, 5) and [5,10) at the point of 5.
		// Note. After the split is complete, Shard A will no longer be used
		newShardsCount := len(splitKeys) + 1
		newIDs, err := pr.prophetClient.AskBatchSplit(current, uint32(newShardsCount))
		if err != nil {
			pr.logger.Error("fail to ask batch split",
				zap.Error(err))
			return false
		}

		if len(newIDs) != newShardsCount {
			panic(fmt.Sprintf("expect %d new splitIDs, got %d", newShardsCount, len(newIDs)))
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

	sc.stopper.Stop()
	sc.mu.running = false
}

func (sc *splitChecker) add(shard Shard) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if !sc.mu.running {
		return
	}

	if shard.State == metapb.ShardState_Destroying ||
		shard.State == metapb.ShardState_Destroyed {
		return
	}

	select {
	case sc.shardsC <- shard:
	default:
	}
}
