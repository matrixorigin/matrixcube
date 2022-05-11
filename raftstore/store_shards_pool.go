package raftstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/log"
	"github.com/matrixorigin/matrixcube/components/prophet"
	pconfig "github.com/matrixorigin/matrixcube/components/prophet/config"
	"github.com/matrixorigin/matrixcube/components/prophet/storage"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/config"
	"github.com/matrixorigin/matrixcube/pb/metapb"
	"github.com/matrixorigin/matrixcube/util/buf"
	"github.com/matrixorigin/matrixcube/util/stop"
	"go.uber.org/zap"
)

var (
	errNoIdleShard   = errors.New("no idle shard")
	batchCreateCount = 4
)

// ShardsPool is a shards pool, it will always create shards until the number of available shards reaches the
// value specified by `capacity`, we called these `Idle Shards`.
//
// The pool will create a Job in the prophet. Once a node became the prophet leader,  shards pool job will start,
// and stop if the node became the follower, So the job can be executed on any node. It will use prophet client to
// create shard after the job starts.
type ShardsPool interface {
	// Alloc alloc a shard from shards pool, returns error if no idle shards left. The `purpose` is used to avoid
	// duplicate allocation.
	Alloc(group uint64, purpose []byte) (metapb.AllocatedShard, error)
}

func (s *store) CreateShardPool(pools ...metapb.ShardPoolJobMeta) (ShardsPool, error) {
	err := s.pd.GetClient().CreateJob(metapb.Job{Type: metapb.JobType_CreateShardPool, Content: protoc.MustMarshal(&metapb.ShardPoolJob{
		Pools: pools,
	})})
	if err != nil {
		return nil, err
	}

	return s.shardPool, nil
}

func (s *store) GetShardPool() ShardsPool {
	return s.shardPool
}

// dynamicShardsPool a dynamic shard pool
type dynamicShardsPool struct {
	cfg     *config.Config
	logger  *zap.Logger
	factory func(g uint64, start, end []byte, unique string, offsetInPool uint64) Shard
	job     metapb.Job
	pd      prophet.Client
	pdC     chan struct{}
	stopper *stop.Stopper

	mu struct {
		sync.RWMutex

		state   int
		pools   metapb.ShardsPool
		createC chan struct{}
	}
}

func newDynamicShardsPool(cfg *config.Config, logger *zap.Logger) *dynamicShardsPool {
	p := &dynamicShardsPool{pdC: make(chan struct{}), cfg: cfg, logger: log.Adjust(logger).Named("shard-pool")}
	p.factory = p.shardFactory
	p.stopper = stop.NewStopper("shards-pool", stop.WithLogger(p.logger))
	if cfg.Customize.CustomShardPoolShardFactory != nil {
		p.factory = cfg.Customize.CustomShardPoolShardFactory
	}

	cfg.Prophet.RegisterJobProcessor(metapb.JobType_CreateShardPool, p)
	return p
}

func (dsp *dynamicShardsPool) setProphetClient(pd prophet.Client) {
	dsp.pd = pd
	close(dsp.pdC)
}

func (dsp *dynamicShardsPool) waitProphetClientSetted() {
	<-dsp.pdC
}

func (dsp *dynamicShardsPool) Alloc(group uint64, purpose []byte) (metapb.AllocatedShard, error) {
	allocated := metapb.AllocatedShard{}
	retry := 0
	for {
		v, err := dsp.pd.ExecuteJob(metapb.Job{Type: metapb.JobType_CreateShardPool},
			protoc.MustMarshal(&metapb.ShardsPoolCmd{
				Type: metapb.ShardsPoolCmdType_AllocShard,
				Alloc: &metapb.ShardsPoolAllocCmd{
					Group:   group,
					Purpose: purpose,
				},
			}))
		if err == nil && len(v) > 0 {
			protoc.MustUnmarshal(&allocated, v)
			return allocated, nil
		}

		if retry > 0 {
			if err != nil {
				return allocated, err
			}

			// no idle shard left
			if len(v) == 0 {
				return allocated, errNoIdleShard
			}
		}

		retry++
		time.Sleep(time.Second)
	}
}

func (dsp *dynamicShardsPool) Start(job metapb.Job, store storage.JobStorage, aware pconfig.ShardsAware) {
	dsp.mu.Lock()
	defer dsp.mu.Unlock()

	if dsp.isStartedLocked() {
		return
	}

	// load or init the job data
	value, err := store.GetJobData(job.Type)
	if err != nil {
		return
	}
	if len(value) > 0 {
		dsp.mu.pools = metapb.ShardsPool{}
		protoc.MustUnmarshal(&dsp.mu.pools, value)
	} else {
		jobContent := &metapb.ShardPoolJob{}
		protoc.MustUnmarshal(jobContent, job.Content)

		dsp.mu.pools.Pools = make(map[uint64]*metapb.ShardPool)
		for _, p := range jobContent.Pools {
			dsp.mu.pools.Pools[p.Group] = &metapb.ShardPool{
				Capacity:    p.Capacity,
				RangePrefix: p.RangePrefix,
			}
		}
	}

	dsp.mu.state = 1
	dsp.job = job
	dsp.mu.createC = make(chan struct{}, 8)
	dsp.startLocked(dsp.mu.createC, store, aware)
}

func (dsp *dynamicShardsPool) Stop(job metapb.Job, store storage.JobStorage, aware pconfig.ShardsAware) {
	dsp.mu.Lock()
	if !dsp.isStartedLocked() {
		dsp.mu.Unlock()
		return
	}

	dsp.mu.state = 0
	dsp.mu.Unlock()

	dsp.stopper.Stop()
	dsp.stopper = stop.NewStopper("shards-pool", stop.WithLogger(dsp.logger))
}

func (dsp *dynamicShardsPool) Remove(job metapb.Job, store storage.JobStorage, aware pconfig.ShardsAware) {
	dsp.Stop(job, store, aware)
}

func (dsp *dynamicShardsPool) Execute(data []byte, store storage.JobStorage, aware pconfig.ShardsAware) ([]byte, error) {
	if len(data) <= 0 {
		return nil,
			util.WrappedError(util.ErrJobInvalidCommand, "data empty")
	}

	dsp.mu.Lock()
	defer dsp.mu.Unlock()

	if !dsp.isStartedLocked() {
		return nil, util.ErrJobProcessorStopped
	}

	cmd := &metapb.ShardsPoolCmd{}
	protoc.MustUnmarshal(cmd, data)
	switch cmd.Type {
	case metapb.ShardsPoolCmdType_AllocShard:
		return dsp.doAllocLocked(cmd.Alloc, store, aware)
	default:
		return nil, util.WrappedError(
			util.ErrJobInvalidCommand,
			fmt.Sprintf("not supported (type=%d)", cmd.Type),
		)
	}
}

func (dsp *dynamicShardsPool) doAllocLocked(cmd *metapb.ShardsPoolAllocCmd, store storage.JobStorage, aware pconfig.ShardsAware) ([]byte, error) {
	group := cmd.Group
	p, ok := dsp.mu.pools.Pools[group]
	if !ok {
		return nil, util.WrappedError(
			util.ErrJobInvalidCommand,
			fmt.Sprintf("missing shard pool for group: %d", group),
		)
	}

	// check whether the purpose has been allocated before
	if len(p.AllocatedShards) > 0 {
		for _, allocated := range p.AllocatedShards {
			if bytes.Equal(allocated.Purpose, cmd.Purpose) {
				dsp.triggerCreateLocked()
				return protoc.MustMarshal(allocated), nil
			}
		}
	}

	// no idle shard left, trigger create, and return nil, client need to retry later
	if p.Seq-p.AllocatedOffset == 0 {
		dsp.triggerCreateLocked()
		return nil, nil
	}

	old := dsp.cloneDataLocked()
	id := uint64(0)
	p.AllocatedOffset++
	unique := dsp.unique(group, p.AllocatedOffset)
	fn := func(res metapb.Shard) {
		shard := res
		if shard.Unique == unique {
			id = shard.ID
		}
	}
	aware.ForeachWaitingCreateShards(fn)
	if id == 0 {
		aware.ForeachShards(group, fn)
	}
	if id == 0 {
		// Anyway the prophet leader node has no corresponding data in memory,
		// Client retry alloc again.
		dsp.mu.pools = old
		return nil, nil
	}

	allocated := &metapb.AllocatedShard{
		ShardID:     id,
		AllocatedAt: p.AllocatedOffset,
		Purpose:     cmd.Purpose,
	}
	p.AllocatedShards = append(p.AllocatedShards, allocated)
	dsp.mu.pools.Pools[group] = p

	if !dsp.saveLocked(store) {
		dsp.mu.pools = old
		return nil, nil
	}

	dsp.triggerCreateLocked()
	return protoc.MustMarshal(allocated), nil
}

func (dsp *dynamicShardsPool) startLocked(c chan struct{}, store storage.JobStorage, aware pconfig.ShardsAware) {
	dsp.triggerCreateLocked()
	err := dsp.stopper.RunTask(context.Background(), func(ctx context.Context) {
		dsp.logger.Info("dynamic shards pool job started")
		defer func() {
			close(c)
		}()

		dsp.waitProphetClientSetted()

		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		checkTicker := time.NewTicker(time.Second)
		defer checkTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				dsp.logger.Info("dynamic shards pool job stopped")
				return
			case <-c:
				dsp.logger.Debug("dynamic shards pool job maybeCreate")
				dsp.maybeCreate(store)
				dsp.logger.Debug("dynamic shards pool job maybeCreate completed")
			case <-checkTicker.C:
				dsp.logger.Debug("dynamic shards pool check create")
				dsp.maybeCreate(store)
				dsp.logger.Debug("dynamic shards pool job maybeCreate completed")
			case <-ticker.C:
				dsp.logger.Info("dynamic shards pool job gcAllocating")
				dsp.gcAllocating(store, aware)
			}
		}
	})
	if err != nil {
		dsp.logger.Fatal("start shards pool failed",
			zap.Error(err))
	}
}

func (dsp *dynamicShardsPool) isStartedLocked() bool {
	return dsp.mu.state == 1
}

func (dsp *dynamicShardsPool) triggerCreateLocked() {
	select {
	case dsp.mu.createC <- struct{}{}:
	default:
	}
}

func (dsp *dynamicShardsPool) maybeCreate(store storage.JobStorage) {
	dsp.mu.Lock()
	if !dsp.isStartedLocked() {
		dsp.mu.Unlock()
		return
	}

	// we don't modify directly
	modified := dsp.cloneDataLocked()
	var creates []metapb.Shard
	for {
		changed := false
		for g, p := range modified.Pools {
			if p.Seq == 0 ||
				(int(p.Seq-p.AllocatedOffset) < int(p.Capacity) && len(creates) < batchCreateCount) {
				p.Seq++
				tmp := dsp.factory(g,
					addPrefix(p.RangePrefix, p.Seq),
					addPrefix(p.RangePrefix, p.Seq+1),
					dsp.unique(g, p.Seq),
					p.Seq)
				creates = append(creates, tmp)
				changed = true
			}
		}

		if !changed {
			break
		}
	}
	dsp.mu.Unlock()

	if len(creates) > 0 {
		if dsp.cfg.Test.ShardPoolCreateWaitC != nil {
			<-dsp.cfg.Test.ShardPoolCreateWaitC
		}
		err := dsp.pd.AsyncAddShards(creates...)
		if err != nil {
			dsp.logger.Error("fail to create shard",
				zap.Error(err))
			return
		}

		// only update seq, all operation to update seq are in the same goroutine.
		dsp.mu.Lock()
		defer dsp.mu.Unlock()

		backup := dsp.cloneDataLocked()
		for g, p := range dsp.mu.pools.Pools {
			p.Seq = modified.Pools[g].Seq
		}
		if !dsp.saveLocked(store) {
			dsp.mu.pools = backup
		}

		dsp.triggerCreateLocked()
	}
}

func (dsp *dynamicShardsPool) gcAllocating(store storage.JobStorage, aware pconfig.ShardsAware) {
	dsp.mu.Lock()
	defer dsp.mu.Unlock()

	if !dsp.isStartedLocked() {
		return
	}

	removed := make(map[uint64][]int)
	for g, p := range dsp.mu.pools.Pools {
		var gc []int
		for idx, allocated := range p.AllocatedShards {
			stats := aware.GetShard(allocated.ShardID).GetStat()
			if stats != nil && stats.WrittenKeys > 0 {
				gc = append(gc, idx)
			}
		}
		removed[g] = gc
	}

	changed := false
	for g, ids := range removed {
		p := dsp.mu.pools.Pools[g]

		if len(ids) > 0 {
			changed = true
			allocates := p.AllocatedShards
			newP := p
			newP.AllocatedShards = newP.AllocatedShards[:0]
			for idx, allocated := range allocates {
				ok := true
				for _, id := range ids {
					if id == idx {
						ok = false
					}
				}
				if ok {
					newP.AllocatedShards = append(newP.AllocatedShards, allocated)
				}
			}

			dsp.mu.pools.Pools[g] = newP
		}
	}

	if changed {
		dsp.saveLocked(store)
	}
}

func (dsp *dynamicShardsPool) saveLocked(store storage.JobStorage) bool {
	err := store.PutJobData(dsp.job.Type, protoc.MustMarshal(&dsp.mu.pools))
	if err != nil {
		dsp.logger.Error("fail to save shard pool metadata, retry later",
			zap.Error(err))
		return false
	}
	return true
}

func (dsp *dynamicShardsPool) cloneDataLocked() metapb.ShardsPool {
	old := metapb.ShardsPool{}
	protoc.MustUnmarshal(&old, protoc.MustMarshal(&dsp.mu.pools))
	return old
}

func (dsp *dynamicShardsPool) shardFactory(g uint64, start, end []byte, unique string, offsetInPool uint64) Shard {
	return Shard{
		Group:  g,
		Start:  start,
		End:    end,
		Unique: unique,
	}
}

func (dsp *dynamicShardsPool) unique(g, seq uint64) string {
	return fmt.Sprintf("%d-%d-%d", dsp.job.Type, g, seq)
}

func addPrefix(prefix []byte, v uint64) []byte {
	if len(prefix) == 0 {
		return format.Uint64ToBytes(v)
	}

	data := make([]byte, len(prefix)+8)
	copy(data, prefix)
	buf.Uint64ToBytesTo(v, data[len(prefix):])
	return data
}
