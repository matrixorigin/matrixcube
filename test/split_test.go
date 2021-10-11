package test

// TODO(TODO): resume
// func TestSplit(t *testing.T) {
// 	defer leaktest.AfterTest(t)()
// 	c := NewSingleTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(node int, cfg *config.Config) {
// 		cfg.Replication.ShardCapacityBytes = typeutil.ByteSize(20)
// 		cfg.Replication.ShardSplitCheckBytes = typeutil.ByteSize(10)
// 	}))
// 	defer c.Stop()

// 	c.Start()
// 	c.WaitShardByCountPerNode(1, testWaitTimeout)

// 	c.Set(0, keys.EncodeDataKey(0, []byte("key1")), []byte("value11"))
// 	c.Set(0, keys.EncodeDataKey(0, []byte("key2")), []byte("value22"))
// 	c.Set(0, keys.EncodeDataKey(0, []byte("key3")), []byte("value33"))

// 	c.WaitShardByCountPerNode(3, testWaitTimeout)
// 	c.WaitShardSplitByCount(c.GetShardByIndex(0, 0).ID, 1, testWaitTimeout)
// 	c.CheckShardRange(0, nil, []byte("key2"))
// 	c.CheckShardRange(1, []byte("key2"), []byte("key3"))
// 	c.CheckShardRange(2, []byte("key3"), nil)
// }

// TODO(fagongzi): resume
// func TestCustomSplit(t *testing.T) {
// 	defer leaktest.AfterTest(t)()
// 	target := keys.EncodeDataKey(0, []byte("key2"))
// 	c := NewSingleTestClusterStore(t, WithAppendTestClusterAdjustConfigFunc(func(i int, cfg *config.Config) {
// 		cfg.Customize.CustomSplitCheckFuncFactory = func(group uint64) func(shard Shard) (uint64, uint64, [][]byte, error) {
// 			return func(shard Shard) (uint64, uint64, [][]byte, error) {
// 				store := cfg.Storage.DataStorageFactory(shard.Group).(storage.KVStorage)
// 				endGroup := shard.Group
// 				if len(shard.End) == 0 {
// 					endGroup++
// 				}
// 				size := uint64(0)
// 				totalKeys := uint64(0)
// 				hasTarget := false
// 				store.Scan(keys.EncodeDataKey(shard.Group, shard.Start), keys.EncodeDataKey(endGroup, shard.End), func(key, value []byte) (bool, error) {
// 					size += uint64(len(key) + len(value))
// 					totalKeys++
// 					if bytes.Equal(key, target) {
// 						hasTarget = true
// 					}
// 					return true, nil
// 				}, false)

// 				if len(shard.End) == 0 && len(shard.Start) == 0 && hasTarget {
// 					return size, totalKeys, [][]byte{target}, nil
// 				}

// 				return size, totalKeys, nil, nil
// 			}
// 		}
// 	}))
// 	defer c.Stop()

// 	c.Start()
// 	c.WaitShardByCountPerNode(1, testWaitTimeout)

// 	c.Set(0, keys.EncodeDataKey(0, []byte("key1")), []byte("value11"))
// 	c.Set(0, keys.EncodeDataKey(0, []byte("key2")), []byte("value22"))
// 	c.Set(0, keys.EncodeDataKey(0, []byte("key3")), []byte("value33"))

// 	c.WaitShardByCountPerNode(2, testWaitTimeout)
// 	c.WaitShardSplitByCount(c.GetShardByIndex(0, 0).ID, 1, testWaitTimeout)
// 	c.CheckShardRange(0, nil, []byte("key2"))
// 	c.CheckShardRange(1, []byte("key2"), nil)
// }
