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

package storage

import (
	"encoding/json"
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/fagongzi/util/format"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/components/prophet/metadata"
	"github.com/matrixorigin/matrixcube/components/prophet/util"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

// JobStorage job  storage
type JobStorage interface {
	// PutJob puts the job metadata to the storage
	PutJob(metapb.Job) error
	// RemoveJob remove job from storage
	RemoveJob(jobType metapb.JobType) error
	// LoadJobs load all jobs
	LoadJobs(limit int64, do func(metapb.Job)) error

	// PutJobData put job data
	PutJobData(metapb.Job, []byte) error
	// GetJobData  returns job data
	GetJobData(metapb.Job) ([]byte, error)
	// RemoveJobData removes job data
	RemoveJobData(metapb.Job) error
}

// RuleStorage rule storage
type RuleStorage interface {
	// PutRule puts the meta to the storage
	PutRule(key string, rule interface{}) error
	// LoadRules load all rules
	LoadRules(limit int64, f func(k, v string) error) error
	// RemoveRule remove rule
	RemoveRule(key string) error

	// PutRuleGroup puts the rule group to the storage
	PutRuleGroup(groupID string, group interface{}) error
	// RemoveRuleGroup remove rule group
	RemoveRuleGroup(groupID string) error
	// LoadRuleGroups load all rule groups
	LoadRuleGroups(limit int64, f func(k, v string) error) error
}

// CustomDataStorage custom data storage
type CustomDataStorage interface {
	// PutCustomData puts the custom data to the storage
	PutCustomData(key []byte, data []byte) error
	// BatchPutCustomData batch puts the custom data to the storage
	BatchPutCustomData(keys [][]byte, data [][]byte) error
	// LoadCustomData load all custom data
	LoadCustomData(limit int64, f func(k, v []byte) error) error
	// RemoveCustomData remove custom data
	RemoveCustomData(key []byte) error
}

// ShardStorage resource storage
type ShardStorage interface {
	// PutShard puts the meta to the storage
	PutShard(meta *metadata.ShardWithRWLock) error
	// PutShards put resource in batch
	PutShards(resources ...*metadata.ShardWithRWLock) error
	// RemoveShard remove resource from storage
	RemoveShard(meta *metadata.ShardWithRWLock) error
	// GetShard returns the spec resource
	GetShard(id uint64) (*metadata.ShardWithRWLock, error)
	// LoadShards load all resources
	LoadShards(limit int64, do func(*metadata.ShardWithRWLock)) error

	// PutShardAndExtra puts the meta and the extra data to the storage
	PutShardAndExtra(meta *metadata.ShardWithRWLock, extra []byte) error
	// GetShardExtra returns the resource extra data
	PutShardExtra(id uint64, extra []byte) error
	// GetShardExtra returns the resource extra data
	GetShardExtra(id uint64) ([]byte, error)

	PutScheduleGroupRule(metapb.ScheduleGroupRule) error
	LoadScheduleGroupRules(limit int64, do func(metapb.ScheduleGroupRule)) error
}

// ConfigStorage  config storage
type ConfigStorage interface {
	// SaveConfig stores marshallable cfg to the configPath.
	SaveConfig(cfg interface{}) error
	// LoadConfig loads config from configPath then unmarshal it to cfg.
	LoadConfig(cfg interface{}) (bool, error)

	// SaveScheduleConfig saves the config of scheduler.
	SaveScheduleConfig(scheduleName string, data []byte) error
	// RemoveScheduleConfig removes the config of scheduler.
	RemoveScheduleConfig(scheduleName string) error
	// LoadScheduleConfig loads the config of scheduler.
	LoadScheduleConfig(scheduleName string) (string, error)
	// LoadAllScheduleConfig loads all schedulers' config.
	LoadAllScheduleConfig() ([]string, []string, error)
}

// StoreStorage container storage
type StoreStorage interface {
	// PutStore returns nil if container is add or update succ
	PutStore(meta *metadata.StoreWithRWLock) error
	// RemoveStore remove container from storage
	RemoveStore(meta *metadata.StoreWithRWLock) error
	// GetStore returns the spec container
	GetStore(id uint64) (*metadata.StoreWithRWLock, error)
	// LoadStores load all containers
	LoadStores(limit int64, do func(meta *metadata.StoreWithRWLock, leaderWeight float64, resourceWeight float64)) error
	//PutStoreWeight saves a container's leader and resource weight to storage.
	PutStoreWeight(id uint64, leaderWeight, resourceWeight float64) error
}

// ClusterStorage cluster storage
type ClusterStorage interface {
	// AlreadyBootstrapped returns the cluster was already bootstrapped
	AlreadyBootstrapped() (bool, error)
	// PutBootstrapped put cluster is bootstrapped
	PutBootstrapped(container *metadata.StoreWithRWLock, resources ...*metadata.ShardWithRWLock) (bool, error)
}

// Storage meta storage
type Storage interface {
	JobStorage
	CustomDataStorage
	RuleStorage
	ConfigStorage
	StoreStorage
	ShardStorage
	StoreStorage
	ClusterStorage

	// KV return KV storage
	KV() KV
}

type storage struct {
	kv                       KV
	rootPath                 string
	configPath               string
	resourcePath             string
	resourceExtraPath        string
	scheduleGroupRulePath    string
	containerPath            string
	rulePath                 string
	ruleGroupPath            string
	clusterPath              string
	customScheduleConfigPath string
	schedulePath             string
	jobPath                  string
	jobDataPath              string
	customDataPath           string
}

// NewTestStorage create test storage
func NewTestStorage() Storage {
	return NewStorage("/test", newMemKV())
}

// NewStorage returns a metadata storage
func NewStorage(rootPath string, kv KV) Storage {
	return &storage{
		kv:                       kv,
		rootPath:                 rootPath,
		configPath:               fmt.Sprintf("%s/config", rootPath),
		resourcePath:             fmt.Sprintf("%s/resources", rootPath),
		resourceExtraPath:        fmt.Sprintf("%s/resources-extra", rootPath),
		scheduleGroupRulePath:    fmt.Sprintf("%s/schdule-group-rules", rootPath),
		containerPath:            fmt.Sprintf("%s/containers", rootPath),
		rulePath:                 fmt.Sprintf("%s/rules", rootPath),
		ruleGroupPath:            fmt.Sprintf("%s/rule-groups", rootPath),
		clusterPath:              fmt.Sprintf("%s/cluster", rootPath),
		customScheduleConfigPath: fmt.Sprintf("%s/scheduler-config", rootPath),
		schedulePath:             fmt.Sprintf("%s/schedule", rootPath),
		jobPath:                  fmt.Sprintf("%s/jobs", rootPath),
		jobDataPath:              fmt.Sprintf("%s/job-data", rootPath),
		customDataPath:           fmt.Sprintf("%s/custom", rootPath),
	}
}

func (s *storage) KV() KV {
	return s.kv
}

// SaveConfig stores marshallable cfg to the configPath.
func (s *storage) SaveConfig(cfg interface{}) error {
	value, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return s.kv.Save(s.configPath, string(value))
}

// LoadConfig loads config from configPath then unmarshal it to cfg.
func (s *storage) LoadConfig(cfg interface{}) (bool, error) {
	value, err := s.kv.Load(s.configPath)
	if err != nil {
		return false, err
	}
	if value == "" {
		return false, nil
	}
	err = json.Unmarshal([]byte(value), cfg)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *storage) SaveScheduleConfig(scheduleName string, data []byte) error {
	configPath := path.Join(s.customScheduleConfigPath, scheduleName)
	return s.kv.Save(configPath, string(data))
}

func (s *storage) RemoveScheduleConfig(scheduleName string) error {
	configPath := path.Join(s.customScheduleConfigPath, scheduleName)
	return s.kv.Remove(configPath)
}

func (s *storage) LoadScheduleConfig(scheduleName string) (string, error) {
	configPath := path.Join(s.customScheduleConfigPath, scheduleName)
	return s.kv.Load(configPath)
}

func (s *storage) LoadAllScheduleConfig() ([]string, []string, error) {
	prefix := s.customScheduleConfigPath + "/"
	keys, values, err := s.kv.LoadRange(prefix, util.GetPrefixRangeEnd(prefix), 1000)
	for i, key := range keys {
		keys[i] = strings.TrimPrefix(key, prefix)
	}
	return keys, values, err
}

func (s *storage) PutRule(key string, rule interface{}) error {
	return s.SaveJSON(s.rulePath, key, rule)
}

func (s *storage) LoadRules(limit int64, f func(k, v string) error) error {
	return s.LoadRangeByPrefix(limit, s.rulePath+"/", f)
}

func (s *storage) RemoveRule(key string) error {
	return s.kv.Remove(path.Join(s.rulePath, key))
}

func (s *storage) PutRuleGroup(groupID string, group interface{}) error {
	return s.SaveJSON(s.ruleGroupPath, groupID, group)
}

func (s *storage) RemoveRuleGroup(groupID string) error {
	return s.kv.Remove(path.Join(s.ruleGroupPath, groupID))
}

func (s *storage) LoadRuleGroups(limit int64, f func(k, v string) error) error {
	return s.LoadRangeByPrefix(limit, s.ruleGroupPath+"/", f)
}

func (s *storage) LoadRangeByPrefix(limit int64, prefix string, f func(k, v string) error) error {
	nextKey := prefix
	endKey := util.GetPrefixRangeEnd(prefix)
	for {
		keys, values, err := s.kv.LoadRange(nextKey, endKey, limit)
		if err != nil {
			return err
		}

		for i := range keys {
			err := f(filepath.Base(keys[i]), values[i])
			if err != nil {
				return err
			}
		}
		if int64(len(keys)) < limit {
			return nil
		}
		nextKey = path.Join(s.rootPath, keys[len(keys)-1]+"\x00")
	}
}

func (s *storage) SaveJSON(prefix, key string, data interface{}) error {
	value, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return s.kv.Save(path.Join(prefix, key), string(value))
}

func (s *storage) PutShard(meta *metadata.ShardWithRWLock) error {
	key := s.getKey(meta.ID(), s.resourcePath)
	data, err := meta.Marshal()
	if err != nil {
		return err
	}

	return s.kv.Save(key, string(data))
}

func (s *storage) PutScheduleGroupRule(rule metapb.ScheduleGroupRule) error {
	return s.kv.Save(s.getKey(rule.ID, s.scheduleGroupRulePath), string(protoc.MustMarshal(&rule)))
}

func (s *storage) LoadScheduleGroupRules(limit int64, do func(metapb.ScheduleGroupRule)) error {
	return s.LoadRangeByPrefix(limit, s.scheduleGroupRulePath+"/", func(k, v string) error {
		var rule metapb.ScheduleGroupRule
		protoc.MustUnmarshal(&rule, []byte(v))
		do(rule)
		return nil
	})
}

func (s *storage) PutShardAndExtra(res *metadata.ShardWithRWLock, extra []byte) error {
	data, err := res.Marshal()
	if err != nil {
		return err
	}

	batch := &Batch{}
	batch.SaveKeys = append(batch.SaveKeys, s.getKey(res.ID(), s.resourcePath))
	batch.SaveValues = append(batch.SaveValues, string(data))
	batch.SaveKeys = append(batch.SaveKeys, s.getKey(res.ID(), s.resourceExtraPath))
	batch.SaveValues = append(batch.SaveValues, string(extra))
	return s.kv.Batch(batch)
}

func (s *storage) GetShardExtra(id uint64) ([]byte, error) {
	key := s.getKey(id, s.resourceExtraPath)
	data, err := s.kv.Load(key)
	if err != nil {
		return nil, err
	}
	return []byte(data), nil
}

func (s *storage) PutShardExtra(id uint64, extra []byte) error {
	return s.kv.Save(s.getKey(id, s.resourceExtraPath), string(extra))
}

func (s *storage) PutShards(resources ...*metadata.ShardWithRWLock) error {
	batch := &Batch{}
	for _, res := range resources {
		data, err := res.Marshal()
		if err != nil {
			return err
		}
		batch.SaveKeys = append(batch.SaveKeys, s.getKey(res.ID(), s.resourcePath))
		batch.SaveValues = append(batch.SaveValues, string(data))
	}
	return s.kv.Batch(batch)
}

func (s *storage) RemoveShard(meta *metadata.ShardWithRWLock) error {
	return s.kv.Remove(s.getKey(meta.ID(), s.resourcePath))
}

func (s *storage) GetShard(id uint64) (*metadata.ShardWithRWLock, error) {
	key := s.getKey(id, s.resourcePath)
	data, err := s.kv.Load(key)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	res := metadata.NewShardWithRWLock()
	err = res.Unmarshal([]byte(data))
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (s *storage) LoadShards(limit int64, do func(*metadata.ShardWithRWLock)) error {
	return s.LoadRangeByPrefix(limit, s.resourcePath+"/", func(k, v string) error {
		data := metadata.NewShardWithRWLock()
		err := data.Unmarshal([]byte(v))
		if err != nil {
			return err
		}
		do(data)
		return nil
	})
}

func (s *storage) PutStore(meta *metadata.StoreWithRWLock) error {
	key := s.getKey(meta.ID(), s.containerPath)
	data, err := meta.Marshal()
	if err != nil {
		return err
	}

	return s.kv.Save(key, string(data))
}

func (s *storage) RemoveStore(meta *metadata.StoreWithRWLock) error {
	return s.kv.Remove(s.getKey(meta.ID(), s.containerPath))
}

func (s *storage) GetStore(id uint64) (*metadata.StoreWithRWLock, error) {
	key := s.getKey(id, s.containerPath)
	data, err := s.kv.Load(key)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		return nil, nil
	}

	c := metadata.NewStoreWithRWLock()
	err = c.Unmarshal([]byte(data))
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (s *storage) LoadStores(limit int64, do func(*metadata.StoreWithRWLock, float64, float64)) error {
	return s.LoadRangeByPrefix(limit, s.containerPath+"/", func(k, v string) error {
		data := metadata.NewStoreWithRWLock()
		err := data.Unmarshal([]byte(v))
		if err != nil {
			return err
		}

		leaderWeight, err := s.loadFloatWithDefaultValue(s.containerWeightPath(data.ID(), "leader"), 1.0)
		if err != nil {
			return err
		}

		resourceWeight, err := s.loadFloatWithDefaultValue(s.containerWeightPath(data.ID(), "resource"), 1.0)
		if err != nil {
			return err
		}

		do(data, leaderWeight, resourceWeight)
		return nil
	})
}

func (s *storage) PutStoreWeight(id uint64, leaderWeight, resourceWeight float64) error {
	batch := &Batch{}
	batch.SaveKeys = append(batch.SaveKeys, s.containerWeightPath(id, "leader"))
	batch.SaveValues = append(batch.SaveValues, strconv.FormatFloat(leaderWeight, 'f', -1, 64))
	batch.SaveKeys = append(batch.SaveKeys, s.containerWeightPath(id, "resource"))
	batch.SaveValues = append(batch.SaveValues, strconv.FormatFloat(resourceWeight, 'f', -1, 64))

	return s.kv.Batch(batch)
}

func (s *storage) PutJob(job metapb.Job) error {
	return s.kv.Save(s.jobKey(job.Type),
		string(protoc.MustMarshal(&job)))
}

func (s *storage) RemoveJob(jobType metapb.JobType) error {
	b := &Batch{}
	b.RemoveKeys = append(b.RemoveKeys, s.jobKey(jobType))
	b.RemoveKeys = append(b.RemoveKeys, s.jobDataKey(jobType))
	return s.kv.Batch(b)
}

func (s *storage) LoadJobs(limit int64, fn func(metapb.Job)) error {
	return s.LoadRangeByPrefix(limit, s.jobPath+"/", func(k, v string) error {
		job := metapb.Job{}
		protoc.MustUnmarshal(&job, []byte(v))
		fn(job)
		return nil
	})
}

func (s *storage) PutJobData(job metapb.Job, data []byte) error {
	return s.kv.Save(s.jobDataKey(job.Type), string(data))
}

func (s *storage) GetJobData(job metapb.Job) ([]byte, error) {
	v, err := s.kv.Load(s.jobDataKey(job.Type))
	if err != nil {
		return nil, err
	}

	return []byte(v), nil
}

func (s *storage) RemoveJobData(job metapb.Job) error {
	return s.kv.Remove(s.jobDataKey(job.Type))
}

func (s *storage) PutCustomData(key []byte, data []byte) error {
	return s.kv.Save(path.Join(s.customDataPath, string(key)), string(data))
}

func (s *storage) BatchPutCustomData(keys [][]byte, data [][]byte) error {
	if len(keys) != len(data) {
		return fmt.Errorf("key length %d != data length %d",
			len(keys),
			len(data))
	}

	batch := &Batch{}
	for i := 0; i < len(keys); i++ {
		batch.SaveKeys = append(batch.SaveKeys, path.Join(s.customDataPath, string(keys[i])))
		batch.SaveValues = append(batch.SaveValues, string(data[i]))
	}
	return s.kv.Batch(batch)
}

func (s *storage) LoadCustomData(limit int64, do func(k, v []byte) error) error {
	return s.LoadRangeByPrefix(limit, s.customDataPath+"/", func(k, v string) error {
		do([]byte(k), []byte(v))
		return nil
	})
}

func (s *storage) RemoveCustomData(key []byte) error {
	return s.kv.Remove(path.Join(s.customDataPath, string(key)))
}

func (s *storage) PutBootstrapped(container *metadata.StoreWithRWLock, resources ...*metadata.ShardWithRWLock) (bool, error) {
	clusterID, err := s.kv.AllocID()
	if err != nil {
		return false, err
	}

	v, err := container.Marshal()
	if err != nil {
		return false, err
	}

	batch := &Batch{}
	batch.SaveKeys = append(batch.SaveKeys, s.getKey(container.ID(), s.containerPath))
	batch.SaveValues = append(batch.SaveValues, string(v))
	for _, res := range resources {
		v, err = res.Marshal()
		if err != nil {
			return false, err
		}

		batch.SaveKeys = append(batch.SaveKeys, s.getKey(res.ID(), s.resourcePath))
		batch.SaveValues = append(batch.SaveValues, string(v))
	}

	ok, _, err := s.kv.SaveIfNotExists(s.clusterPath, string(format.Uint64ToString(clusterID)), batch)
	return ok, err
}

func (s *storage) AlreadyBootstrapped() (bool, error) {
	v, err := s.kv.Load(s.clusterPath)
	if err != nil {
		return false, err
	}

	return v != "", nil
}

func (s *storage) getKey(id uint64, base string) string {
	return path.Join(base, fmt.Sprintf("%020d", id))
}

func (s *storage) loadFloatWithDefaultValue(path string, def float64) (float64, error) {
	res, err := s.kv.Load(path)
	if err != nil {
		return 0, err
	}
	if res == "" {
		return def, nil
	}
	val, err := strconv.ParseFloat(res, 64)
	if err != nil {
		return 0, err
	}
	return val, nil
}

func (s *storage) containerWeightPath(id uint64, typ string) string {
	return path.Join(s.schedulePath, "weight", fmt.Sprintf("%020d", id), typ)
}

func (s *storage) jobKey(jobType metapb.JobType) string {
	return path.Join(s.jobPath, string(format.Uint64ToString(uint64(jobType))))
}

func (s *storage) jobDataKey(jobType metapb.JobType) string {
	return path.Join(s.jobDataPath, string(format.Uint64ToString(uint64(jobType))))
}
