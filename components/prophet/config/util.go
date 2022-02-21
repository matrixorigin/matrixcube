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

package config

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/matrixorigin/matrixcube/components/prophet/util/typeutil"
	"github.com/matrixorigin/matrixcube/pb/metapb"
)

const (
	// Label key consists of alphanumeric characters, '-', '_', '.' or '/', and must start and end with an
	// alphanumeric character. If can also contain an extra '$' at the beginning.
	keyFormat = "^[$]?[A-Za-z0-9]([-A-Za-z0-9_./]*[A-Za-z0-9])?$"
	// Value key can be any combination of alphanumeric characters, '-', '_', '.' or '/'. It can also be empty to
	// mark the label as deleted.
	valueFormat = "^[-A-Za-z0-9_./]*$"
)

func validateFormat(s, format string) error {
	isValid, _ := regexp.MatchString(format, s)
	if !isValid {
		return fmt.Errorf("%s does not match format %q", s, format)
	}
	return nil
}

// ValidateLabels checks the legality of the labels.
func ValidateLabels(labels []metapb.Pair) error {
	for _, label := range labels {
		if err := validateFormat(label.Key, keyFormat); err != nil {
			return err
		}
		if err := validateFormat(label.Value, valueFormat); err != nil {
			return err
		}
	}
	return nil
}

// ValidateURLWithScheme checks the format of the URL.
func ValidateURLWithScheme(rawURL string) error {
	u, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return err
	}
	if u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("%s has no scheme", rawURL)
	}
	return nil
}

var schedulerMap = make(map[string]struct{})

// RegisterScheduler registers the scheduler type.
func RegisterScheduler(typ string) {
	schedulerMap[typ] = struct{}{}
}

// IsSchedulerRegistered checks if the named scheduler type is registered.
func IsSchedulerRegistered(name string) bool {
	_, ok := schedulerMap[name]
	return ok
}

// Utility to test if a configuration is defined.
type configMetaData struct {
	meta *toml.MetaData
	path []string
}

func newConfigMetadata(meta *toml.MetaData) *configMetaData {
	return &configMetaData{meta: meta}
}

func (m *configMetaData) IsDefined(key string) bool {
	if m.meta == nil {
		return false
	}
	keys := append([]string(nil), m.path...)
	keys = append(keys, key)
	return m.meta.IsDefined(keys...)
}

func (m *configMetaData) Child(path ...string) *configMetaData {
	newPath := append([]string(nil), m.path...)
	newPath = append(newPath, path...)
	return &configMetaData{
		meta: m.meta,
		path: newPath,
	}
}

func (m *configMetaData) CheckUndecoded() error {
	if m.meta == nil {
		return nil
	}
	undecoded := m.meta.Undecoded()
	if len(undecoded) == 0 {
		return nil
	}
	errInfo := "Config contains undefined item: "
	for _, key := range undecoded {
		errInfo += key.String() + ", "
	}
	return errors.New(errInfo[:len(errInfo)-2])
}

func adjustString(v *string, defValue string) {
	if len(*v) == 0 {
		*v = defValue
	}
}

func adjustUint64(v *uint64, defValue uint64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustInt64(v *int64, defValue int64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustFloat64(v *float64, defValue float64) {
	if *v == 0 {
		*v = defValue
	}
}

func adjustDuration(v *typeutil.Duration, defValue time.Duration) {
	if v.Duration <= 0 {
		v.Duration = defValue
	}
}

func adjustSchedulers(v *SchedulerConfigs, defValue SchedulerConfigs) {
	if len(*v) == 0 {
		// Make a copy to avoid changing DefaultSchedulers unexpectedly.
		// When reloading from storage, the config is passed to json.Unmarshal.
		// Without clone, the DefaultSchedulers could be overwritten.
		*v = append(defValue[:0:0], defValue...)
	}
}

func adjustPath(p *string) {
	absPath, err := filepath.Abs(*p)
	if err == nil {
		*p = absPath
	}
}

// NewTestOptions creates default options for testing.
func NewTestOptions() *PersistOptions {
	// register default schedulers in case config check fail.
	for _, d := range DefaultSchedulers {
		RegisterScheduler(d.Type)
	}
	c := NewConfig()
	c.Schedule.EnableReplaceOfflineReplica = true
	c.Schedule.EnableMakeUpReplica = true
	c.Schedule.EnableRemoveExtraReplica = true
	c.Schedule.EnableRemoveDownReplica = true
	c.Schedule.EnableLocationReplacement = true
	c.Adjust(nil, false)
	return NewPersistOptions(c, nil)
}
