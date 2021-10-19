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

package schedule

import (
	"path/filepath"
	"plugin"
	"sync"

	"github.com/matrixorigin/matrixcube/components/log"
	"go.uber.org/zap"
)

// PluginInterface is used to manage all plugin.
type PluginInterface struct {
	pluginMap     map[string]*plugin.Plugin
	pluginMapLock sync.RWMutex
	logger        *zap.Logger
}

// NewPluginInterface create a plugin interface
func NewPluginInterface(logger *zap.Logger) *PluginInterface {
	return &PluginInterface{
		logger:        log.Adjust(logger),
		pluginMap:     make(map[string]*plugin.Plugin),
		pluginMapLock: sync.RWMutex{},
	}
}

// GetFunction gets func by funcName from plugin(.so)
func (p *PluginInterface) GetFunction(path string, funcName string) (plugin.Symbol, error) {
	p.pluginMapLock.Lock()
	defer p.pluginMapLock.Unlock()
	if _, ok := p.pluginMap[path]; !ok {
		//open plugin
		filePath, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		p.logger.Info("open plugin file ", zap.String("file", filePath))
		plugin, err := plugin.Open(filePath)
		if err != nil {
			return nil, err
		}
		p.pluginMap[path] = plugin
	}
	//get func from plugin
	f, err := p.pluginMap[path].Lookup(funcName)
	if err != nil {
		return nil, err
	}
	return f, nil
}
