package schedule

import (
	"path/filepath"
	"plugin"
	"sync"

	"github.com/deepfabric/prophet/util"
)

// PluginInterface is used to manage all plugin.
type PluginInterface struct {
	pluginMap     map[string]*plugin.Plugin
	pluginMapLock sync.RWMutex
}

// NewPluginInterface create a plugin interface
func NewPluginInterface() *PluginInterface {
	return &PluginInterface{
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
		util.GetLogger().Infof("open plugin file %s", filePath)
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
