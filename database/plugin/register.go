// Copyright 2025 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plugin

import (
	"flag"
	"fmt"
)

type PluginType int

const (
	PluginTypeMetadata PluginType = 1
	PluginTypeBlob     PluginType = 2
)

func PluginTypeName(pluginType PluginType) string {
	switch pluginType {
	case PluginTypeMetadata:
		return "metadata"
	case PluginTypeBlob:
		return "blob"
	default:
		return ""
	}
}

type PluginEntry struct {
	NewFromOptionsFunc func() Plugin
	Name               string
	Description        string
	Options            []PluginOption
	Type               PluginType
}

var pluginEntries []PluginEntry

func Register(pluginEntry PluginEntry) {
	pluginEntries = append(pluginEntries, pluginEntry)
}

func PopulateCmdlineOptions(fs *flag.FlagSet) error {
	for _, plugin := range pluginEntries {
		for _, option := range plugin.Options {
			if err := option.AddToFlagSet(fs, PluginTypeName(plugin.Type), plugin.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

func ProcessEnvVars() error {
	for _, plugin := range pluginEntries {
		// Generate env var prefix based on plugin type and name
		envVarPrefix := fmt.Sprintf(
			"%s-%s-",
			PluginTypeName(plugin.Type),
			plugin.Name,
		)
		for _, option := range plugin.Options {
			if err := option.ProcessEnvVars(envVarPrefix); err != nil {
				return err
			}
		}
	}
	return nil
}

func ProcessConfig(
	pluginConfig map[string]map[string]map[any]any,
) error {
	for _, plugin := range pluginEntries {
		if pluginTypeData, ok := pluginConfig[PluginTypeName(plugin.Type)]; ok {
			if pluginData, ok := pluginTypeData[plugin.Name]; ok {
				for _, option := range plugin.Options {
					if err := option.ProcessConfig(pluginData); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func GetPlugins(pluginType PluginType) []PluginEntry {
	ret := []PluginEntry{}
	for _, plugin := range pluginEntries {
		if plugin.Type == pluginType {
			ret = append(ret, plugin)
		}
	}
	return ret
}

func GetPlugin(pluginType PluginType, name string) Plugin {
	for _, plugin := range pluginEntries {
		if plugin.Type == pluginType {
			if plugin.Name == name {
				return plugin.NewFromOptionsFunc()
			}
		}
	}
	return nil
}
