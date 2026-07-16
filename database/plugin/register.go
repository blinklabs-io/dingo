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
	"fmt"
	"slices"
	"strings"

	"github.com/spf13/pflag"
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

// Register adds a plugin entry to the global registry.
// NOTE: This function is not thread-safe and should only be called during
// package initialization (e.g., in init() functions) before any concurrent
// goroutines begin. Concurrent access to pluginEntries is not protected.
func Register(pluginEntry PluginEntry) {
	pluginEntries = append(pluginEntries, pluginEntry)
}

func PopulateCmdlineOptions(fs *pflag.FlagSet) error {
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
			"DINGO_DATABASE_%s_%s_",
			strings.ToUpper(PluginTypeName(plugin.Type)),
			strings.ToUpper(plugin.Name),
		)
		for _, option := range plugin.Options {
			if err := option.ProcessEnvVars(envVarPrefix); err != nil {
				return err
			}
		}
	}
	return nil
}

// buildTagGatedPluginNames lists plugin names that are valid but may not be
// registered in this build because they're compiled in only under specific
// build tags (e.g. -tags dingo_extra_plugins for gcs/s3/mysql/postgres).
// Config sections for these names are tolerated even when the corresponding
// plugin isn't compiled into this build; selecting one as the active
// blob/metadata plugin still fails downstream via MissingPluginError.
var buildTagGatedPluginNames = map[PluginType]map[string]struct{}{
	PluginTypeBlob:     {"gcs": {}, "s3": {}},
	PluginTypeMetadata: {"mysql": {}, "postgres": {}},
}

// ProcessConfig applies plugin-specific config values from a parsed YAML
// document. It rejects config keys that don't match any known option for
// the targeted plugin (e.g. "buckit" instead of "bucket") and config
// sections whose plugin name doesn't match any known plugin (e.g. "badgre"
// instead of "badger"), so typos fail config load instead of being
// silently ignored. All keys for a plugin are validated before any of
// its options are applied, so a config block mixing valid and invalid
// keys never partially applies before the error is returned.
func ProcessConfig(
	pluginConfig map[string]map[string]map[string]any,
) error {
	matched := make(map[PluginType]map[string]struct{})
	for _, plugin := range pluginEntries {
		pluginTypeData, ok := pluginConfig[PluginTypeName(plugin.Type)]
		if !ok {
			continue
		}
		pluginData, ok := pluginTypeData[plugin.Name]
		if !ok {
			continue
		}
		if matched[plugin.Type] == nil {
			matched[plugin.Type] = make(map[string]struct{})
		}
		matched[plugin.Type][plugin.Name] = struct{}{}

		known := make(map[string]struct{}, len(plugin.Options))
		for _, option := range plugin.Options {
			known[option.Name] = struct{}{}
		}
		var unknown []string
		for key := range pluginData {
			if _, ok := known[key]; !ok {
				unknown = append(unknown, key)
			}
		}
		if len(unknown) > 0 {
			slices.Sort(unknown)
			return fmt.Errorf(
				"unknown config key(s) %v for %s plugin %q",
				unknown,
				PluginTypeName(plugin.Type),
				plugin.Name,
			)
		}
		for _, option := range plugin.Options {
			if err := option.ProcessConfig(pluginData); err != nil {
				return err
			}
		}
	}
	for _, pluginType := range []PluginType{PluginTypeBlob, PluginTypeMetadata} {
		typeName := PluginTypeName(pluginType)
		pluginTypeData, ok := pluginConfig[typeName]
		if !ok {
			continue
		}
		for name := range pluginTypeData {
			if _, ok := matched[pluginType][name]; ok {
				continue
			}
			if _, ok := buildTagGatedPluginNames[pluginType][name]; ok {
				continue
			}
			return fmt.Errorf(
				"unknown %s plugin %q in config",
				typeName,
				name,
			)
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
