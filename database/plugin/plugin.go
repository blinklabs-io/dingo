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

import "fmt"

type Plugin interface {
	Start() error
	Stop() error
}

// ErrorPlugin is a plugin that always returns an error on Start()
type ErrorPlugin struct {
	Err error
}

func (e *ErrorPlugin) Start() error {
	return e.Err
}

func (e *ErrorPlugin) Stop() error {
	return nil
}

// NewErrorPlugin creates a new error plugin that returns the given error on Start()
func NewErrorPlugin(err error) Plugin {
	return &ErrorPlugin{Err: err}
}

// StartPlugin gets a plugin from the registry and starts it
func StartPlugin(pluginType PluginType, pluginName string) (Plugin, error) {
	// Get the plugin from the registry
	p := GetPlugin(pluginType, pluginName)
	if p == nil {
		return nil, fmt.Errorf(
			"%s plugin '%s' not found",
			PluginTypeName(pluginType),
			pluginName,
		)
	}

	// Start the plugin
	if err := p.Start(); err != nil {
		return nil, fmt.Errorf(
			"failed to start %s plugin '%s': %w",
			PluginTypeName(pluginType),
			pluginName,
			err,
		)
	}

	return p, nil
}

// SetPluginOption sets the value of a named option for a plugin entry. This
// is used by callers that need to programmatically override plugin defaults
// (for example to set data-dir before starting a plugin). It returns an error
// if the plugin or option is not found or if the value type is incompatible.
// NOTE: This function accesses the global pluginEntries slice without
// synchronization. It should only be called during initialization or in
// single-threaded contexts to avoid race conditions.
// NOTE: This function writes directly to plugin option destinations (e.g.,
// cmdlineOptions fields) without acquiring the plugin's cmdlineOptionsMutex.
// It must be called before any plugin instantiation to avoid data races with
// concurrent reads in NewFromCmdlineOptions.
func SetPluginOption(
	pluginType PluginType,
	pluginName string,
	optionName string,
	value any,
) error {
	for i := range pluginEntries {
		p := &pluginEntries[i]
		if p.Type != pluginType || p.Name != pluginName {
			continue
		}
		for _, opt := range p.Options {
			if opt.Name != optionName {
				continue
			}
			// Perform a type-checked assignment into the Dest pointer
			switch opt.Type {
			case PluginOptionTypeString:
				v, ok := value.(string)
				if !ok {
					return fmt.Errorf(
						"invalid type for option %s: expected string",
						optionName,
					)
				}
				if opt.Dest == nil {
					return fmt.Errorf(
						"nil destination for option %s",
						optionName,
					)
				}
				dest, ok := opt.Dest.(*string)
				if !ok {
					return fmt.Errorf(
						"invalid destination type for option %s: expected *string",
						optionName,
					)
				}
				if dest == nil {
					return fmt.Errorf(
						"nil destination pointer for option %s",
						optionName,
					)
				}
				*dest = v
				return nil
			case PluginOptionTypeBool:
				v, ok := value.(bool)
				if !ok {
					return fmt.Errorf(
						"invalid type for option %s: expected bool",
						optionName,
					)
				}
				if opt.Dest == nil {
					return fmt.Errorf(
						"nil destination for option %s",
						optionName,
					)
				}
				dest, ok := opt.Dest.(*bool)
				if !ok {
					return fmt.Errorf(
						"invalid destination type for option %s: expected *bool",
						optionName,
					)
				}
				if dest == nil {
					return fmt.Errorf(
						"nil destination pointer for option %s",
						optionName,
					)
				}
				*dest = v
				return nil
			case PluginOptionTypeInt:
				v, ok := value.(int)
				if !ok {
					return fmt.Errorf(
						"invalid type for option %s: expected int",
						optionName,
					)
				}
				if opt.Dest == nil {
					return fmt.Errorf(
						"nil destination for option %s",
						optionName,
					)
				}
				dest, ok := opt.Dest.(*int)
				if !ok {
					return fmt.Errorf(
						"invalid destination type for option %s: expected *int",
						optionName,
					)
				}
				if dest == nil {
					return fmt.Errorf(
						"nil destination pointer for option %s",
						optionName,
					)
				}
				*dest = v
				return nil
			case PluginOptionTypeUint:
				// accept uint64 or int
				switch tv := value.(type) {
				case uint64:
					if opt.Dest == nil {
						return fmt.Errorf("nil destination for option %s", optionName)
					}
					dest, ok := opt.Dest.(*uint64)
					if !ok {
						return fmt.Errorf("invalid destination type for option %s: expected *uint64", optionName)
					}
					if dest == nil {
						return fmt.Errorf("nil destination pointer for option %s", optionName)
					}
					*dest = tv
					return nil
				case int:
					if tv < 0 {
						return fmt.Errorf("invalid value for option %s: negative int", optionName)
					}
					if opt.Dest == nil {
						return fmt.Errorf("nil destination for option %s", optionName)
					}
					dest, ok := opt.Dest.(*uint64)
					if !ok {
						return fmt.Errorf("invalid destination type for option %s: expected *uint64", optionName)
					}
					if dest == nil {
						return fmt.Errorf("nil destination pointer for option %s", optionName)
					}
					*dest = uint64(tv)
					return nil
				default:
					return fmt.Errorf("invalid type for option %s: expected uint64 or int", optionName)
				}
			default:
				return fmt.Errorf(
					"unknown plugin option type %d for option %s",
					opt.Type,
					optionName,
				)
			}
		}
		// Option not found for this plugin: treat as non-fatal. This allows
		// callers to attempt to set options that may not exist for all
		// implementations (for example `data-dir` may not be relevant).
		return nil
	}
	return fmt.Errorf(
		"plugin %s of type %s not found",
		pluginName,
		PluginTypeName(pluginType),
	)
}
