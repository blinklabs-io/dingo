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
