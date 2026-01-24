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

package sqlite

import (
	"sync"

	"github.com/blinklabs-io/dingo/database/plugin"
)

// DefaultMaxConnections is the default connection pool size for SQLite.
// This should match the DatabaseWorkers configuration (default 5).
const DefaultMaxConnections = 5

var (
	cmdlineOptions struct {
		dataDir        string
		maxConnections int
	}
	cmdlineOptionsMutex sync.RWMutex
)

// initCmdlineOptions sets default values for cmdlineOptions
func initCmdlineOptions() {
	cmdlineOptionsMutex.Lock()
	defer cmdlineOptionsMutex.Unlock()
	cmdlineOptions.dataDir = ".dingo"
	cmdlineOptions.maxConnections = DefaultMaxConnections
}

// Register plugin
func init() {
	initCmdlineOptions()
	plugin.Register(
		plugin.PluginEntry{
			Type:               plugin.PluginTypeMetadata,
			Name:               "sqlite",
			Description:        "SQLite relational database",
			NewFromOptionsFunc: NewFromCmdlineOptions,
			Options: []plugin.PluginOption{
				{
					Name:         "data-dir",
					Type:         plugin.PluginOptionTypeString,
					Description:  "Data directory for sqlite storage",
					DefaultValue: ".dingo",
					Dest:         &(cmdlineOptions.dataDir),
				},
				{
					Name:         "max-connections",
					Type:         plugin.PluginOptionTypeInt,
					Description:  "Maximum number of database connections (should match DatabaseWorkers)",
					DefaultValue: DefaultMaxConnections,
					Dest:         &(cmdlineOptions.maxConnections),
				},
			},
		},
	)
}

func NewFromCmdlineOptions() plugin.Plugin {
	cmdlineOptionsMutex.RLock()
	dataDir := cmdlineOptions.dataDir
	maxConnections := cmdlineOptions.maxConnections
	cmdlineOptionsMutex.RUnlock()

	opts := []SqliteOptionFunc{
		WithDataDir(dataDir),
		WithMaxConnections(maxConnections),
		// Logger and promRegistry will use defaults if nil
	}
	p, err := NewWithOptions(opts...)
	if err != nil {
		// Return a plugin that defers the error to Start()
		return plugin.NewErrorPlugin(err)
	}
	return p
}
