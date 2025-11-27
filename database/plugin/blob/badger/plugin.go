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

package badger

import (
	"sync"

	"github.com/blinklabs-io/dingo/database/plugin"
)

// Default cache sizes for BadgerDB (in bytes)
const (
	DefaultBlockCacheSize = 805306368 // 768MB
	DefaultIndexCacheSize = 268435456 // 256MB
)

var (
	cmdlineOptions struct {
		DataDir        string
		BlockCacheSize uint64
		IndexCacheSize uint64
		GcEnabled      bool
	}
	cmdlineOptionsMutex sync.RWMutex
)

// initCmdlineOptions sets default values for cmdlineOptions
func initCmdlineOptions() {
	cmdlineOptionsMutex.Lock()
	defer cmdlineOptionsMutex.Unlock()
	cmdlineOptions.BlockCacheSize = DefaultBlockCacheSize
	cmdlineOptions.IndexCacheSize = DefaultIndexCacheSize
	cmdlineOptions.GcEnabled = true
}

// Register plugin
func init() {
	initCmdlineOptions()
	plugin.Register(
		plugin.PluginEntry{
			Type:               plugin.PluginTypeBlob,
			Name:               "badger",
			Description:        "BadgerDB local key-value store",
			NewFromOptionsFunc: NewFromCmdlineOptions,
			Options: []plugin.PluginOption{
				{
					Name:         "data-dir",
					Type:         plugin.PluginOptionTypeString,
					Description:  "Data directory for badger storage",
					DefaultValue: "",
					Dest:         &(cmdlineOptions.DataDir),
				},
				{
					Name:         "block-cache-size",
					Type:         plugin.PluginOptionTypeUint,
					Description:  "Badger block cache size",
					DefaultValue: uint64(DefaultBlockCacheSize),
					Dest:         &(cmdlineOptions.BlockCacheSize),
				},
				{
					Name:         "index-cache-size",
					Type:         plugin.PluginOptionTypeUint,
					Description:  "Badger index cache size",
					DefaultValue: uint64(DefaultIndexCacheSize),
					Dest:         &(cmdlineOptions.IndexCacheSize),
				},
				{
					Name:         "gc",
					Type:         plugin.PluginOptionTypeBool,
					Description:  "Enable garbage collection",
					DefaultValue: true,
					Dest:         &(cmdlineOptions.GcEnabled),
				},
			},
		},
	)
}

func NewFromCmdlineOptions() plugin.Plugin {
	cmdlineOptionsMutex.RLock()
	opts := []BlobStoreBadgerOptionFunc{
		WithDataDir(cmdlineOptions.DataDir),
		WithBlockCacheSize(cmdlineOptions.BlockCacheSize),
		WithIndexCacheSize(cmdlineOptions.IndexCacheSize),
		WithGc(cmdlineOptions.GcEnabled),
	}
	cmdlineOptionsMutex.RUnlock()
	p, err := New(opts...)
	if err != nil {
		// Return a plugin that defers the error to Start()
		return plugin.NewErrorPlugin(err)
	}
	return p
}
