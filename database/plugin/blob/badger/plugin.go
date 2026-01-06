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
	"math"
	"sync"

	"github.com/blinklabs-io/dingo/database/plugin"
)

// Default cache sizes for BadgerDB (in bytes)
const (
	DefaultBlockCacheSize   = 1610612736 // 1.5GB (increased from 768MB)
	DefaultIndexCacheSize   = 536870912  // 512MB (increased from 256MB)
	DefaultValueLogFileSize = 1073741824 // 1GB (keep large values on disk, write amplification reduction)
	DefaultMemTableSize     = 134217728  // 128MB (increased from 64MB for better write buffering)
	DefaultValueThreshold   = 1048576    // 1MB (keep small values in LSM, large blobs in value log)
)

var (
	cmdlineOptions struct {
		dataDir          string
		blockCacheSize   uint64
		indexCacheSize   uint64
		valueLogFileSize uint64
		memTableSize     uint64
		valueThreshold   uint64
		gcEnabled        bool
	}
	cmdlineOptionsMutex sync.RWMutex
)

// initCmdlineOptions sets default values for cmdlineOptions
func initCmdlineOptions() {
	cmdlineOptionsMutex.Lock()
	defer cmdlineOptionsMutex.Unlock()
	cmdlineOptions.blockCacheSize = DefaultBlockCacheSize
	cmdlineOptions.indexCacheSize = DefaultIndexCacheSize
	cmdlineOptions.valueLogFileSize = DefaultValueLogFileSize
	cmdlineOptions.memTableSize = DefaultMemTableSize
	cmdlineOptions.valueThreshold = DefaultValueThreshold
	cmdlineOptions.gcEnabled = true
	cmdlineOptions.dataDir = ".dingo"
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
					DefaultValue: ".dingo",
					Dest:         &(cmdlineOptions.dataDir),
				},
				{
					Name:         "block-cache-size",
					Type:         plugin.PluginOptionTypeUint,
					Description:  "Badger block cache size",
					DefaultValue: uint64(DefaultBlockCacheSize),
					Dest:         &(cmdlineOptions.blockCacheSize),
				},
				{
					Name:         "index-cache-size",
					Type:         plugin.PluginOptionTypeUint,
					Description:  "Badger index cache size",
					DefaultValue: uint64(DefaultIndexCacheSize),
					Dest:         &(cmdlineOptions.indexCacheSize),
				},
				{
					Name:         "gc",
					Type:         plugin.PluginOptionTypeBool,
					Description:  "Enable garbage collection",
					DefaultValue: true,
					Dest:         &(cmdlineOptions.gcEnabled),
				},
				{
					Name:         "value-log-file-size",
					Type:         plugin.PluginOptionTypeUint,
					Description:  "Badger value log file size",
					DefaultValue: uint64(DefaultValueLogFileSize),
					Dest:         &(cmdlineOptions.valueLogFileSize),
				},
				{
					Name:         "memtable-size",
					Type:         plugin.PluginOptionTypeUint,
					Description:  "Badger memtable size",
					DefaultValue: uint64(DefaultMemTableSize),
					Dest:         &(cmdlineOptions.memTableSize),
				},
				{
					Name:         "value-threshold",
					Type:         plugin.PluginOptionTypeUint,
					Description:  "Badger value threshold for LSM vs value log",
					DefaultValue: uint64(DefaultValueThreshold),
					Dest:         &(cmdlineOptions.valueThreshold),
				},
			},
		},
	)
}

func NewFromCmdlineOptions() plugin.Plugin {
	cmdlineOptionsMutex.RLock()
	// Safe conversion from uint64 to int64 with bounds checking
	valueLogFileSize := min(cmdlineOptions.valueLogFileSize,
		// Cap at max int64
		uint64(math.MaxInt64))
	memTableSize := min(cmdlineOptions.memTableSize,
		// Cap at max int64
		uint64(math.MaxInt64))
	valueThreshold := min(cmdlineOptions.valueThreshold,
		// Cap at max int64
		uint64(math.MaxInt64))
	// #nosec G115
	opts := []BlobStoreBadgerOptionFunc{
		WithDataDir(cmdlineOptions.dataDir),
		WithBlockCacheSize(cmdlineOptions.blockCacheSize),
		WithIndexCacheSize(cmdlineOptions.indexCacheSize),
		WithValueLogFileSize(
			int64(valueLogFileSize),
		),
		WithMemTableSize(
			int64(memTableSize),
		),
		WithValueThreshold(
			int64(valueThreshold),
		),
		WithGc(cmdlineOptions.gcEnabled),
	}
	cmdlineOptionsMutex.RUnlock()
	p, err := New(opts...)
	if err != nil {
		// Return a plugin that defers the error to Start()
		return plugin.NewErrorPlugin(err)
	}
	return p
}
