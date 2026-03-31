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

// Default cache sizes for BadgerDB (in bytes).
//
// Block cache is needed when Snappy compression is enabled. Badger's
// own default is 256 MB block cache and 0 (unlimited) index cache.
// Set compression=false and block-cache-size=0 for mmap-only mode.
// Setting IndexCacheSize to 0 lets Badger memory-map the full index
// without eviction, which is preferable to capping it.
//
// Operators can override via block-cache-size / index-cache-size CLI
// flags or YAML config.
const (
	DefaultBlockCacheSize         = 268435456  // 256 MB
	DefaultIndexCacheSize         = 0          // 0 = unlimited
	DefaultValueLogFileSize       = 1073741824 // 1 GB
	DefaultMemTableSize           = 134217728  // 128 MB
	DefaultValueThreshold         = 1048576    // 1 MB
	DefaultCoreBlockCacheSize     = 0
	DefaultCoreIndexCacheSize     = 0
	DefaultCoreCompressionEnabled = false
	DefaultAPIBlockCacheSize      = DefaultBlockCacheSize
	DefaultAPIIndexCacheSize      = DefaultIndexCacheSize
	DefaultAPICompressionEnabled  = true
)

var (
	cmdlineOptions struct {
		dataDir               string
		runMode               string
		storageMode           string
		blockCacheSize        uint64
		indexCacheSize        uint64
		valueLogFileSize      uint64
		memTableSize          uint64
		valueThreshold        uint64
		gcEnabled             bool
		compressionEnabled    bool
		blockCacheSizeSet     bool
		indexCacheSizeSet     bool
		compressionEnabledSet bool
	}
	cmdlineOptionsMutex sync.RWMutex
)

func applyStorageModeDefaults(
	storageMode string,
	blockCacheSize *uint64,
	indexCacheSize *uint64,
	compressionEnabled *bool,
	blockCacheSizeSet bool,
	indexCacheSizeSet bool,
	compressionEnabledSet bool,
) {
	switch storageMode {
	case "api":
		if !blockCacheSizeSet {
			*blockCacheSize = DefaultAPIBlockCacheSize
		}
		if !indexCacheSizeSet {
			*indexCacheSize = DefaultAPIIndexCacheSize
		}
		if !compressionEnabledSet {
			*compressionEnabled = DefaultAPICompressionEnabled
		}
	default:
		if !blockCacheSizeSet {
			*blockCacheSize = DefaultCoreBlockCacheSize
		}
		if !indexCacheSizeSet {
			*indexCacheSize = DefaultCoreIndexCacheSize
		}
		if !compressionEnabledSet {
			*compressionEnabled = DefaultCoreCompressionEnabled
		}
	}
}

func useCompactBlockMetadata(runMode, storageMode string) bool {
	return (runMode == "serve" || runMode == "leios") &&
		storageMode == "core"
}

// initCmdlineOptions sets default values for cmdlineOptions
func initCmdlineOptions() {
	cmdlineOptionsMutex.Lock()
	defer cmdlineOptionsMutex.Unlock()
	cmdlineOptions.blockCacheSize = DefaultCoreBlockCacheSize
	cmdlineOptions.indexCacheSize = DefaultCoreIndexCacheSize
	cmdlineOptions.valueLogFileSize = DefaultValueLogFileSize
	cmdlineOptions.memTableSize = DefaultMemTableSize
	cmdlineOptions.valueThreshold = DefaultValueThreshold
	cmdlineOptions.runMode = ""
	cmdlineOptions.storageMode = "core"
	cmdlineOptions.gcEnabled = true
	cmdlineOptions.compressionEnabled = DefaultCoreCompressionEnabled
	cmdlineOptions.blockCacheSizeSet = false
	cmdlineOptions.indexCacheSizeSet = false
	cmdlineOptions.compressionEnabledSet = false
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
					Name:         "run-mode",
					Type:         plugin.PluginOptionTypeString,
					Description:  "Operational run mode",
					DefaultValue: "",
					Dest:         &(cmdlineOptions.runMode),
				},
				{
					Name:         "storage-mode",
					Type:         plugin.PluginOptionTypeString,
					Description:  "Storage tier: core or api",
					DefaultValue: "core",
					Dest:         &(cmdlineOptions.storageMode),
				},
				{
					Name:         "block-cache-size",
					Type:         plugin.PluginOptionTypeUint,
					Description:  "Badger block cache size (runtime default: 0 in core mode, 268435456 in api mode)",
					DefaultValue: uint64(DefaultCoreBlockCacheSize),
					Dest:         &(cmdlineOptions.blockCacheSize),
					SetIndicator: &(cmdlineOptions.blockCacheSizeSet),
				},
				{
					Name:         "index-cache-size",
					Type:         plugin.PluginOptionTypeUint,
					Description:  "Badger index cache size",
					DefaultValue: uint64(DefaultIndexCacheSize),
					Dest:         &(cmdlineOptions.indexCacheSize),
					SetIndicator: &(cmdlineOptions.indexCacheSizeSet),
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
				{
					Name:         "compression",
					Type:         plugin.PluginOptionTypeBool,
					Description:  "Enable Snappy compression (runtime default: false in core mode, true in api mode)",
					DefaultValue: DefaultCoreCompressionEnabled,
					Dest:         &(cmdlineOptions.compressionEnabled),
					SetIndicator: &(cmdlineOptions.compressionEnabledSet),
				},
			},
		},
	)
}

func NewFromCmdlineOptions() plugin.Plugin {
	cmdlineOptionsMutex.Lock()
	blockCacheSize := cmdlineOptions.blockCacheSize
	indexCacheSize := cmdlineOptions.indexCacheSize
	memTableSize := cmdlineOptions.memTableSize
	compressionEnabled := cmdlineOptions.compressionEnabled
	applyStorageModeDefaults(
		cmdlineOptions.storageMode,
		&blockCacheSize,
		&indexCacheSize,
		&compressionEnabled,
		cmdlineOptions.blockCacheSizeSet,
		cmdlineOptions.indexCacheSizeSet,
		cmdlineOptions.compressionEnabledSet,
	)
	// Safe conversion from uint64 to int64 with bounds checking
	valueLogFileSize := min(
		cmdlineOptions.valueLogFileSize,
		uint64(math.MaxInt64),
	)
	memTableSizeCapped := min(
		memTableSize,
		uint64(math.MaxInt64),
	)
	valueThreshold := min(
		cmdlineOptions.valueThreshold,
		uint64(math.MaxInt64),
	)
	// #nosec G115
	opts := []BlobStoreBadgerOptionFunc{
		WithDataDir(cmdlineOptions.dataDir),
		WithBlockCacheSize(blockCacheSize),
		WithIndexCacheSize(indexCacheSize),
		WithValueLogFileSize(int64(valueLogFileSize)),
		WithMemTableSize(int64(memTableSizeCapped)),
		WithCompactBlockMetadata(
			useCompactBlockMetadata(
				cmdlineOptions.runMode,
				cmdlineOptions.storageMode,
			),
		),
		WithValueThreshold(int64(valueThreshold)),
		WithGc(cmdlineOptions.gcEnabled),
		WithCompressionEnabled(compressionEnabled),
		WithDeferOpen(),
	}
	cmdlineOptionsMutex.Unlock()
	p, err := New(opts...)
	if err != nil {
		// Return a plugin that defers the error to Start()
		return plugin.NewErrorPlugin(err)
	}
	return p
}
