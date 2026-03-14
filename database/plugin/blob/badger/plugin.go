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
// Block cache is needed because we use Snappy compression. Badger's
// own default is 256 MB block cache and 0 (unlimited) index cache.
// Setting IndexCacheSize to 0 lets Badger memory-map the full index
// without eviction, which is preferable to capping it.
//
// Operators can override via block-cache-size / index-cache-size CLI
// flags or YAML config.
const (
	DefaultBlockCacheSize   = 268435456  // 256 MB — Badger's own default; sufficient for core nodes
	DefaultIndexCacheSize   = 0          // 0 = unlimited; Badger memory-maps the full index
	DefaultValueLogFileSize = 1073741824 // 1 GB
	DefaultMemTableSize     = 134217728  // 128 MB
	DefaultValueThreshold   = 1048576    // 1 MB
)

type Profile string

const (
	ProfileCore Profile = "core"
	ProfileAPI  Profile = "api"
	ProfileLoad Profile = "load"
)

type ProfileSettings struct {
	BlockCacheSize       uint64
	IndexCacheSize       uint64
	MemTableSize         uint64
	CompactBlockMetadata bool
}

var (
	cmdlineOptions struct {
		dataDir          string
		runMode          string
		storageMode      string
		blockCacheSize   uint64
		indexCacheSize   uint64
		valueLogFileSize uint64
		memTableSize     uint64
		valueThreshold   uint64
		gcEnabled        bool
	}
	cmdlineOptionsMutex sync.RWMutex
)

func profileSettings(profile Profile) ProfileSettings {
	switch profile {
	case ProfileLoad:
		return ProfileSettings{
			BlockCacheSize:       268435456, // 256MB
			IndexCacheSize:       67108864,  // 64MB
			MemTableSize:         67108864,  // 64MB
			CompactBlockMetadata: false,
		}
	case ProfileCore:
		return ProfileSettings{
			BlockCacheSize:       536870912, // 512MB
			IndexCacheSize:       134217728, // 128MB
			MemTableSize:         67108864,  // 64MB
			CompactBlockMetadata: false,
		}
	case ProfileAPI:
		fallthrough
	default:
		return ProfileSettings{
			BlockCacheSize:       DefaultBlockCacheSize,
			IndexCacheSize:       DefaultIndexCacheSize,
			MemTableSize:         DefaultMemTableSize,
			CompactBlockMetadata: false,
		}
	}
}

func resolveProfile(runMode, storageMode string) Profile {
	if runMode == string(ProfileLoad) {
		return ProfileLoad
	}
	if storageMode == string(ProfileAPI) {
		return ProfileAPI
	}
	return ProfileCore
}

func applyOperationalDefaults(
	runMode string,
	storageMode string,
	blockCacheSize *uint64,
	indexCacheSize *uint64,
	memTableSize *uint64,
) {
	settings := profileSettings(resolveProfile(runMode, storageMode))
	if *blockCacheSize == DefaultBlockCacheSize {
		*blockCacheSize = settings.BlockCacheSize
	}
	if *indexCacheSize == DefaultIndexCacheSize {
		*indexCacheSize = settings.IndexCacheSize
	}
	if *memTableSize == DefaultMemTableSize {
		*memTableSize = settings.MemTableSize
	}
}

func useCompactBlockMetadata(runMode, storageMode string) bool {
	return (runMode == "serve" || runMode == "leios") &&
		storageMode == string(ProfileCore)
}

// initCmdlineOptions sets default values for cmdlineOptions
func initCmdlineOptions() {
	cmdlineOptionsMutex.Lock()
	defer cmdlineOptionsMutex.Unlock()
	cmdlineOptions.blockCacheSize = DefaultBlockCacheSize
	cmdlineOptions.indexCacheSize = DefaultIndexCacheSize
	cmdlineOptions.valueLogFileSize = DefaultValueLogFileSize
	cmdlineOptions.memTableSize = DefaultMemTableSize
	cmdlineOptions.valueThreshold = DefaultValueThreshold
	cmdlineOptions.runMode = ""
	cmdlineOptions.storageMode = "core"
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
	cmdlineOptionsMutex.Lock()
	// Copy cmdline values to locals so applyOperationalDefaults does not
	// mutate the shared cmdlineOptions struct.
	blockCacheSize := cmdlineOptions.blockCacheSize
	indexCacheSize := cmdlineOptions.indexCacheSize
	memTableSize := cmdlineOptions.memTableSize
	profile := resolveProfile(cmdlineOptions.runMode, cmdlineOptions.storageMode)
	settings := profileSettings(profile)
	applyOperationalDefaults(
		cmdlineOptions.runMode,
		cmdlineOptions.storageMode,
		&blockCacheSize,
		&indexCacheSize,
		&memTableSize,
	)
	// Safe conversion from uint64 to int64 with bounds checking
	valueLogFileSize := min(cmdlineOptions.valueLogFileSize,
		// Cap at max int64
		uint64(math.MaxInt64))
	memTableSizeCapped := min(memTableSize,
		// Cap at max int64
		uint64(math.MaxInt64))
	valueThreshold := min(cmdlineOptions.valueThreshold,
		// Cap at max int64
		uint64(math.MaxInt64))
	// #nosec G115
	opts := []BlobStoreBadgerOptionFunc{
		WithDataDir(cmdlineOptions.dataDir),
		WithBlockCacheSize(blockCacheSize),
		WithIndexCacheSize(indexCacheSize),
		WithValueLogFileSize(
			int64(valueLogFileSize),
		),
		WithMemTableSize(
			int64(memTableSizeCapped),
		),
		WithCompactBlockMetadata(
			settings.CompactBlockMetadata ||
				useCompactBlockMetadata(
					cmdlineOptions.runMode,
					cmdlineOptions.storageMode,
				),
		),
		WithValueThreshold(
			int64(valueThreshold),
		),
		WithGc(cmdlineOptions.gcEnabled),
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
