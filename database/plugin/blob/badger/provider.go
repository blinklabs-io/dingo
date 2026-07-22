// Copyright 2026 Blink Labs Software
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
	"context"
	"math"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
)

// Config contains Badger-specific configuration. Shared application settings
// such as the data directory and storage mode are supplied through
// Dependencies.
type Config struct {
	// DataDir overrides the application-wide database path for this provider.
	// An empty value uses ProviderDependencies.DataDir.
	DataDir          string  `yaml:"dataDir"`
	BlockCacheSize   *uint64 `yaml:"blockCacheSize"`
	IndexCacheSize   *uint64 `yaml:"indexCacheSize"`
	ValueLogFileSize uint64  `yaml:"valueLogFileSize"`
	MemTableSize     uint64  `yaml:"memTableSize"`
	ValueThreshold   uint64  `yaml:"valueThreshold"`
	GC               *bool   `yaml:"gc"`
	Compression      *bool   `yaml:"compression"`
	CompressionLevel uint64  `yaml:"compressionLevel"`
}

func defaultConfig() Config {
	return Config{
		ValueLogFileSize: DefaultValueLogFileSize,
		MemTableSize:     DefaultMemTableSize,
		ValueThreshold:   DefaultValueThreshold,
		CompressionLevel: DefaultCompressionLevel,
	}
}

// RegisterProvider registers the Badger blob provider with host.
func RegisterProvider(host *hostplugin.Host) error {
	return hostplugin.Register(
		host,
		hostplugin.Descriptor{
			Capability:  hostplugin.CapabilityStorageBlob,
			Name:        "badger",
			Description: "BadgerDB local key-value store",
		},
		defaultConfig,
		func(_ context.Context, cfg Config, deps blob.ProviderDependencies) (*BlobStoreBadger, hostplugin.Instance, error) {
			dataDir := deps.DataDir
			if cfg.DataDir != "" {
				dataDir = cfg.DataDir
			}
			blockCacheSize := uint64(DefaultCoreBlockCacheSize)
			indexCacheSize := uint64(DefaultCoreIndexCacheSize)
			compression := DefaultCoreCompressionEnabled
			if deps.StorageMode == "api" {
				blockCacheSize = DefaultAPIBlockCacheSize
				indexCacheSize = DefaultAPIIndexCacheSize
				compression = DefaultAPICompressionEnabled
			}
			if cfg.BlockCacheSize != nil {
				blockCacheSize = *cfg.BlockCacheSize
			}
			if cfg.IndexCacheSize != nil {
				indexCacheSize = *cfg.IndexCacheSize
			}
			if cfg.Compression != nil {
				compression = *cfg.Compression
			}
			gcEnabled := deps.RunMode != "load"
			if cfg.GC != nil {
				gcEnabled = *cfg.GC
			}
			store, err := New(
				WithDataDir(dataDir),
				WithLogger(deps.Logger),
				WithPromRegistry(deps.PromRegistry),
				WithBlockCacheSize(blockCacheSize),
				WithIndexCacheSize(indexCacheSize),
				// #nosec G115 -- values are capped to the destination maximum.
				WithValueLogFileSize(int64(min(cfg.ValueLogFileSize, uint64(math.MaxInt64)))),
				// #nosec G115 -- values are capped to the destination maximum.
				WithMemTableSize(int64(min(cfg.MemTableSize, uint64(math.MaxInt64)))),
				// #nosec G115 -- values are capped to the destination maximum.
				WithValueThreshold(int64(min(cfg.ValueThreshold, uint64(math.MaxInt64)))),
				WithCompactBlockMetadata(useCompactBlockMetadata(deps.RunMode, deps.StorageMode)),
				WithGc(gcEnabled),
				WithCompressionEnabled(compression),
				// #nosec G115 -- value is capped to the destination maximum.
				WithCompressionLevel(int(min(cfg.CompressionLevel, uint64(math.MaxInt)))),
				WithDeferOpen(),
			)
			if err != nil {
				return nil, nil, err
			}
			lifecycle := hostplugin.Lifecycle{
				StartFunc: func(context.Context) error { return store.Start() },
				StopFunc:  func(context.Context) error { return store.Stop() },
			}
			return store, lifecycle, nil
		},
	)
}
