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

package sqlite

import (
	"context"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	hostplugin "github.com/blinklabs-io/dingo/plugin"
)

// Config contains SQLite-specific configuration.
type Config struct {
	// DataDir overrides the application-wide database path for this provider.
	// An empty value uses ProviderDependencies.DataDir.
	DataDir        string `yaml:"dataDir"`
	MaxConnections int    `yaml:"maxConnections"`
}

// DefaultMaxConnections is the default SQLite connection pool size.
const DefaultMaxConnections = 5

// RegisterProvider registers the SQLite metadata provider with host.
func RegisterProvider(host *hostplugin.Host) error {
	return hostplugin.Register(
		host,
		hostplugin.Descriptor{
			Capability:  hostplugin.CapabilityStorageMetadata,
			Name:        "sqlite",
			Description: "SQLite relational database",
		},
		func() Config { return Config{} },
		func(_ context.Context, cfg Config, deps metadata.ProviderDependencies) (*MetadataStoreSqlite, hostplugin.Instance, error) {
			dataDir := deps.DataDir
			if cfg.DataDir != "" {
				dataDir = cfg.DataDir
			}
			maxConnections := deps.MaxConnections
			if cfg.MaxConnections > 0 {
				maxConnections = cfg.MaxConnections
			}
			if maxConnections <= 0 {
				maxConnections = DefaultMaxConnections
			}
			store, err := NewWithOptions(
				WithDataDir(dataDir),
				WithMaxConnections(maxConnections),
				WithStorageMode(deps.StorageMode),
				WithLogger(deps.Logger),
				WithPromRegistry(deps.PromRegistry),
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
