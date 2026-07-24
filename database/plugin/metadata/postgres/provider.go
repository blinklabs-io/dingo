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

//go:build dingo_extra_plugins

package postgres

import (
	"context"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/plugin"
)

type Config struct {
	Host     string `yaml:"host"`
	Port     uint   `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	SSLMode  string `yaml:"sslMode"`
	TimeZone string `yaml:"timeZone"`
	DSN      string `yaml:"dsn"`
}

func RegisterProvider(host *plugin.Host) error {
	return plugin.Register(host, plugin.Descriptor{Capability: plugin.CapabilityStorageMetadata, Name: "postgres", Description: "PostgreSQL relational database"},
		func() Config {
			return Config{Host: "localhost", Port: 5432, User: "postgres", Database: "postgres", SSLMode: "disable", TimeZone: "UTC"}
		},
		func(_ context.Context, cfg Config, deps metadata.ProviderDependencies) (*MetadataStorePostgres, plugin.Instance, error) {
			store, err := NewWithOptions(WithHost(cfg.Host), WithPort(cfg.Port), WithUser(cfg.User), WithPassword(cfg.Password), WithDatabase(cfg.Database), WithSSLMode(cfg.SSLMode), WithTimeZone(cfg.TimeZone), WithDSN(cfg.DSN), WithStorageMode(deps.StorageMode), WithLogger(deps.Logger), WithPromRegistry(deps.PromRegistry))
			if err != nil {
				return nil, nil, err
			}
			return store, plugin.Lifecycle{
				StartFunc: func(ctx context.Context) error {
					if err := ctx.Err(); err != nil {
						return err
					}
					return store.Start() //nolint:contextcheck
				},
				StopFunc: func(context.Context) error { return store.Stop() },
			}, nil
		})
}
