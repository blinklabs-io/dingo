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
	"errors"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/plugin"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type Config struct {
	Host                string        `yaml:"host"`
	Port                uint          `yaml:"port"`
	User                string        `yaml:"user"`
	Password            string        `yaml:"password"`
	Database            string        `yaml:"database"`
	SSLMode             string        `yaml:"sslMode"`
	TimeZone            string        `yaml:"timeZone"`
	DSN                 string        `yaml:"dsn"`
	PoolMaxOpenConns    int           `yaml:"poolMaxOpenConns"`
	PoolMaxIdleConns    int           `yaml:"poolMaxIdleConns"`
	PoolConnMaxLifetime time.Duration `yaml:"poolConnMaxLifetime"`
}

func RegisterProvider(host *plugin.Host) error {
	return plugin.Register(
		host,
		plugin.Descriptor{
			Capability:  plugin.CapabilityStorageMetadata,
			Name:        "postgres",
			Description: "PostgreSQL relational database",
		},
		func() Config {
			return Config{
				Host:     "localhost",
				Port:     5432,
				User:     "postgres",
				Database: "postgres",
				SSLMode:  "disable",
				TimeZone: "UTC",
			}
		},
		func(
			_ context.Context,
			cfg Config,
			deps metadata.ProviderDependencies,
		) (*sqlstore.Store, plugin.Instance, error) {
			store, err := openStore(cfg, deps)
			if err != nil {
				return nil, nil, err
			}
			return store, plugin.Lifecycle{
				StartFunc: store.Start,
				StopFunc: func(ctx context.Context) error {
					return store.CloseContext(ctx)
				},
			}, nil
		},
	)
}

func openStore(
	cfg Config,
	deps metadata.ProviderDependencies,
) (*sqlstore.Store, error) {
	if cfg.PoolMaxOpenConns < 0 ||
		cfg.PoolMaxIdleConns < 0 ||
		cfg.PoolConnMaxLifetime < 0 {
		return nil, errors.New("PostgreSQL pool limits must not be negative")
	}
	dsn := cfg.DSN
	if dsn == "" {
		connectionURL := &url.URL{
			Scheme: "postgres",
			User:   url.UserPassword(cfg.User, cfg.Password),
			Host: net.JoinHostPort(
				cfg.Host,
				strconv.FormatUint(uint64(cfg.Port), 10),
			),
			Path: cfg.Database,
		}
		query := connectionURL.Query()
		if cfg.SSLMode != "" {
			query.Set("sslmode", cfg.SSLMode)
		}
		if cfg.TimeZone != "" {
			query.Set("timezone", cfg.TimeZone)
		}
		connectionURL.RawQuery = query.Encode()
		dsn = connectionURL.String()
	}
	db, err := sqlstore.OpenDB("pgx", dsn, "postgresql")
	if err != nil {
		return nil, err
	}
	maxOpen := cfg.PoolMaxOpenConns
	if maxOpen == 0 {
		maxOpen = deps.MaxConnections
	}
	if maxOpen <= 0 {
		maxOpen = 100
	}
	maxIdle := cfg.PoolMaxIdleConns
	if maxIdle == 0 {
		maxIdle = 10
	}
	connMaxLifetime := cfg.PoolConnMaxLifetime
	if connMaxLifetime == 0 {
		connMaxLifetime = time.Hour
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(connMaxLifetime)
	registry, err := migrations.PostgresRegistry()
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	store, err := sqlstore.New(sqlstore.Config{
		WriteDB:     db,
		ReadDB:      db,
		Dialect:     sqlstore.PostgresDialect(),
		Logger:      deps.Logger,
		StorageMode: deps.StorageMode,
		Migrations:  registry,
		MigrationLocker: migrations.NewAdvisoryLocker(
			"postgres",
			0x64696e676f6d6574,
			30*time.Second,
		),
		BackupTo: func(ctx context.Context, dstPath string) error {
			return backupPostgres(ctx, dsn, dstPath)
		},
		RestoreFrom: func(ctx context.Context, srcPath string) error {
			return restorePostgres(ctx, dsn, srcPath)
		},
		Reset: func(ctx context.Context) error {
			return resetDatabase(ctx, db)
		},
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}
