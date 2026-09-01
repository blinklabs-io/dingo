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

// Package dbtest composes storage providers for tests that need a real
// database without putting provider construction back into package database.
package dbtest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/plugin"
	_ "github.com/glebarez/go-sqlite"
)

var databaseHosts sync.Map

// StorageProvider selects a storage provider by name, supplies its
// provider-specific config, and names the registrar that installs it on the
// test host. A zero value selects the always-built default for its capability
// (badger for blob, sqlite for metadata). Register must be set for any
// non-default provider, such as the tag-gated cloud blob stores.
type StorageProvider struct {
	Name     string
	Config   map[string]any
	Register func(*plugin.Host) error
}

// RawSQLiteMetadata opens a repository-internal database/sql fixture against
// the metadata file owned by db. It exists only for tests that must seed a
// deliberately impossible or partially-upgraded state that the public store
// contract cannot represent. Production code must use MetadataStore methods.
func RawSQLiteMetadata(
	tb testing.TB,
	db *database.Database,
) (*sql.DB, error) {
	tb.Helper()
	if db == nil {
		return nil, errors.New("nil test database")
	}
	if db.DataDir() == "" {
		return nil, errors.New(
			"raw SQLite fixture requires a file-backed test database",
		)
	}
	path := filepath.Join(db.DataDir(), "metadata.sqlite")
	raw, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:%s?_pragma=busy_timeout(30000)&_pragma=foreign_keys(1)",
			path,
		),
	)
	if err != nil {
		return nil, err
	}
	if err := raw.Ping(); err != nil {
		_ = raw.Close()
		return nil, err
	}
	tb.Cleanup(func() {
		if err := raw.Close(); err != nil {
			tb.Errorf("close raw SQLite metadata fixture: %v", err)
		}
	})
	return raw, nil
}

// Options configures a test database. The zero value composes the badger blob
// store and the sqlite metadata store with an unset run mode, matching
// NewDatabase.
type Options struct {
	Config         *database.Config
	Blob           StorageProvider
	Metadata       StorageProvider
	RunMode        string
	MaxConnections int

	// InMemoryMetadata opts out of the migrated SQLite template (see
	// template.go) and builds the metadata store the old way: a fresh
	// shared-cache in-memory database with the full migration run against
	// it. That costs ~1.7s per call under -race, so use it only for a test
	// that genuinely depends on in-memory semantics rather than on a
	// file-backed database -- WAL behavior, locking, or the absence of a
	// data directory. Ignored unless the metadata provider is sqlite and
	// the caller supplied no Config of its own.
	InMemoryMetadata bool
}

// NewDatabase composes Badger and SQLite for a test. The database is closed
// before its provider host during the cleanup registered with tb.
func NewDatabase(
	tb testing.TB,
	config *database.Config,
) (*database.Database, error) {
	tb.Helper()
	return NewDatabaseWithOptions(tb, Options{Config: config})
}

// NewDatabaseWithOptions composes the selected storage providers for a test.
// It registers the chosen providers on a fresh host, resolves them with the
// supplied dependencies (including run mode and metadata connection pool
// size), and injects the resulting stores into a database. The database is
// closed before its provider host during the cleanup registered with tb.
func NewDatabaseWithOptions(
	tb testing.TB,
	opts Options,
) (*database.Database, error) {
	tb.Helper()
	config := opts.Config
	if config == nil {
		config = database.DefaultConfig
	}
	blobName := opts.Blob.Name
	if blobName == "" {
		blobName = "badger"
	}
	blobRegister := opts.Blob.Register
	if blobRegister == nil {
		blobRegister = badger.RegisterProvider
	}
	metadataName := opts.Metadata.Name
	if metadataName == "" {
		metadataName = "sqlite"
	}
	metadataRegister := opts.Metadata.Register
	if metadataRegister == nil {
		metadataRegister = sqlite.RegisterProvider
	}
	host := plugin.NewHost()
	if err := blobRegister(host); err != nil {
		return nil, err
	}
	if err := metadataRegister(host); err != nil {
		return nil, err
	}
	blobStore, err := plugin.Resolve[blob.BlobStore](
		context.Background(), host,
		plugin.CapabilityStorageBlob, blobName, opts.Blob.Config,
		blob.ProviderDependencies{
			DataDir: config.DataDir, RunMode: opts.RunMode,
			StorageMode: config.StorageMode,
			Logger:      config.Logger, PromRegistry: config.PromRegistry,
		},
	)
	if err != nil {
		_ = host.Stop(context.Background())
		return nil, err
	}
	metadataConfig := opts.Metadata.Config
	if metadataConfig == nil &&
		metadataName == "sqlite" &&
		!opts.InMemoryMetadata {
		// Seed a copy of the process's migrated template so the provider's
		// migration runner finds an already-migrated database instead of
		// recreating 84 tables (see template.go).
		//
		// Where it is seeded matters. When the caller supplied a DataDir,
		// the template goes *into that directory* and the provider config
		// is left alone, because callers rely on the metadata file living
		// at db.DataDir()/metadata.sqlite -- RawSQLiteMetadata opens
		// exactly that path, and several ledger helpers seed fixtures
		// through it. Redirecting the provider elsewhere in that case
		// leaves the raw handle pointing at a different file that SQLite
		// creates empty on open, which fails as "no such table".
		//
		// Only when the caller supplied no DataDir (previously an
		// in-memory store, which RawSQLiteMetadata already refuses) is the
		// provider pointed at a directory of our own.
		if config.DataDir != "" {
			if err := seedMetadataTemplate(config.DataDir); err != nil {
				_ = host.Stop(context.Background())
				return nil, err
			}
		} else {
			metadataConfig, err = writeMetadataTemplate(tb.TempDir())
			if err != nil {
				_ = host.Stop(context.Background())
				return nil, err
			}
		}
	}
	metadataStore, err := plugin.Resolve[metadata.MetadataStore](
		context.Background(), host,
		plugin.CapabilityStorageMetadata, metadataName, metadataConfig,
		metadata.ProviderDependencies{
			DataDir: config.DataDir, StorageMode: config.StorageMode,
			MaxConnections: opts.MaxConnections,
			Logger:         config.Logger, PromRegistry: config.PromRegistry,
		},
	)
	if err != nil {
		_ = host.Stop(context.Background())
		return nil, err
	}
	db, err := database.New(
		config,
		database.Stores{Blob: blobStore, Metadata: metadataStore},
	)
	if db == nil {
		_ = host.Stop(context.Background())
		return nil, err
	}
	databaseHosts.Store(db, host)
	tb.Cleanup(func() {
		if closeErr := CloseDatabase(db); closeErr != nil {
			tb.Errorf("close test database runtime: %v", closeErr)
		}
	})
	return db, err
}

// CloseDatabase closes a test database and its provider host in dependency
// order. Use it when a test must close a database before its cleanup phase.
func CloseDatabase(db *database.Database) error {
	if db == nil {
		return nil
	}
	err := db.Close()
	if hostValue, ok := databaseHosts.LoadAndDelete(db); ok {
		err = errors.Join(
			err,
			hostValue.(*plugin.Host).Stop(context.Background()),
		)
	}
	return err
}
