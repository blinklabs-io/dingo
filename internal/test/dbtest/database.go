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
	"errors"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/plugin"
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

// Options configures a test database. The zero value composes the badger blob
// store and the sqlite metadata store with an unset run mode, matching
// NewDatabase.
type Options struct {
	Config         *database.Config
	Blob           StorageProvider
	Metadata       StorageProvider
	RunMode        string
	MaxConnections int
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
	metadataStore, err := plugin.Resolve[metadata.MetadataStore](
		context.Background(), host,
		plugin.CapabilityStorageMetadata, metadataName, opts.Metadata.Config,
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
