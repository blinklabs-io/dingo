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

package database

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/plugin"
)

var testDatabaseHosts sync.Map

func newTestDatabase(
	tb testing.TB,
	config *Config,
) (*Database, error) {
	tb.Helper()
	return newTestDatabaseWithHost(tb, config, false)
}

// newTestDatabaseWithRunMode builds a test database whose blob store is
// resolved with the given run mode. It exists because the badger plugin
// switches block metadata to a compact binary encoding for run mode
// "serve" or "leios" with storage mode "core", and nothing else in these
// tests reaches that encoding.
func newTestDatabaseWithRunMode(
	tb testing.TB,
	config *Config,
	runMode string,
) (*Database, error) {
	tb.Helper()
	return newTestDatabaseWithHostRunMode(tb, config, false, runMode)
}

// newTestDatabaseWithHost is the shared body behind newTestDatabase and
// openForRecoveryTest (database/node_settings_gates_test.go): register the
// badger and sqlite providers, resolve the blob and metadata stores from
// config, and call New. The two callers differ only in keepOnError: New can
// return a non-nil *Database alongside an error on a CommitTimestampError,
// since that database is available for recovery rather than closed.
// newTestDatabase passes false -- none of its many callers need the failed
// handle, so it is closed (via a host stop) and discarded like any other
// open error. openForRecoveryTest passes true, matching node.go's
// dbNeedsRecovery path, which specifically needs the *Database New still
// returns alongside the error.
func newTestDatabaseWithHost(
	tb testing.TB,
	config *Config,
	keepOnError bool,
) (*Database, error) {
	tb.Helper()
	return newTestDatabaseWithHostRunMode(tb, config, keepOnError, "")
}

func newTestDatabaseWithHostRunMode(
	tb testing.TB,
	config *Config,
	keepOnError bool,
	runMode string,
) (*Database, error) {
	tb.Helper()
	if config == nil {
		config = DefaultConfig
	}
	host := plugin.NewHost()
	if err := badger.RegisterProvider(host); err != nil {
		return nil, err
	}
	if err := sqlite.RegisterProvider(host); err != nil {
		return nil, err
	}
	blobStore, err := plugin.Resolve[blob.BlobStore](
		context.Background(), host,
		plugin.CapabilityStorageBlob, "badger", nil,
		blob.ProviderDependencies{
			DataDir: config.DataDir, StorageMode: config.StorageMode,
			RunMode: runMode,
			Logger:  config.Logger, PromRegistry: config.PromRegistry,
		},
	)
	if err != nil {
		return nil, err
	}
	metadataStore, err := plugin.Resolve[metadata.MetadataStore](
		context.Background(), host,
		plugin.CapabilityStorageMetadata, "sqlite", nil,
		metadata.ProviderDependencies{
			DataDir: config.DataDir, StorageMode: config.StorageMode,
			Logger: config.Logger, PromRegistry: config.PromRegistry,
		},
	)
	if err != nil {
		_ = host.Stop(context.Background())
		return nil, err
	}
	db, dbErr := New(config, Stores{Blob: blobStore, Metadata: metadataStore})
	if db == nil || (dbErr != nil && !keepOnError) {
		_ = host.Stop(context.Background())
		return nil, dbErr
	}
	testDatabaseHosts.Store(db, host)
	tb.Cleanup(func() {
		if closeErr := closeTestDatabase(db); closeErr != nil {
			tb.Errorf("close test database runtime: %v", closeErr)
		}
	})
	return db, dbErr
}

func closeTestDatabase(db *Database) error {
	if db == nil {
		return nil
	}
	err := db.Close()
	if hostValue, ok := testDatabaseHosts.LoadAndDelete(db); ok {
		err = errors.Join(
			err,
			hostValue.(*plugin.Host).Stop(context.Background()),
		)
	}
	return err
}
