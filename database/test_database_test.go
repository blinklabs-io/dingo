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
			Logger: config.Logger, PromRegistry: config.PromRegistry,
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
	db, err := New(config, Stores{Blob: blobStore, Metadata: metadataStore})
	if err != nil {
		_ = host.Stop(context.Background())
		return nil, err
	}
	testDatabaseHosts.Store(db, host)
	tb.Cleanup(func() {
		if closeErr := closeTestDatabase(db); closeErr != nil {
			tb.Errorf("close test database runtime: %v", closeErr)
		}
	})
	return db, err
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
