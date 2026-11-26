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
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

// newReleasedTestDB opens an on-disk database as the source of the v0.94.2
// migration: a block store and the SQLite metadata store holding the ledger
// state the migration moves.
func newReleasedTestDB(t *testing.T) *Database {
	t.Helper()
	db, err := newTestDatabase(
		t, &Config{DataDir: t.TempDir(), Role: RoleReleased},
	)
	require.NoError(t, err)
	return db
}

// indexerTestDB is an IndexerDB composed as a node composes it: its own
// API-mode SQLite store, which also keeps its CBOR offsets, reading blocks
// from the ledger's block store. Blocks are written through ledger; the
// IndexerDB cannot write them.
type indexerTestDB struct {
	ledger  *Database
	indexer *Database
}

// newIndexerTestDB opens the ledger's database and an IndexerDB over its
// block store. The wrap function, when set, receives the IndexerDB's
// metadata store before it is injected.
func newIndexerTestDB(
	t *testing.T,
	wrap func(metadata.MetadataStore) metadata.MetadataStore,
) indexerTestDB {
	t.Helper()
	return newIndexerTestDBOver(t, wrap, nil)
}

// newIndexerTestDBOver is newIndexerTestDB with the IndexerDB reading the
// ledger's block store through wrapBlocks, when it is set.
func newIndexerTestDBOver(
	t *testing.T,
	wrap func(metadata.MetadataStore) metadata.MetadataStore,
	wrapBlocks func(blob.BlobStore) blob.BlobStore,
) indexerTestDB {
	t.Helper()
	ledger, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	dir := t.TempDir()
	seedMigratedMetadata(t, dir, types.StorageModeAPI)
	host := plugin.NewHost()
	require.NoError(t, sqlite.RegisterProvider(host))
	store, err := plugin.Resolve[metadata.MetadataStore](
		context.Background(), host,
		plugin.CapabilityStorageMetadata, "sqlite", nil,
		metadata.ProviderDependencies{
			DataDir:     dir,
			StorageMode: types.StorageModeAPI,
			Logger:      ledger.logger,
		},
	)
	if err != nil {
		_ = host.Stop(context.Background())
		require.NoError(t, err)
	}
	ledgerBlocks := ledger.Blob()
	if wrapBlocks != nil {
		ledgerBlocks = wrapBlocks(ledgerBlocks)
	}
	blocks, err := NewIndexerBlobStore(ledgerBlocks, store)
	require.NoError(t, err)
	if wrap != nil {
		store = wrap(store)
	}
	indexer, err := New(
		context.Background(),
		&Config{
			DataDir:     dir,
			Logger:      ledger.logger,
			Role:        RoleIndexer,
			StorageMode: types.StorageModeAPI,
		},
		Stores{Blob: blocks, Metadata: store},
	)
	if err != nil {
		_ = host.Stop(context.Background())
		require.NoError(t, err)
	}
	testDatabaseHosts.Store(indexer, host)
	// Registered after the ledger's cleanup, so it runs first: the IndexerDB
	// reads the block store the ledger's database closes.
	closing := indexer
	t.Cleanup(func() {
		target := closing
		closing = nil
		if closeErr := closeTestDatabase(target); closeErr != nil {
			t.Errorf("close test indexer database: %v", closeErr)
		}
	})
	return indexerTestDB{ledger: ledger, indexer: indexer}
}

// newIndexerTestDatabase returns the IndexerDB of newIndexerTestDB, in the
// shape of newTestDatabase.
func newIndexerTestDatabase(t *testing.T) (*Database, error) {
	t.Helper()
	return newIndexerTestDB(t, nil).indexer, nil
}

// openIndexerTestDB is openTestDB for an IndexerDB: an API-mode database
// whose SQL store holds the rows the facade reads and writes. It keeps a
// block store of its own, so a test can write the blocks those rows name.
func openIndexerTestDB(t *testing.T) *Database {
	t.Helper()
	return openTestDBWithStorageMode(t, types.StorageModeAPI)
}
