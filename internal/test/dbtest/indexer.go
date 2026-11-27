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

package dbtest

import (
	"context"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/plugin"
)

// IndexerOptions configures NewIndexerDatabase. The zero value is a plain
// SQLite metadata store.
type IndexerOptions struct {
	// MaxConnections bounds the metadata store's connection pool; zero
	// keeps the provider default.
	MaxConnections int
	// WrapMetadata, when set, receives the metadata store before it is
	// injected into the database.
	WrapMetadata func(metadata.MetadataStore) metadata.MetadataStore
}

// NewIndexerDatabase composes an IndexerDB over consensus for a test, as a
// node does: its own API-mode SQLite metadata store, in a directory of its
// own so RawSQLiteMetadata reaches it, which also keeps its UTxO and
// transaction CBOR entries, reading blocks from the consensus database's blob
// store.
//
// Call it after consensus is built: the IndexerDB is closed during tb's
// cleanup, which must run before the consensus database closes the blob store
// it reads.
func NewIndexerDatabase(
	tb testing.TB,
	consensus *database.Database,
	opts IndexerOptions,
) (*database.Database, error) {
	tb.Helper()
	if consensus == nil {
		return nil, errors.New("nil consensus database")
	}
	dir := tb.TempDir()
	if err := seedMetadataTemplate(dir); err != nil {
		return nil, err
	}
	host := plugin.NewHost()
	if err := sqlite.RegisterProvider(host); err != nil {
		return nil, err
	}
	consensusConfig := consensus.Config()
	store, err := plugin.Resolve[metadata.MetadataStore](
		context.Background(), host,
		plugin.CapabilityStorageMetadata, "sqlite", nil,
		metadata.ProviderDependencies{
			DataDir:        dir,
			StorageMode:    types.StorageModeAPI,
			MaxConnections: opts.MaxConnections,
			Logger:         consensusConfig.Logger,
		},
	)
	if err != nil {
		_ = host.Stop(context.Background())
		return nil, err
	}
	// The offsets go to the provider's store itself: a wrapper that embeds
	// metadata.MetadataStore hides the optional capability.
	blocks, err := database.NewIndexerBlobStore(consensus.Blob(), store)
	if err != nil {
		_ = host.Stop(context.Background())
		return nil, err
	}
	if opts.WrapMetadata != nil {
		store = opts.WrapMetadata(store)
	}
	db, err := database.New(
		context.Background(),
		&database.Config{
			DataDir:     dir,
			Logger:      consensusConfig.Logger,
			Role:        database.RoleIndexer,
			StorageMode: types.StorageModeAPI,
			Network:     consensusConfig.Network,
			AlonzoLovelacePerUtxoWord: consensusConfig.
				AlonzoLovelacePerUtxoWord,
		},
		database.Stores{
			Blob:      blocks,
			Metadata:  store,
			LeiosPeer: consensus,
		},
	)
	if db == nil {
		_ = host.Stop(context.Background())
		return nil, err
	}
	databaseHosts.Store(db, host)
	tb.Cleanup(func() {
		if closeErr := CloseDatabase(db); closeErr != nil {
			tb.Errorf("close test indexer database: %v", closeErr)
		}
	})
	return db, err
}
