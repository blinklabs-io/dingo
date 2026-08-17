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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package mesh

import (
	"context"
	"log/slog"
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

// newAdapterDatabase opens a real database backed by the default
// badger/sqlite providers in a temporary directory. The adapter is the
// only place where Mesh's dependency interface meets the storage
// layer's own numbering and lookup helpers, so it is exercised against
// real storage rather than a double.
func newAdapterDatabase(t *testing.T) *database.Database {
	t.Helper()
	dataDir := t.TempDir()
	logger := slog.New(
		slog.NewTextHandler(newDiscardWriter(), nil),
	)
	host := plugin.NewHost()
	require.NoError(t, badger.RegisterProvider(host))
	require.NoError(t, sqlite.RegisterProvider(host))

	blobStore, err := plugin.Resolve[blob.BlobStore](
		context.Background(),
		host,
		plugin.CapabilityStorageBlob,
		"badger",
		nil,
		blob.ProviderDependencies{
			DataDir: dataDir,
			Logger:  logger,
		},
	)
	require.NoError(t, err)
	metadataStore, err := plugin.Resolve[metadata.MetadataStore](
		context.Background(),
		host,
		plugin.CapabilityStorageMetadata,
		"sqlite",
		nil,
		metadata.ProviderDependencies{
			DataDir: dataDir,
			Logger:  logger,
		},
	)
	require.NoError(t, err)

	db, err := database.New(
		&database.Config{DataDir: dataDir, Logger: logger},
		database.Stores{
			Blob: blobStore, Metadata: metadataStore,
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, host.Stop(context.Background()))
	})
	return db
}

// TestMeshDatabaseAdapterBlockByIndexUsesChainHeight covers the block
// numbering contract between the Mesh API and storage. /block reports
// block_identifier.index as the Cardano block height, so feeding that
// index back into /block must return the same block. The blob store
// keys blocks by a 1-based storage index, so the adapter translates
// height to that index -- the same translation the Blockfrost adapter
// documents and applies.
func TestMeshDatabaseAdapterBlockByIndexUsesChainHeight(t *testing.T) {
	db := newAdapterDatabase(t)
	meshDB := NewMeshDatabase(db)

	// Heights 0, 1 and 2, stored in chain order as a from-genesis node
	// would, so storage indices run 1, 2, 3.
	for height := range uint64(3) {
		var prevHash []byte
		if height > 0 {
			prevHash = testHash(byte(height - 1))
		}
		require.NoError(t, db.BlockCreate(models.Block{
			Hash:     testHash(byte(height)),
			PrevHash: prevHash,
			Number:   height,
			Slot:     height * 20,
		}, nil))
	}

	for height := range uint64(3) {
		block, err := meshDB.BlockByIndex(height)

		require.NoError(t, err, "height %d", height)
		require.Equal(
			t, height, block.Number,
			"index %d resolved to height %d",
			height, block.Number,
		)
		require.Equal(
			t, testHash(byte(height)), block.Hash,
		)
	}
}

// TestMeshDatabaseAdapterBlockByIndexNotFound asserts an unknown height
// surfaces the shared not-found error, which the /block handler maps to
// the Mesh block-not-found code.
func TestMeshDatabaseAdapterBlockByIndexNotFound(t *testing.T) {
	db := newAdapterDatabase(t)
	meshDB := NewMeshDatabase(db)

	_, err := meshDB.BlockByIndex(9999)

	require.ErrorIs(t, err, models.ErrBlockNotFound)
}

// TestMeshDatabaseAdapterBlockByHash asserts hash lookups reach the
// blob store's hash index rather than the height index.
func TestMeshDatabaseAdapterBlockByHash(t *testing.T) {
	db := newAdapterDatabase(t)
	meshDB := NewMeshDatabase(db)
	hash := testHash(0x5a)
	require.NoError(t, db.BlockCreate(models.Block{
		Hash:   hash,
		Number: 41,
		Slot:   410,
	}, nil))

	block, err := meshDB.BlockByHash(hash)

	require.NoError(t, err)
	require.Equal(t, hash, block.Hash)
	require.Equal(t, uint64(41), block.Number)

	_, err = meshDB.BlockByHash(testHash(0x5b))
	require.ErrorIs(t, err, models.ErrBlockNotFound)
}

// TestMeshDatabaseAdapterTransactionLookups asserts the transaction
// methods reach the metadata store and report a miss as an empty
// result, which the handlers translate into transaction-not-found.
func TestMeshDatabaseAdapterTransactionLookups(t *testing.T) {
	db := newAdapterDatabase(t)
	meshDB := NewMeshDatabase(db)

	tx, err := meshDB.GetTransactionByHash(testHash(0x5c))
	require.NoError(t, err)
	require.Nil(t, tx)

	txs, err := meshDB.GetTransactionsByBlockHash(testHash(0x5d))
	require.NoError(t, err)
	require.Empty(t, txs)
}

// TestMeshDatabaseAdapterBlockIndexOverflow asserts a height that would
// overflow the storage index is reported as not found rather than
// wrapping around to a valid index.
func TestMeshDatabaseAdapterBlockIndexOverflow(t *testing.T) {
	db := newAdapterDatabase(t)
	meshDB := NewMeshDatabase(db)

	_, err := meshDB.BlockByIndex(math.MaxUint64)

	require.ErrorIs(t, err, models.ErrBlockNotFound)
}
