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

package mesh

import (
	"context"
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

// newAdapterDatabase opens a real database backed by the default
// badger/sqlite providers in a temporary directory. The adapter is the
// only place where Mesh's dependency interface meets the storage
// layer's own numbering and lookup helpers, so it is exercised against
// real storage rather than a double.
func newAdapterDatabase(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })
	return db
}

func TestMeshDatabaseAdapterBlockByIndexUsesChainHeight(t *testing.T) {
	t.Parallel()

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
		block, err := meshDB.BlockByIndex(context.Background(), height)

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

// TestMeshDatabaseAdapterBlockByHash asserts hash lookups reach the
// blob store's hash index rather than the height index.
func TestMeshDatabaseAdapterBlockByHash(t *testing.T) {
	t.Parallel()

	db := newAdapterDatabase(t)
	meshDB := NewMeshDatabase(db)
	hash := testHash(0x5a)
	require.NoError(t, db.BlockCreate(models.Block{
		Hash:   hash,
		Number: 41,
		Slot:   410,
	}, nil))

	block, err := meshDB.BlockByHash(context.Background(), hash)

	require.NoError(t, err)
	require.Equal(t, hash, block.Hash)
	require.Equal(t, uint64(41), block.Number)

	_, err = meshDB.BlockByHash(context.Background(), testHash(0x5b))
	require.ErrorIs(t, err, models.ErrBlockNotFound)
}

// TestMeshDatabaseAdapterEmptyDatabaseLookups shares one read-only empty
// database across the lookups that must report not-found or empty results
// and honor a cancelled context.
func TestMeshDatabaseAdapterEmptyDatabaseLookups(t *testing.T) {
	t.Parallel()

	db := newAdapterDatabase(t)
	meshDB := NewMeshDatabase(db)

	t.Run("block by index not found", func(t *testing.T) {
		_, err := meshDB.BlockByIndex(context.Background(), 9999)
		require.ErrorIs(t, err, models.ErrBlockNotFound)
	})

	t.Run("block index overflow", func(t *testing.T) {
		_, err := meshDB.BlockByIndex(context.Background(), math.MaxUint64)
		require.ErrorIs(t, err, models.ErrBlockNotFound)
	})

	t.Run("transaction lookups", func(t *testing.T) {
		tx, err := meshDB.GetTransactionByHash(
			context.Background(), testHash(0x5c),
		)
		require.NoError(t, err)
		require.Nil(t, tx)

		txs, err := meshDB.GetTransactionsByBlockHash(
			context.Background(), testHash(0x5d),
		)
		require.NoError(t, err)
		require.Empty(t, txs)
	})

	t.Run("cancelled context", func(t *testing.T) {
		adapter := &meshDatabaseAdapter{db: db}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := adapter.BlockByHash(ctx, make([]byte, 32))
		require.ErrorIs(t, err, context.Canceled)
		_, err = adapter.BlockByIndex(ctx, 0)
		require.ErrorIs(t, err, context.Canceled)
		_, err = adapter.GetTransactionByHash(ctx, make([]byte, 32))
		require.ErrorIs(t, err, context.Canceled)
		_, err = adapter.GetTransactionsByBlockHash(ctx, make([]byte, 32))
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestMeshHeightWithBoundaryBlockStorageOffset(t *testing.T) {
	t.Parallel()
	db := newAdapterDatabase(t)
	for i, number := range []uint64{0, 0, 1, 2} {
		require.NoError(
			t,
			db.BlockCreate(
				models.Block{
					ID:     uint64(i) + 1,
					Number: number,
					Slot:   uint64(i) * 20,
					Hash:   testHash(byte(i)),
					Cbor:   []byte{byte(i)},
				},
				nil,
			),
		)
	}
	block, err := NewMeshDatabase(db).BlockByIndex(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), block.Number)
	require.Equal(t, testHash(3), block.Hash)
}
