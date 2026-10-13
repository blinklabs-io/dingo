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

package sqlstore

import (
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// TestInsertUtxoModelCachesAssetInsert proves the conflict-tolerant asset
// insert is cached, ordinary inserts populate caller-visible asset IDs, and
// staged replay writes persist the relation without resolving transient IDs.
func TestInsertUtxoModelCachesAssetInsert(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	utxo := utxoForInsertCacheTest(10, 0, 2_000_000)
	utxo.Assets = []models.Asset{
		{
			Name:        []byte("token"),
			PolicyId:    bytes.Repeat([]byte{0xAA}, 28),
			Fingerprint: []byte("asset1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			Amount:      types.Uint64(42),
		},
	}
	insertUtxoInTxn(t, store, utxo, true)
	require.NotZero(t, utxo.Assets[0].ID)
	var firstAssetID uint
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT id FROM asset WHERE utxo_id = ? AND policy_id = ? AND name = ?`,
		utxo.ID, utxo.Assets[0].PolicyId, utxo.Assets[0].Name,
	).Scan(&firstAssetID))
	require.NotZero(t, firstAssetID)

	store.stmtMu.Lock()
	cachedBefore := store.stmts[importAssetQuery]
	store.stmtMu.Unlock()
	require.NotNil(
		t,
		cachedBefore,
		"expected importAssetQuery to be cached on SQLite",
	)

	utxo2 := utxoForInsertCacheTest(11, 0, 3_000_000)
	utxo2.Assets = []models.Asset{
		{
			Name:        []byte("token2"),
			PolicyId:    bytes.Repeat([]byte{0xBB}, 28),
			Fingerprint: []byte("asset1bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
			Amount:      types.Uint64(7),
		},
	}
	insertUtxoInTxn(t, store, utxo2, true)
	require.NotZero(t, utxo2.Assets[0].ID)
	var secondAssetID uint
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT id FROM asset WHERE utxo_id = ? AND policy_id = ? AND name = ?`,
		utxo2.ID, utxo2.Assets[0].PolicyId, utxo2.Assets[0].Name,
	).Scan(&secondAssetID))
	require.NotZero(t, secondAssetID)
	require.NotEqual(t, firstAssetID, secondAssetID)

	batched := utxoForInsertCacheTest(12, 0, 4_000_000)
	batched.Assets = []models.Asset{
		{
			Name:        []byte("token3"),
			PolicyId:    bytes.Repeat([]byte{0xDD}, 28),
			Fingerprint: []byte("asset1ddddddddddddddddddddddddddddddddddddddd"),
			Amount:      types.Uint64(9),
		},
	}
	var queued rowBatch
	_, err := store.insertUtxoModelCheckedWithRows(
		context.Background(), store.writeDB, batched, true, &queued,
	)
	require.NoError(t, err)
	require.Zero(t, batched.Assets[0].ID)
	require.NoError(t, queued.flush(
		context.Background(), store.writeDB, store.dialect.ParameterLimit(),
	))
	require.Zero(t, batched.Assets[0].ID)
	var batchedAssetID uint
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT id FROM asset WHERE utxo_id = ? AND policy_id = ? AND name = ?`,
		batched.ID, batched.Assets[0].PolicyId, batched.Assets[0].Name,
	).Scan(&batchedAssetID))
	require.NotZero(t, batchedAssetID)

	store.stmtMu.Lock()
	cachedAfter := store.stmts[importAssetQuery]
	store.stmtMu.Unlock()
	require.Same(
		t,
		cachedBefore,
		cachedAfter,
		"expected the same cached *sql.Stmt across independent write transactions",
	)
}
