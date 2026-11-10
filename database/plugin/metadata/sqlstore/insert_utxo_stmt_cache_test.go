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

// TestInsertUtxoModelCachesAssetInsert proves the holding insert is cached,
// ordinary inserts store the holding, and staged replay writes persist the
// holding once flushed.
func TestInsertUtxoModelCachesAssetInsert(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	holdingAmount := func(utxoID uint, policy, name []byte) string {
		var amount string
		require.NoError(t, store.writeDB.QueryRow(`
SELECT h.amount FROM asset_holding h
JOIN asset_identity i ON i.id = h.asset_id
WHERE h.utxo_id = ? AND i.policy_id = ? AND i.name = ?`,
			utxoID, policy, name,
		).Scan(&amount))
		return amount
	}

	utxo := utxoForInsertCacheTest(10, 0, 2_000_000)
	utxo.Address = []byte("test-address")
	utxo.Assets = []models.Asset{
		{
			Name:     []byte("token"),
			PolicyId: bytes.Repeat([]byte{0xAA}, 28),
			Amount:   types.Uint64(42),
		},
	}
	insertUtxoInTxn(t, store, utxo, true)
	require.Equal(t, "42", holdingAmount(
		utxo.ID, utxo.Assets[0].PolicyId, utxo.Assets[0].Name,
	))

	store.stmtMu.Lock()
	cachedBefore := store.stmts[insertAssetHoldingQuery]
	identityBefore := store.stmts[insertAssetIdentityQuery]
	store.stmtMu.Unlock()
	require.NotNil(
		t,
		cachedBefore,
		"expected insertAssetHoldingQuery to be cached on SQLite",
	)
	require.NotNil(
		t,
		identityBefore,
		"expected insertAssetIdentityQuery to be cached on SQLite",
	)

	utxo2 := utxoForInsertCacheTest(11, 0, 3_000_000)
	utxo2.Address = []byte("test-address")
	utxo2.Assets = []models.Asset{
		{
			Name:     []byte("token2"),
			PolicyId: bytes.Repeat([]byte{0xBB}, 28),
			Amount:   types.Uint64(7),
		},
	}
	insertUtxoInTxn(t, store, utxo2, true)
	require.Equal(t, "7", holdingAmount(
		utxo2.ID, utxo2.Assets[0].PolicyId, utxo2.Assets[0].Name,
	))

	batched := utxoForInsertCacheTest(12, 0, 4_000_000)
	batched.Address = []byte("test-address")
	batched.Assets = []models.Asset{
		{
			Name:     []byte("token3"),
			PolicyId: bytes.Repeat([]byte{0xDD}, 28),
			Amount:   types.Uint64(9),
		},
	}
	var queued rowBatch
	_, err := store.insertUtxoModelCheckedWithRows(
		context.Background(), store.writeDB, batched, true, &queued,
	)
	require.NoError(t, err)
	var queuedHoldings int
	for _, entry := range queued.entries {
		if entry.shape.resolveAssets {
			queuedHoldings += len(entry.rows)
		}
	}
	require.Equal(t, 1, queuedHoldings,
		"the holding stays queued until the batch flushes")
	require.NoError(t, queued.flush(
		context.Background(), store.writeDB, store.dialect.ParameterLimit(),
	))
	require.Equal(t, "9", holdingAmount(
		batched.ID, batched.Assets[0].PolicyId, batched.Assets[0].Name,
	))

	store.stmtMu.Lock()
	cachedAfter := store.stmts[insertAssetHoldingQuery]
	store.stmtMu.Unlock()
	require.Same(
		t,
		cachedBefore,
		cachedAfter,
		"expected the same cached *sql.Stmt across independent write transactions",
	)
}
