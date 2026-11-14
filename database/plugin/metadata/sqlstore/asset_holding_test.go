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
	"database/sql"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func countRows(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRow(query, args...).Scan(&count))
	return count
}

// TestAssetIdentityStoredOncePerAsset writes the parity scenario through every
// write path and requires exactly one identity per distinct asset and one
// holding per (UTxO, asset), however many UTxOs hold the asset.
func TestAssetIdentityStoredOncePerAsset(t *testing.T) {
	t.Parallel()
	const distinctAssets, holdings = 5, 9
	for _, path := range parityPaths() {
		t.Run(path.name, func(t *testing.T) {
			t.Parallel()
			store := newMigratedSQLiteStore(t)
			path.apply(t, store, false)
			require.Equal(t, distinctAssets, countRows(t, store.writeDB,
				`SELECT COUNT(*) FROM asset_identity`))
			require.Equal(t, distinctAssets, countRows(t, store.writeDB,
				`SELECT COUNT(DISTINCT policy_id || x'ff' || name)
FROM asset_identity`))
			require.Equal(t, holdings, countRows(t, store.writeDB,
				`SELECT COUNT(*) FROM asset_holding`))
			require.Equal(t, 4, countRows(t, store.writeDB,
				`SELECT COUNT(*) FROM asset_holding h
JOIN asset_identity i ON i.id = h.asset_id
WHERE i.policy_id = ? AND i.name = ?`,
				parityPolicy(1), []byte("tok"),
			), "tok is held by four UTxOs and has one identity")
		})
	}
}

// TestNilAssetNameStoredAsEmpty requires a nil name and an empty name to be
// one asset, stored as an empty blob rather than NULL, on every write path.
func TestNilAssetNameStoredAsEmpty(t *testing.T) {
	t.Parallel()
	policy := bytes.Repeat([]byte{0x61}, 28)
	assets := func(name []byte) []models.Asset {
		return []models.Asset{{PolicyId: policy, Name: name, Amount: 3}}
	}
	for _, write := range []struct {
		name string
		do   func(t *testing.T, store *Store, seed byte, name []byte)
	}{
		{"per-row", func(t *testing.T, store *Store, seed byte, name []byte) {
			utxo := utxoForInsertCacheTest(seed, 0, 1_000_000)
			utxo.Address = []byte("test-address")
			utxo.Assets = assets(name)
			insertUtxoInTxn(t, store, utxo, true)
		}},
		{"import", func(t *testing.T, store *Store, seed byte, name []byte) {
			utxo := utxoForInsertCacheTest(seed, 0, 1_000_000)
			utxo.Address = []byte("test-address")
			utxo.Assets = assets(name)
			require.NoError(t, store.ImportUtxos([]models.Utxo{*utxo}, nil))
		}},
		{"batched", func(t *testing.T, store *Store, seed byte, name []byte) {
			utxo := utxoForInsertCacheTest(seed, 0, 1_000_000)
			utxo.Address = []byte("test-address")
			utxo.Assets = assets(name)
			var queued rowBatch
			_, err := store.insertUtxoModelCheckedWithRows(
				t.Context(), store.writeDB, utxo, true, &queued,
			)
			require.NoError(t, err)
			require.NoError(t, queued.flush(
				t.Context(), store.writeDB, store.dialect.ParameterLimit(),
			))
		}},
	} {
		t.Run(write.name, func(t *testing.T) {
			t.Parallel()
			store := newMigratedSQLiteStore(t)
			write.do(t, store, 1, nil)
			write.do(t, store, 2, []byte{})
			require.Equal(t, 1, countRows(t, store.writeDB,
				`SELECT COUNT(*) FROM asset_identity`))
			require.Equal(t, 1, countRows(t, store.writeDB,
				`SELECT COUNT(*) FROM asset_identity
WHERE typeof(name) = 'blob' AND length(name) = 0`))
			require.Equal(t, 2, countRows(t, store.writeDB,
				`SELECT COUNT(*) FROM asset_holding`))
			quantity, err := store.GetAssetQuantityByPolicyAndName(
				lcommon.NewBlake2b224(policy), nil, nil,
			)
			require.NoError(t, err)
			require.Equal(t, uint64(6), quantity,
				"a nil name looks up the empty-named asset")
		})
	}
}

// TestIdentityWithoutHoldingsIsInvisible deletes the only UTxO holding an
// asset. Its holdings cascade away and the identity stays, so every read must
// treat the asset as absent.
func TestIdentityWithoutHoldingsIsInvisible(t *testing.T) {
	t.Parallel()
	store := newForeignKeySQLiteStore(t)
	policy := bytes.Repeat([]byte{0x62}, 28)
	name := []byte("gone")
	hash := lcommon.NewBlake2b224(policy)
	utxo := utxoForInsertCacheTest(5, 0, 1_000_000)
	utxo.Address = []byte("test-address")
	utxo.Assets = []models.Asset{{PolicyId: policy, Name: name, Amount: 4}}
	insertUtxoInTxn(t, store, utxo, true)

	held, err := store.AssetHeld(hash, name, nil)
	require.NoError(t, err)
	require.True(t, held)

	_, err = store.writeDB.Exec(`DELETE FROM utxo`)
	require.NoError(t, err)
	require.Zero(t, countRows(t, store.writeDB,
		`SELECT COUNT(*) FROM asset_holding`), "holdings cascade with the UTxO")
	require.Equal(t, 1, countRows(t, store.writeDB,
		`SELECT COUNT(*) FROM asset_identity`), "identities outlive holdings")

	held, err = store.AssetHeld(hash, name, nil)
	require.NoError(t, err)
	require.False(t, held)
	quantity, err := store.GetAssetQuantityByPolicyAndName(hash, name, nil)
	require.NoError(t, err)
	require.Zero(t, quantity)
	byAsset, err := store.GetUtxosByAssets(policy, name, nil)
	require.NoError(t, err)
	require.Empty(t, byAsset)
	holders, total, err := store.GetAssetHolders(models.AssetHolderQuery{
		PolicyID: policy, AssetName: name, Limit: 10,
	}, nil)
	require.NoError(t, err)
	require.Empty(t, holders)
	require.Zero(t, total)
	transactions, err := store.GetAssetTransactions(
		hash, name, nil, nil, 0, 0, false, nil,
	)
	require.NoError(t, err)
	require.Empty(t, transactions)
}

// TestRollbackRemovesHoldingsKeepsIdentity rolls the chain back past every
// UTxO the scenario created and requires the holdings gone and identities
// kept: identity ids are never reused, so a rollback never needs to undo one.
func TestRollbackRemovesHoldingsKeepsIdentity(t *testing.T) {
	t.Parallel()
	store := newForeignKeySQLiteStore(t)
	for _, path := range parityPaths() {
		if path.name == "per-row" {
			path.apply(t, store, false)
		}
	}
	require.Positive(t, countRows(t, store.writeDB,
		`SELECT COUNT(*) FROM asset_holding`))
	identities := countRows(t, store.writeDB,
		`SELECT COUNT(*) FROM asset_identity`)
	require.NoError(t, store.DeleteUtxosAfterSlot(0, nil))
	require.Zero(t, countRows(t, store.writeDB,
		`SELECT COUNT(*) FROM asset_holding`))
	require.Equal(t, identities, countRows(t, store.writeDB,
		`SELECT COUNT(*) FROM asset_identity`))

	// A replay after the rollback reuses the identities.
	for _, path := range parityPaths() {
		if path.name == "per-row" {
			path.apply(t, store, false)
		}
	}
	require.Equal(t, identities, countRows(t, store.writeDB,
		`SELECT COUNT(*) FROM asset_identity`))
}

// TestImportReplayAddsNoDuplicateHoldings imports the same UTxOs twice, as a
// resumed snapshot import does, and requires no new holding or identity.
func TestImportReplayAddsNoDuplicateHoldings(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	utxos := parityModels(t, parityAddresses(t), parityStages())
	require.NoError(t, store.ImportUtxos(utxos, nil))
	holdings := countRows(
		t,
		store.writeDB,
		`SELECT COUNT(*) FROM asset_holding`,
	)
	identities := countRows(
		t,
		store.writeDB,
		`SELECT COUNT(*) FROM asset_identity`,
	)
	require.NoError(t, store.ImportUtxos(utxos, nil))
	require.Equal(t, holdings, countRows(t, store.writeDB,
		`SELECT COUNT(*) FROM asset_holding`))
	require.Equal(t, identities, countRows(t, store.writeDB,
		`SELECT COUNT(*) FROM asset_identity`))
}

// TestAssetHoldingAmountsRoundTrip stores quantities at the edges of the
// unsigned 64-bit range verbatim as decimal text.
func TestAssetHoldingAmountsRoundTrip(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	policy := bytes.Repeat([]byte{0x63}, 28)
	amounts := []uint64{1, 2, 1<<63 - 1, 1 << 63, 1<<64 - 1}
	for i, amount := range amounts {
		insertAssetHolding(t, store, byte(80+i), policy,
			[]byte(fmt.Sprintf("a%d", i)), amount, 0)
	}
	for i, amount := range amounts {
		var stored string
		require.NoError(t, store.writeDB.QueryRow(`
SELECT h.amount FROM asset_holding h
JOIN asset_identity i ON i.id = h.asset_id WHERE i.name = ?`,
			[]byte(fmt.Sprintf("a%d", i)),
		).Scan(&stored))
		require.Equal(t, fmt.Sprint(amount), stored)
		got, err := store.GetAssetQuantityByPolicyAndName(
			lcommon.NewBlake2b224(policy), []byte(fmt.Sprintf("a%d", i)), nil,
		)
		require.NoError(t, err)
		require.Equal(t, amount, got)
	}
}
