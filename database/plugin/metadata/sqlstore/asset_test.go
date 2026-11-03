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
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestGetAssetByPolicyAndNameRecoversExternalNameHex proves an asset written
// and read back still yields the exact hex-encoded name external API
// consumers publish, even though asset.name_hex is no longer a stored,
// indexed column. api/blockfrost's NodeAdapter.Asset and
// api/mesh's appendUtxoOps both compute hex.EncodeToString(name) on demand;
// this pins the one fact that computation depends on -- Name must still
// round-trip through the store unchanged -- against the asset row
// GetAssetByPolicyAndName itself returns.
func TestGetAssetByPolicyAndNameRecoversExternalNameHex(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	policyID := bytes.Repeat([]byte{0x77}, 28)
	assetName := []byte("asset")
	utxo := utxoForInsertCacheTest(30, 0, 1_000_000)
	utxo.Address = []byte("test-address")
	utxo.Assets = []models.Asset{
		{
			Name:        assetName,
			PolicyId:    policyID,
			Fingerprint: []byte("fingerprint"),
			Amount:      types.Uint64(9),
		},
	}
	insertUtxoInTxn(t, store, utxo, true)
	require.NotZero(t, utxo.Assets[0].ID)

	got, err := store.GetAssetByPolicyAndName(
		lcommon.NewBlake2b224(policyID), assetName, nil,
	)
	require.NoError(t, err)
	require.NotZero(t, got.ID)
	require.Equal(t, assetName, got.Name)
	require.Equal(t, "6173736574", hex.EncodeToString(got.Name))
}

// TestAssetAmountAndFingerprintReadCorrectlyWithoutIndexes proves an asset's
// amount and fingerprint still round-trip correctly through the store once
// idx_asset_amount and idx_asset_fingerprint no longer exist (migration
// v21). Unlike the dropped asset.name_hex, neither column was
// dropped: both are genuinely read and returned via the blockfrost/mesh API
// adapters, so only the dead indexes are gone -- this pins that the values
// themselves are unaffected. store comes from newMigratedSQLiteStore, which
// runs the real SQLiteRegistry migrations rather than a hand-rolled CREATE
// TABLE, so the indexes under test are actually absent.
func TestAssetAmountAndFingerprintReadCorrectlyWithoutIndexes(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	policyID := bytes.Repeat([]byte{0x42}, 28)
	assetName := []byte("dead-index-asset")
	fingerprint := []byte("asset1deadindexfingerprint00")
	const wantAmount = uint64(123_456_789)

	utxo := utxoForInsertCacheTest(31, 0, 2_000_000)
	utxo.Address = []byte("test-address")
	utxo.Assets = []models.Asset{
		{
			Name:        assetName,
			PolicyId:    policyID,
			Fingerprint: fingerprint,
			Amount:      types.Uint64(wantAmount),
		},
	}
	insertUtxoInTxn(t, store, utxo, true)
	require.NotZero(t, utxo.Assets[0].ID)

	got, err := store.GetAssetByPolicyAndName(
		lcommon.NewBlake2b224(policyID), assetName, nil,
	)
	require.NoError(t, err)
	require.NotZero(t, got.ID)
	require.Equal(t, fingerprint, got.Fingerprint)
	require.Equal(t, wantAmount, uint64(got.Amount))

	quantity, err := store.GetAssetQuantityByPolicyAndName(
		lcommon.NewBlake2b224(policyID), assetName, nil,
	)
	require.NoError(t, err)
	require.Equal(t, wantAmount, quantity)
}

// insertAssetHolding stores a UTxO that holds amount of the asset. A
// deletedSlot above zero makes it a spent holding.
func insertAssetHolding(
	t *testing.T,
	store *Store,
	seed byte,
	policyID, assetName []byte,
	amount uint64,
	deletedSlot uint64,
) {
	t.Helper()
	utxo := utxoForInsertCacheTest(seed, 0, 1_000_000)
	utxo.Address = []byte("test-address")
	utxo.DeletedSlot = deletedSlot
	utxo.Assets = []models.Asset{{
		Name:        assetName,
		PolicyId:    policyID,
		Fingerprint: []byte("fingerprint"),
		Amount:      types.Uint64(amount),
	}}
	insertUtxoInTxn(t, store, utxo, true)
}

// TestGetAssetQuantityByPolicyAndNameSumsLiveHoldings covers the quantity
// being totalled by the database: every live holding counts, a spent one and
// another asset's do not, and an asset nobody holds totals zero.
func TestGetAssetQuantityByPolicyAndNameSumsLiveHoldings(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	policyID := bytes.Repeat([]byte{0x51}, 28)
	assetName := []byte("summed")
	other := []byte("other")
	policy := lcommon.NewBlake2b224(policyID)

	zero, err := store.GetAssetQuantityByPolicyAndName(policy, assetName, nil)
	require.NoError(t, err)
	require.Zero(t, zero)

	insertAssetHolding(t, store, 40, policyID, assetName, 10, 0)
	insertAssetHolding(t, store, 41, policyID, assetName, 25, 0)
	insertAssetHolding(t, store, 42, policyID, assetName, 7, 0)
	insertAssetHolding(t, store, 43, policyID, assetName, 1000, 9)
	insertAssetHolding(t, store, 44, policyID, other, 500, 0)

	got, err := store.GetAssetQuantityByPolicyAndName(policy, assetName, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(42), got)
}

// TestGetAssetQuantityByPolicyAndNameRejectsOverflow covers a total past 64
// bits, which must fail rather than wrap.
func TestGetAssetQuantityByPolicyAndNameRejectsOverflow(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	policyID := bytes.Repeat([]byte{0x52}, 28)
	assetName := []byte("overflow")
	insertAssetHolding(t, store, 50, policyID, assetName, 1<<63, 0)
	insertAssetHolding(t, store, 51, policyID, assetName, 1<<63, 0)

	_, err := store.GetAssetQuantityByPolicyAndName(
		lcommon.NewBlake2b224(policyID), assetName, nil,
	)
	require.ErrorContains(t, err, "overflow")
}

// TestGetAssetQuantityByPolicyAndNameStopsWithItsContext covers the scan
// running under the caller's transaction: once that context is done the query
// fails instead of running to completion.
func TestGetAssetQuantityByPolicyAndNameStopsWithItsContext(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	policyID := bytes.Repeat([]byte{0x53}, 28)
	assetName := []byte("canceled")
	insertAssetHolding(t, store, 60, policyID, assetName, 5, 0)

	ctx, cancel := context.WithCancel(t.Context())
	txn := store.ReadTransaction(ctx)
	defer txn.Rollback() //nolint:errcheck
	cancel()
	_, err := store.GetAssetQuantityByPolicyAndName(
		lcommon.NewBlake2b224(policyID), assetName, txn,
	)
	require.ErrorIs(t, err, context.Canceled)
}

// TestAssetQuantityStopsAtItsHoldingCap covers the scan bound: an asset held
// by exactly the cap still totals, and one holding more fails with the query
// budget error instead of reading the rest.
func TestAssetQuantityStopsAtItsHoldingCap(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	policyID := bytes.Repeat([]byte{0x54}, 28)
	assetName := []byte("capped")
	policy := lcommon.NewBlake2b224(policyID)
	for i := range 3 {
		insertAssetHolding(t, store, byte(70+i), policyID, assetName, 4, 0)
	}

	got, err := store.assetQuantity(policy, assetName, 3, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(12), got)

	_, err = store.assetQuantity(policy, assetName, 2, nil)
	require.ErrorIs(t, err, models.ErrUtxoQueryBudgetExceeded)
}
