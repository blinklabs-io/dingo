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
// indexed column (dingo#4464). api/blockfrost's NodeAdapter.Asset and
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
// idx_asset_amount and idx_asset_fingerprint no longer exist (dingo#4598,
// migration v21). Unlike dingo#4482's asset.name_hex, neither column was
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
