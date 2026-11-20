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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// A database written before assets were stored in canonical order holds rows
// in arbitrary insertion order. Reads must not depend on it.
func TestUtxoAssetsReadInCanonicalOrderRegardlessOfInsertion(t *testing.T) {
	t.Parallel()
	store, db := newManagementTestStoreWithRawDB(t)
	store.storageMode = types.StorageModeAPI

	var p1, p2 lcommon.Blake2b224
	p1[0], p2[0] = 0x01, 0x02
	ma := lcommon.NewMultiAsset(
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeOutput{
			p1: {cbor.NewByteString([]byte("a")): big.NewInt(1)},
			p2: {cbor.NewByteString([]byte("a")): big.NewInt(2)},
		},
	)
	utxo := ledger.Utxo{
		Id: shelley.MustNewShelleyTransactionInput(
			"0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
			0,
		),
		Output: &mary.MaryTransactionOutput{
			OutputAmount: mary.MaryTransactionOutputValue{
				Amount: 1_000_000,
				Assets: &ma,
			},
		},
	}
	require.NoError(
		t,
		store.AddUtxos([]models.UtxoSlot{{Utxo: utxo, Slot: 1}}, nil),
	)
	// Move the policy 1 row after the policy 2 row in insertion order.
	_, err := db.Exec(
		`UPDATE asset SET id = id + 1000 WHERE policy_id = ?`,
		p1.Bytes(),
	)
	require.NoError(t, err)

	got, err := store.GetUtxosByAddressWithOrdering(
		&models.UtxoWithOrderingQuery{MatchAllAddresses: true},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Len(t, got[0].Assets, 2)
	require.Equal(t, p1.Bytes(), got[0].Assets[0].PolicyId)
	require.Equal(t, p2.Bytes(), got[0].Assets[1].PolicyId)
}

// Mint rows of one transaction may sit in arbitrary insertion order; assets
// first minted by the same transaction list in policy then name order.
func TestListMintedAssetsBreaksTiesCanonically(t *testing.T) {
	t.Parallel()
	store, db := newManagementTestStoreWithRawDB(t)
	p1, p2 := assetPolicy(0x01), assetPolicy(0x02)
	insertMintBurn(t, db, credentialHash(0x21), p2, "a", 10, 0, "1")
	insertMintBurn(t, db, credentialHash(0x21), p1, "b", 10, 0, "1")
	insertMintBurn(t, db, credentialHash(0x21), p1, "a", 10, 0, "1")
	insertMintBurn(t, db, credentialHash(0x22), p1, "c", 11, 0, "1")

	keys := func(assets []models.MintedAsset) []string {
		out := make([]string, 0, len(assets))
		for _, asset := range assets {
			out = append(out, string(asset.PolicyId[:1])+string(asset.Name))
		}
		return out
	}
	all, err := store.ListMintedAssets(nil, 0, 0, false, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"\x01a", "\x01b", "\x02a", "\x01c"}, keys(all))

	desc, err := store.ListMintedAssets(nil, 0, 0, true, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"\x01c", "\x02a", "\x01b", "\x01a"}, keys(desc))
}

// The first event is compared as a (slot, transaction index) pair. Folding the
// pair into one integer overflows past slot 2^31, which SQLite turns into a
// float that loses the transaction index and PostgreSQL and MySQL reject.
func TestListMintedAssetsOrdersFirstEventPastSlotFoldLimit(t *testing.T) {
	t.Parallel()
	store, _ := newManagementTestStoreWithRawDB(t)
	testListMintedAssetsPastSlotFoldLimit(t, store)
}

func testListMintedAssetsPastSlotFoldLimit(t *testing.T, store *Store) {
	t.Helper()
	const slot = uint64(1) << 31
	p1, p2 := assetPolicy(0x01), assetPolicy(0x02)
	insert := func(txHash, policy []byte, slot uint64, txIndex uint32) {
		_, err := store.writeDB.Exec(
			store.dialect.Rebind(`INSERT INTO asset_mint_burn `+
				`(tx_hash, policy_id, name, fingerprint, slot, quantity, tx_index) `+
				`VALUES (?, ?, ?, ?, ?, ?, ?)`),
			txHash, policy, []byte("a"), []byte("fp"), slot, "1", txIndex,
		)
		require.NoError(t, err)
	}
	insert(credentialHash(0x31), p2, slot, 1)
	insert(credentialHash(0x32), p1, slot, 2)
	// A later event at a smaller transaction index must not move p1's asset.
	insert(credentialHash(0x33), p1, slot+1, 0)

	all, err := store.ListMintedAssets(nil, 0, 0, false, nil)
	require.NoError(t, err)
	require.Len(t, all, 2)
	require.Equal(t, p2, all[0].PolicyId)
	require.Equal(t, p1, all[1].PolicyId)

	desc, err := store.ListMintedAssets(nil, 0, 0, true, nil)
	require.NoError(t, err)
	require.Len(t, desc, 2)
	require.Equal(t, p1, desc[0].PolicyId)
	require.Equal(t, p2, desc[1].PolicyId)

	filtered, err := store.ListMintedAssets(p1, 0, 0, false, nil)
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	require.Equal(t, p1, filtered[0].PolicyId)
}
