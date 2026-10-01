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
	"fmt"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

// seedOrderingAssets writes asset rows over the first rows of
// seedStakeRefLookupUtxos: row ids are 1-based, and id 1 (i=0) and id 98
// (i=97) are already spent there.
//
//	policy A: ids 5 (two names), 7, 98 (spent)
//	policy B: id 200
func seedOrderingAssets(t *testing.T, store *Store) {
	t.Helper()
	for _, a := range []struct {
		utxoID int
		policy byte
		name   string
	}{
		{5, 0xaa, "one"}, {5, 0xaa, "two"}, {7, 0xaa, "one"},
		{98, 0xaa, "one"}, {200, 0xbb, "one"},
	} {
		_, err := store.writeDB.Exec(
			"INSERT INTO asset (utxo_id, policy_id, name, amount) "+
				"VALUES (?, ?, ?, '1')",
			a.utxoID, []byte{a.policy}, []byte(a.name),
		)
		require.NoError(t, err)
	}
}

// An asset-only filter has no address to narrow the live UTxO set, so the
// statement must start from the asset rows of the policy and look UTxOs up by
// primary key. Starting from utxo probes asset once per live UTxO, a cost
// that does not depend on how many UTxOs hold the policy.
func TestUtxoOrderingAssetOnlyFilterDrivesFromAsset(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	seedStakeRefLookupUtxos(t, store, 20_000, 256)
	seedOrderingAssets(t, store)

	statement, args, err := countUtxosOrderingStatement(
		&models.UtxoWithOrderingQuery{
			MatchAllAddresses: true,
			FilterByAsset:     true,
			AssetPolicyID:     []byte{0xaa},
		},
	)
	require.NoError(t, err)
	plan := queryPlan(t, store.writeDB, statement, args...)
	require.NotContains(t, plan, "EXISTS", plan)
	require.Contains(t, plan, "SEARCH asset USING", plan)
	require.Contains(
		t, plan, "SEARCH utxo USING INTEGER PRIMARY KEY", plan,
	)
}

// testUtxoOrderingAssetFilter checks the asset-only filter's results on
// store's backend: a UTxO holding several names of the policy counts once, a
// spent UTxO never matches, and a name narrows the match.
func testUtxoOrderingAssetFilter(t *testing.T, store *Store) {
	t.Helper()
	policy := func(b byte) lcommon.Blake2b224 {
		var p lcommon.Blake2b224
		p[0] = b
		return p
	}
	type holding struct {
		policy byte
		name   string
	}
	output := func(held ...holding) *mary.MaryTransactionOutput {
		assets := map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeOutput{}
		for _, h := range held {
			if assets[policy(h.policy)] == nil {
				assets[policy(h.policy)] = map[cbor.ByteString]lcommon.MultiAssetTypeOutput{}
			}
			assets[policy(h.policy)][cbor.NewByteString([]byte(h.name))] = big.NewInt(1)
		}
		out := &mary.MaryTransactionOutput{
			OutputAmount: mary.MaryTransactionOutputValue{Amount: 1_000_000},
		}
		if len(held) > 0 {
			multiAsset := lcommon.NewMultiAsset[lcommon.MultiAssetTypeOutput](assets)
			out.OutputAmount.Assets = &multiAsset
		}
		return out
	}
	txHash := func(i int) string {
		return fmt.Sprintf("%064x", i)
	}
	outputs := []*mary.MaryTransactionOutput{
		output(holding{0xaa, "one"}, holding{0xaa, "two"}), // 1
		output(holding{0xaa, "one"}),                       // 2
		output(holding{0xaa, "one"}),                       // 3, spent below
		output(holding{0xbb, "one"}),                       // 4
		output(),                                           // 5
	}
	var utxos []models.UtxoSlot
	for i, out := range outputs {
		utxos = append(utxos, models.UtxoSlot{
			Utxo: ledger.Utxo{
				Id:     shelley.NewShelleyTransactionInput(txHash(i+1), 0),
				Output: out,
			},
			Slot: 1,
		})
	}
	require.NoError(t, store.AddUtxos(utxos, nil))
	_, err := store.writeDB.Exec(
		store.dialect.Rebind(
			"UPDATE utxo SET deleted_slot = 9 WHERE tx_id = ?",
		),
		mustDecodeHex(t, txHash(3)),
	)
	require.NoError(t, err)

	for _, test := range []struct {
		name   string
		policy byte
		asset  []byte
		want   []int
	}{
		{name: "policy", policy: 0xaa, want: []int{1, 2}},
		{name: "policy and name", policy: 0xaa, asset: []byte("one"), want: []int{1, 2}},
		{name: "other name", policy: 0xaa, asset: []byte("two"), want: []int{1}},
		{name: "other policy", policy: 0xbb, want: []int{4}},
		{name: "absent policy", policy: 0xcc, want: []int{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			query := &models.UtxoWithOrderingQuery{
				MatchAllAddresses: true,
				FilterByAsset:     true,
				AssetPolicyID:     policy(test.policy).Bytes(),
				AssetName:         test.asset,
			}
			count, err := store.CountUtxosByAddressWithOrdering(query, nil)
			require.NoError(t, err)
			require.Equal(t, len(test.want), count)

			rows, err := store.GetUtxosByAddressWithOrdering(query, nil)
			require.NoError(t, err)
			got := []int{}
			for _, row := range rows {
				for i := range outputs {
					if bytes.Equal(row.TxId, mustDecodeHex(t, txHash(i+1))) {
						got = append(got, i+1)
					}
				}
			}
			require.ElementsMatch(t, test.want, got)
		})
	}
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

func TestUtxoOrderingAssetFilterResultsSQLite(t *testing.T) {
	t.Parallel()
	testUtxoOrderingAssetFilter(t, newMigratedSQLiteStore(t))
}

// With an address filter the live set is already narrowed, so the asset
// filter stays a per-row check against it; both filters must still apply.
func TestUtxoOrderingAddressAndAssetFilterResults(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	seedStakeRefLookupUtxos(t, store, 2_000, 256)
	seedOrderingAssets(t, store)

	// Row id n was seeded with staking credential (n-1) % 256 in the low
	// bytes of a 28-byte key.
	delegation := func(cred int) models.UtxoAddressPattern {
		key := make([]byte, 28)
		key[1], key[2] = byte(cred>>8), byte(cred)
		return models.UtxoAddressPattern{DelegationPart: key}
	}
	for _, test := range []struct {
		name   string
		cred   int
		policy byte
		want   []int
	}{
		{name: "address holds asset", cred: 4, policy: 0xaa, want: []int{5}},
		{name: "address lacks asset", cred: 4, policy: 0xbb, want: []int{}},
		{name: "other address", cred: 199, policy: 0xbb, want: []int{200}},
	} {
		t.Run(test.name, func(t *testing.T) {
			query := &models.UtxoWithOrderingQuery{
				AddressPatterns: []models.UtxoAddressPattern{
					delegation(test.cred),
				},
				FilterByAsset: true,
				AssetPolicyID: []byte{test.policy},
			}
			count, err := store.CountUtxosByAddressWithOrdering(query, nil)
			require.NoError(t, err)
			require.Equal(t, len(test.want), count)

			rows, err := store.GetUtxosByAddressWithOrdering(query, nil)
			require.NoError(t, err)
			got := []int{}
			for _, row := range rows {
				got = append(got, int(row.Utxo.ID))
			}
			require.ElementsMatch(t, test.want, got)
		})
	}
}
