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
	"fmt"
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// legacyGetUtxosByRefsQuery rebuilds GetUtxosByRefs' pre-fix predicate: an OR
// of (tx_id = ? AND output_idx = ?) equalities gated by "deleted_slot = 0",
// the same shape issue #4067 named for queryUtxoStakeRefs.
func legacyGetUtxosByRefsQuery(refs []models.UtxoId) (string, []any) {
	predicate, args := utxoIDPredicate(refs)
	return "deleted_slot = 0 AND (" + predicate + ")", args
}

// legacyGetUtxosByRefsAsOfQuery rebuilds GetUtxosByRefsAsOf's pre-fix
// predicate.
func legacyGetUtxosByRefsAsOfQuery(
	refs []models.UtxoId,
	sqlSlot int64,
) (string, []any) {
	predicate, idArgs := utxoIDPredicate(refs)
	args := append([]any{sqlSlot, sqlSlot}, idArgs...)
	return "added_slot <= ? AND (deleted_slot = 0 OR deleted_slot > ?) AND (" +
		predicate + ")", args
}

// TestGetUtxosByRefsUsesTxIDIndex pins the query plan of the predicate
// GetUtxosByRefs and GetUtxosByRefsAsOf run, the same planner-fallback shape
// issue #4067 documents for queryUtxoStakeRefs: the legacy OR-predicate form
// abandons tx_id_output_idx for idx_utxo_deleted_payment_script/
// idx_utxo_deleted_staking_amount past a handful of terms, and the tx_id-IN
// form this test also exercises does not.
func TestGetUtxosByRefsUsesTxIDIndex(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	seedStakeRefLookupUtxos(t, store, 50_000, 256)

	for _, nTerms := range []int{2, 4, 64, 400} {
		t.Run(fmt.Sprintf("terms=%d", nTerms), func(t *testing.T) {
			refs := make([]models.UtxoId, nTerms)
			for i := range refs {
				refs[i] = utxoIDAt(i * 7)
			}

			legacyPredicate, legacyArgs := legacyGetUtxosByRefsQuery(refs)
			legacyPlan := queryPlan(
				t,
				store.writeDB,
				"SELECT id FROM utxo WHERE "+legacyPredicate,
				legacyArgs...,
			)
			t.Logf("legacy GetUtxosByRefs plan (n=%d): %s", nTerms, legacyPlan)

			txIDs, _ := distinctUtxoTxIDs(refs)
			newArgs := make([]any, len(txIDs))
			for i, id := range txIDs {
				newArgs[i] = id
			}
			placeholders := ""
			for range txIDs {
				placeholders += "?,"
			}
			placeholders = placeholders[:len(placeholders)-1]
			newPlan := queryPlan(
				t,
				store.writeDB,
				"SELECT id FROM utxo WHERE tx_id IN ("+placeholders+")",
				newArgs...,
			)
			t.Logf("tx_id-IN GetUtxosByRefs plan (n=%d): %s", nTerms, newPlan)

			require.Contains(t, newPlan, "tx_id_output_idx")
			require.NotContains(t, newPlan, "idx_utxo_deleted_payment_script")
			require.NotContains(t, newPlan, "idx_utxo_deleted_staking_amount")

			if nTerms >= 4 {
				require.Contains(
					t,
					legacyPlan,
					"idx_utxo_deleted",
					"expected the legacy predicate to fall back to a "+
						"deleted-slot index past a handful of terms: %s",
					legacyPlan,
				)
			}
		})
	}
}

// TestGetUtxosByRefsReturnsSameRows proves the rewrite returns exactly the
// rows the legacy OR-predicate implementation did: live rows only, one row
// per requested ref, absent for a deleted or nonexistent ref, and correct
// when two requested refs share a transaction hash.
func TestGetUtxosByRefsReturnsSameRows(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	seedStakeRefLookupUtxos(t, store, 5_000, 64)

	refs := []models.UtxoId{
		utxoIDAt(0),
		utxoIDAt(1),
		utxoIDAt(2),
		utxoIDAt(388), // deleted (i%97==0)
		utxoIDAt(4999),
		utxoIDAt(123456), // does not exist
	}

	legacyPredicate, legacyArgs := legacyGetUtxosByRefsQuery(
		dedupeUtxoIDs(refs),
	)
	legacy, err := store.queryUtxosWithAssets(
		nil,
		legacyPredicate,
		legacyArgs,
		"",
	)
	require.NoError(t, err)

	got, err := store.GetUtxosByRefs(refs, nil)
	require.NoError(t, err)

	require.NotEmpty(t, legacy)
	require.Len(t, got, len(legacy))
	requireSameUtxoRefs(t, legacy, got)
}

// TestGetUtxosByRefsAsOfReturnsSameRows is the GetUtxosByRefsAsOf
// counterpart, including a ref only live before atSlot (excluded) and one
// deleted after atSlot (included, per the "spent strictly after" contract).
func TestGetUtxosByRefsAsOfReturnsSameRows(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	const atSlot = int64(2_000)
	type row struct {
		i           int
		addedSlot   int64
		deletedSlot int64
	}
	rows := []row{
		{i: 0, addedSlot: 100, deletedSlot: 0},     // live, added before atSlot: included
		{i: 1, addedSlot: 100, deletedSlot: 3_000}, // spent after atSlot: included
		{i: 2, addedSlot: 100, deletedSlot: 1_500}, // spent before atSlot: excluded
		{i: 3, addedSlot: 3_000, deletedSlot: 0},   // added after atSlot: excluded
		// i: 4 deliberately not inserted: reference to a nonexistent row.
	}
	tx, err := store.writeDB.Begin()
	require.NoError(t, err)
	stmt, err := tx.Prepare(
		"INSERT INTO utxo (tx_id, output_idx, staking_key, credential_tag, " +
			"added_slot, deleted_slot, amount) VALUES (?, ?, ?, ?, ?, ?, ?)",
	)
	require.NoError(t, err)
	for _, r := range rows {
		id := utxoIDAt(r.i)
		key := make([]byte, 28)
		key[0] = byte(r.i)
		_, err := stmt.Exec(id.Hash, id.Idx, key, 0, r.addedSlot, r.deletedSlot, "1000000")
		require.NoError(t, err)
	}
	require.NoError(t, stmt.Close())
	require.NoError(t, tx.Commit())

	refs := []models.UtxoId{
		utxoIDAt(0),
		utxoIDAt(1),
		utxoIDAt(2),
		utxoIDAt(3),
		utxoIDAt(4),
	}

	legacyPredicate, legacyArgs := legacyGetUtxosByRefsAsOfQuery(
		dedupeUtxoIDs(refs),
		atSlot,
	)
	legacy, err := store.queryUtxosWithAssets(
		nil,
		legacyPredicate,
		legacyArgs,
		"",
	)
	require.NoError(t, err)

	got, err := store.GetUtxosByRefsAsOf(refs, uint64(atSlot), nil)
	require.NoError(t, err)

	require.Len(t, legacy, 2, "expected rows 0 and 1 to qualify")
	require.Len(t, got, len(legacy))
	requireSameUtxoRefs(t, legacy, got)
}

// TestGetUtxosByRefsAsOfRejectsOutOfDomainSlot pins GetUtxosByRefsAsOf's
// overflow contract: SQLite stores slots as signed INTEGERs, so an atSlot
// above math.MaxInt64 is rejected with an error, as the SQL-bound form did
// through checkedInt64, rather than compared in Go against a live row and
// returned. The check runs before any lookup, so it holds with no refs too.
func TestGetUtxosByRefsAsOfRejectsOutOfDomainSlot(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	id := utxoIDAt(0)
	_, err := store.writeDB.Exec(
		"INSERT INTO utxo (tx_id, output_idx, staking_key, credential_tag, "+
			"added_slot, deleted_slot, amount) VALUES (?, ?, ?, ?, ?, ?, ?)",
		id.Hash, id.Idx, make([]byte, 28), 0, 100, 0, "1000000",
	)
	require.NoError(t, err)
	refs := []models.UtxoId{id}

	t.Run("max in-domain slot", func(t *testing.T) {
		t.Parallel()
		got, err := store.GetUtxosByRefsAsOf(refs, math.MaxInt64, nil)
		require.NoError(t, err)
		requireSameUtxoRefs(
			t,
			[]models.Utxo{{TxId: id.Hash, OutputIdx: id.Idx}},
			got,
		)
	})

	for _, tc := range []struct {
		name   string
		refs   []models.UtxoId
		atSlot uint64
	}{
		{"just past MaxInt64", refs, uint64(math.MaxInt64) + 1},
		{"MaxUint64", refs, math.MaxUint64},
		{"no refs", nil, uint64(math.MaxInt64) + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := store.GetUtxosByRefsAsOf(tc.refs, tc.atSlot, nil)
			require.ErrorContains(t, err, "exceeds int64")
			require.Nil(t, got)
		})
	}
}

// requireSameUtxoRefs asserts a and b contain the same (tx_id, output_idx)
// pairs, ignoring order.
func requireSameUtxoRefs(t *testing.T, a, b []models.Utxo) {
	t.Helper()
	key := func(u models.Utxo) string {
		return fmt.Sprintf("%x:%d", u.TxId, u.OutputIdx)
	}
	aKeys := make([]string, len(a))
	for i, u := range a {
		aKeys[i] = key(u)
	}
	bKeys := make([]string, len(b))
	for i, u := range b {
		bKeys[i] = key(u)
	}
	require.ElementsMatch(t, aKeys, bKeys)
}

// BenchmarkGetUtxosByRefs is the timing counterpart to
// TestGetUtxosByRefsUsesTxIDIndex.
func BenchmarkGetUtxosByRefs(b *testing.B) {
	for _, n := range []int{100_000, 500_000} {
		store := newMigratedSQLiteStore(b)
		seedStakeRefLookupUtxos(b, store, n, 512)
		for _, nTerms := range []int{4, 50, 400} {
			refs := make([]models.UtxoId, nTerms)
			for i := range refs {
				refs[i] = utxoIDAt(i * 7)
			}
			b.Run(
				fmt.Sprintf("n=%d/terms=%d/legacy_or_predicate", n, nTerms),
				func(b *testing.B) {
					legacyPredicate, legacyArgs := legacyGetUtxosByRefsQuery(
						refs,
					)
					for b.Loop() {
						if _, err := store.queryUtxosWithAssets(
							nil,
							legacyPredicate,
							legacyArgs,
							"",
						); err != nil {
							b.Fatal(err)
						}
					}
				},
			)
			b.Run(
				fmt.Sprintf("n=%d/terms=%d/tx_id_in", n, nTerms),
				func(b *testing.B) {
					for b.Loop() {
						if _, err := store.GetUtxosByRefs(
							refs,
							nil,
						); err != nil {
							b.Fatal(err)
						}
					}
				},
			)
		}
	}
}
