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
	"context"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// legacyUtxoStakeRefsQuery rebuilds the OR-of-pairs statement
// queryUtxoStakeRefs used to run (see issue #4067): a per-(tx_id,
// output_idx) equality OR'd together, gated by liveOnly's "deleted_slot = 0".
// Kept here, rather than in production code, purely so the plan and
// correctness tests below can show the old and new statements side by side
// against the same fixture.
func legacyUtxoStakeRefsQuery(
	ids []models.UtxoId,
	liveOnly bool,
) (string, []any) {
	predicate, args := utxoIDPredicate(ids)
	query := "SELECT DISTINCT credential_tag, staking_key FROM utxo WHERE (" +
		predicate + ")"
	if liveOnly {
		query += " AND deleted_slot = 0"
	}
	return query, args
}

// seedStakeRefLookupUtxos inserts n live utxo rows with sequential, distinct
// (tx_id, output_idx) pairs spread over stakeCredentials distinct stake
// credentials, plus a handful of already-deleted rows so liveOnly has
// something to exclude.
func seedStakeRefLookupUtxos(
	tb testing.TB,
	store *Store,
	n int,
	stakeCredentials int,
) {
	tb.Helper()
	tx, err := store.writeDB.Begin()
	require.NoError(tb, err)
	stmt, err := tx.Prepare(
		"INSERT INTO utxo (tx_id, output_idx, staking_key, credential_tag, " +
			"added_slot, deleted_slot, amount) VALUES (?, ?, ?, ?, ?, ?, ?)",
	)
	require.NoError(tb, err)
	for i := range n {
		key := make([]byte, 28)
		cred := i % stakeCredentials
		key[0] = byte(cred >> 16)
		key[1] = byte(cred >> 8)
		key[2] = byte(cred)
		txID := make([]byte, 32)
		txID[0] = byte(i >> 24)
		txID[1] = byte(i >> 16)
		txID[2] = byte(i >> 8)
		txID[3] = byte(i)
		deletedSlot := int64(0)
		if i%97 == 0 {
			// A small, scattered fraction already spent, so liveOnly has
			// rows to exclude.
			deletedSlot = int64(i + 1)
		}
		_, err := stmt.Exec(
			txID,
			i%4,
			key,
			int64(cred%2),
			int64(i),
			deletedSlot,
			"1000000",
		)
		require.NoError(tb, err)
	}
	require.NoError(tb, stmt.Close())
	require.NoError(tb, tx.Commit())
}

// utxoIDAt returns the UtxoId of the i'th row seedStakeRefLookupUtxos wrote.
func utxoIDAt(i int) models.UtxoId {
	txID := make([]byte, 32)
	txID[0] = byte(i >> 24)
	txID[1] = byte(i >> 16)
	txID[2] = byte(i >> 8)
	txID[3] = byte(i)
	return models.UtxoId{Hash: txID, Idx: uint32(i % 4)}
}

// TestQueryUtxoStakeRefsUsesTxIDIndex pins the SQLite query plan of the
// per-batch lookup queryUtxoStakeRefs runs from DeleteUtxos (liveOnly) and
// the per-transaction write path (transaction_write.go, not liveOnly).
//
// The old OR-of-(tx_id = ? AND output_idx = ?) form abandons the unique
// tx_id_output_idx index once liveOnly's "deleted_slot = 0" gives the
// planner a falsely attractive alternative: idx_utxo_deleted_staking_amount
// matches nearly every live row, so past a handful of OR terms SQLite drives
// off that index instead and visits the whole table (issue #4067). The
// tx_id-IN form this test also exercises stays on tx_id_output_idx
// regardless of term count, because a plain IN list has no such competing
// index to be lured by.
func TestQueryUtxoStakeRefsUsesTxIDIndex(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	seedStakeRefLookupUtxos(t, store, 50_000, 256)

	for _, nTerms := range []int{2, 4, 8, 64, 400} {
		t.Run(fmt.Sprintf("terms=%d", nTerms), func(t *testing.T) {
			ids := make([]models.UtxoId, nTerms)
			for i := range ids {
				// Spread references across the table rather than
				// clustering them at the start.
				ids[i] = utxoIDAt(i * 7)
			}

			legacyQuery, legacyArgs := legacyUtxoStakeRefsQuery(ids, true)
			legacyPlan := queryPlan(
				t,
				store.writeDB,
				legacyQuery,
				legacyArgs...)
			t.Logf("legacy OR-predicate plan (n=%d): %s", nTerms, legacyPlan)

			txIDs, _ := distinctUtxoTxIDs(ids)
			newArgs := make([]any, len(txIDs))
			for i, id := range txIDs {
				newArgs[i] = id
			}
			newQuery := utxoStakeRefsByTxIDQuery(len(txIDs))
			newPlan := queryPlan(t, store.writeDB, newQuery, newArgs...)
			t.Logf("tx_id-IN plan (n=%d):         %s", nTerms, newPlan)

			require.Contains(
				t,
				newPlan,
				"tx_id_output_idx",
				"tx_id-IN lookup must use the unique tx_id/output_idx "+
					"index: %s",
				newPlan,
			)
			require.NotContains(
				t,
				newPlan,
				"idx_utxo_deleted_staking_amount",
				"tx_id-IN lookup must not fall back to the deleted-slot "+
					"index: %s",
				newPlan,
			)

			if nTerms >= 4 {
				// This assertion documents the defect the fix avoids: at
				// >=4 OR terms, the legacy statement is expected to abandon
				// the index. If a future SQLite/modernc.org upgrade changes
				// this planner behavior, this sub-test -- not the fix
				// above -- is the one to revisit.
				require.Contains(
					t,
					legacyPlan,
					"idx_utxo_deleted_staking_amount",
					"expected the legacy OR-predicate to fall back to the "+
						"deleted-slot index past a handful of terms "+
						"(if this fails, the planner defect issue #4067 "+
						"describes may no longer reproduce): %s",
					legacyPlan,
				)
			}
		})
	}
}

// TestQueryUtxoStakeRefsReturnsSameRows proves the tx_id-IN rewrite returns
// exactly the same stake-credential references as the legacy OR-predicate
// form, for both liveOnly settings, including a request spanning multiple
// output indexes of one transaction and a reference to a spent row.
func TestQueryUtxoStakeRefsReturnsSameRows(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	seedStakeRefLookupUtxos(t, store, 5_000, 64)

	ids := []models.UtxoId{
		utxoIDAt(0),
		utxoIDAt(1),
		utxoIDAt(2), // same tx_id as 0/1 has different output_idx (i%4)
		utxoIDAt(97),
		utxoIDAt(388), // 97*4: also lands on a deleted row (i%97==0)
		utxoIDAt(4999),
		utxoIDAt(123456), // does not exist
	}

	ctx := context.Background()
	for _, liveOnly := range []bool{false, true} {
		t.Run(fmt.Sprintf("liveOnly=%v", liveOnly), func(t *testing.T) {
			legacyQuery, legacyArgs := legacyUtxoStakeRefsQuery(ids, liveOnly)
			legacyRows, err := queryStakeRefs(
				ctx,
				store.writeDB,
				legacyQuery,
				legacyArgs...,
			)
			require.NoError(t, err)

			got, err := queryUtxoStakeRefs(ctx, store.writeDB, ids, liveOnly)
			require.NoError(t, err)

			require.NotEmpty(t, legacyRows, "fixture must exercise real rows")
			require.ElementsMatch(t, legacyRows, got)
		})
	}
}

// BenchmarkQueryUtxoStakeRefs is the timing counterpart to
// TestQueryUtxoStakeRefsUsesTxIDIndex: same fixture, same statements, ns/op
// instead of a plan assertion.
func BenchmarkQueryUtxoStakeRefs(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{100_000, 500_000} {
		store := newMigratedSQLiteStore(b)
		seedStakeRefLookupUtxos(b, store, n, 512)
		for _, nTerms := range []int{4, 50, 400} {
			ids := make([]models.UtxoId, nTerms)
			for i := range ids {
				ids[i] = utxoIDAt(i * 7)
			}
			b.Run(
				fmt.Sprintf("n=%d/terms=%d/legacy_or_predicate", n, nTerms),
				func(b *testing.B) {
					legacyQuery, legacyArgs := legacyUtxoStakeRefsQuery(
						ids,
						true,
					)
					for b.Loop() {
						if _, err := queryStakeRefs(
							ctx,
							store.writeDB,
							legacyQuery,
							legacyArgs...,
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
						if _, err := queryUtxoStakeRefs(
							ctx,
							store.writeDB,
							ids,
							true,
						); err != nil {
							b.Fatal(err)
						}
					}
				},
			)
		}
	}
}
