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
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	sqlitequery "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/internal/query/sqlite"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func BenchmarkGetUtxoPreparedStatementCache(b *testing.B) {
	for _, rowCount := range []int{1_000, 50_000} {
		b.Run(fmt.Sprintf("rows=%d", rowCount), func(b *testing.B) {
			store := newMigratedSQLiteStore(b)
			seedUtxoLookupRows(b, store, rowCount)
			txID := benchmarkUtxoTxID(rowCount / 2)

			b.Run("generated-one-shot", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					utxo, err := getUtxoGeneratedOneShot(
						store,
						txID,
						0,
					)
					if err != nil {
						b.Fatal(err)
					}
					if utxo == nil {
						b.Fatal("generated query returned no UTxO")
					}
				}
				b.ReportMetric(float64(rowCount), "utxos")
			})

			b.Run("prepared-cache", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					utxo, err := store.GetUtxo(txID, 0, nil)
					if err != nil {
						b.Fatal(err)
					}
					if utxo == nil {
						b.Fatal("cached query returned no UTxO")
					}
				}
				b.ReportMetric(float64(rowCount), "utxos")
			})
		})
	}
}

func seedUtxoLookupRows(tb testing.TB, store *Store, count int) {
	tb.Helper()
	tx, err := store.writeDB.BeginTx(context.Background(), nil)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = tx.Rollback() })
	stmt, err := tx.PrepareContext(
		context.Background(),
		"INSERT INTO utxo (tx_id, output_idx, added_slot, deleted_slot, amount) "+
			"VALUES (?, 0, ?, 0, '1000000')",
	)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = stmt.Close() }()
	for i := range count {
		if _, err := stmt.ExecContext(
			context.Background(),
			benchmarkUtxoTxID(i),
			int64(i+1),
		); err != nil {
			tb.Fatalf("seed UTxO %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		tb.Fatal(err)
	}
}

func benchmarkUtxoTxID(index int) []byte {
	txID := make([]byte, 32)
	binary.BigEndian.PutUint64(txID[24:], uint64(index+1)) // #nosec G115 -- benchmark index is non-negative.
	return txID
}

func getUtxoGeneratedOneShot(
	store *Store,
	txID []byte,
	index uint32,
) (*models.Utxo, error) {
	db, ctx, err := store.readDBFromTxn(nil)
	if err != nil {
		return nil, err
	}
	row, err := store.operationalQueries(db).GetLiveUtxo(
		ctx,
		sqlitequery.GetLiveUtxoParams{
			TxID: txID,
			OutputIdx: sql.NullInt64{
				Int64: int64(index),
				Valid: true,
			},
		},
	)
	if err != nil {
		return nil, err
	}
	utxo, err := utxoFromSQLite(row)
	if err != nil {
		return nil, err
	}
	if err := store.loadUtxoAssets(ctx, db, []*models.Utxo{utxo}); err != nil {
		return nil, err
	}
	return utxo, nil
}

// legacyGetUtxosByRefsQuery rebuilds GetUtxosByRefs' pre-fix predicate: an OR
// of (tx_id = ? AND output_idx = ?) equalities gated by "deleted_slot = 0",
// the same shape described by queryUtxoStakeRefs.
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
// documents for queryUtxoStakeRefs: the legacy OR-predicate form
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

func TestAddressTransactionInputQueryUsesTxIDIndex(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	seedStakeRefLookupUtxos(t, store, 50_000, 256)

	args := make([]any, maxCachedAddressInputQuerySize)
	for i := range args {
		args[i] = utxoIDAt(i * 7).Hash
	}
	plan := queryPlan(
		t,
		store.writeDB,
		addressTransactionInputQuery(maxCachedAddressInputQuerySize),
		args...,
	)
	require.Contains(t, plan, "tx_id_output_idx")
	require.NotContains(t, plan, "SCAN utxo")
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

func legacyMarkUtxosDeletedAtSlotQuery(
	ids []models.UtxoId,
	slot int64,
) (string, []any) {
	predicate, idArgs := utxoIDPredicate(ids)
	args := append([]any{slot}, idArgs...)
	return "UPDATE utxo SET deleted_slot = ? WHERE deleted_slot = 0 AND (" +
		predicate + ")", args
}

// TestMarkUtxosDeletedAtSlotUsesTxIDIndex pins the lookup plan before the
// method updates the selected primary keys. The former OR-of-pairs update
// prefers a deleted_slot index after a few terms, which turns a bounded
// batch into a scan of nearly every live UTxO. Selecting by tx_id and
// filtering output indexes in Go keeps the lookup on tx_id_output_idx.
func TestMarkUtxosDeletedAtSlotUsesTxIDIndex(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	seedStakeRefLookupUtxos(t, store, 50_000, 256)

	for _, nTerms := range []int{2, 4, 64, 400} {
		t.Run(fmt.Sprintf("terms=%d", nTerms), func(t *testing.T) {
			ids := make([]models.UtxoId, nTerms)
			for i := range ids {
				ids[i] = utxoIDAt(i * 7)
			}

			legacyQuery, legacyArgs := legacyMarkUtxosDeletedAtSlotQuery(
				ids,
				1,
			)
			legacyPlan := queryPlan(
				t,
				store.writeDB,
				legacyQuery,
				legacyArgs...,
			)
			t.Logf("legacy MarkUtxosDeletedAtSlot plan (n=%d): %s", nTerms, legacyPlan)

			txIDs, _ := distinctUtxoTxIDs(ids)
			query, args := utxoDeletionRowsByTxIDQuery(txIDs, false)
			newPlan := queryPlan(t, store.writeDB, query, args...)
			t.Logf("tx_id-IN mark lookup plan (n=%d): %s", nTerms, newPlan)

			require.Contains(t, newPlan, "tx_id_output_idx")
			require.NotContains(t, newPlan, "idx_utxo_deleted_payment_script")
			require.NotContains(t, newPlan, "idx_utxo_deleted_staking_amount")

			if nTerms >= 4 {
				require.Contains(
					t,
					legacyPlan,
					"idx_utxo_deleted",
					"expected the legacy update to fall back to a deleted-slot index: %s",
					legacyPlan,
				)
			}
		})
	}
}

func TestUtxoDeletionRowsByTxIDQueryLocksNonSQLiteRows(t *testing.T) {
	t.Parallel()

	txIDs := [][]byte{[]byte("tx-a"), []byte("tx-b")}
	query, _ := utxoDeletionRowsByTxIDQuery(txIDs, true)
	require.Contains(t, query, "ORDER BY id FOR UPDATE")
	query, _ = utxoDeletionRowsByTxIDQuery(txIDs, false)
	require.NotContains(t, query, "FOR UPDATE")
}

// TestMarkUtxosDeletedAtSlotUpdatesOnlyRequestedLiveRows proves the indexed
// lookup does not widen a request to sibling outputs of the same transaction
// and leaves rows that were already spent untouched.
func TestMarkUtxosDeletedAtSlotUpdatesOnlyRequestedLiveRows(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	type row struct {
		txID        []byte
		outputIdx   uint32
		deletedSlot int64
	}
	rows := []row{
		{txID: []byte("transaction-a"), outputIdx: 0, deletedSlot: 0},
		{txID: []byte("transaction-a"), outputIdx: 1, deletedSlot: 0},
		{txID: []byte("transaction-b"), outputIdx: 0, deletedSlot: 0},
		{txID: []byte("transaction-c"), outputIdx: 0, deletedSlot: 9},
	}
	for i, row := range rows {
		_, err := store.writeDB.Exec(
			"INSERT INTO utxo (tx_id, output_idx, staking_key, credential_tag, "+
				"added_slot, deleted_slot, amount) VALUES (?, ?, ?, ?, ?, ?, ?)",
			row.txID,
			row.outputIdx,
			[]byte{byte(i + 1)},
			0,
			1,
			row.deletedSlot,
			"1000000",
		)
		require.NoError(t, err)
	}

	require.NoError(t, store.MarkUtxosDeletedAtSlot(
		nil,
		[]types.UtxoKey{
			{TxId: rows[0].txID, OutputIdx: rows[0].outputIdx},
			{TxId: rows[2].txID, OutputIdx: rows[2].outputIdx},
			{TxId: rows[3].txID, OutputIdx: rows[3].outputIdx},
			{TxId: []byte("missing"), OutputIdx: 0},
		},
		20,
	))

	want := []int64{20, 0, 20, 9}
	for i, row := range rows {
		var got int64
		require.NoError(t, store.writeDB.QueryRow(
			"SELECT deleted_slot FROM utxo WHERE tx_id = ? AND output_idx = ?",
			row.txID,
			row.outputIdx,
		).Scan(&got))
		require.Equal(t, want[i], got)
	}
}

// TestMarkUtxosDeletedAtSlotUpdatePlansOnPrimaryKey pins the plan of the
// statement MarkUtxosDeletedAtSlot actually executes -- markUtxosDeletedQuery
// builds the only UPDATE in that method -- rather than only the lookup that
// precedes it.
//
// The lookup fix alone left the update carrying "deleted_slot = 0", and with
// no sqlite_stat1 SQLite drives that form from
// idx_utxo_deleted_payment_script (deleted_slot=?) from two terms upwards,
// evaluating "id IN (...)" against every live row. That is
// whole-table pass moved from the first statement to the second. Statistics
// hide it, so this test must not run ANALYZE.
func TestMarkUtxosDeletedAtSlotUpdatePlansOnPrimaryKey(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	seedStakeRefLookupUtxos(t, store, 50_000, 256)

	for _, nRows := range []int{1, 2, 4, 64, 400, 998} {
		t.Run(fmt.Sprintf("rows=%d", nRows), func(t *testing.T) {
			rowIDs := make([]int64, nRows)
			for i := range rowIDs {
				rowIDs[i] = int64(i + 1)
			}

			query, args := markUtxosDeletedQuery(rowIDs, 1, false)
			plan := queryPlan(t, store.writeDB, query, args...)
			require.Contains(
				t, plan, "INTEGER PRIMARY KEY",
				"the update must resolve rows by primary key: %s", plan,
			)
			require.NotContains(t, plan, "idx_utxo_deleted")

			// Control: the guarded form this replaced, planned on the same
			// statistics-free database.
			guardedArgs := make([]any, 0, len(args))
			guardedArgs = append(guardedArgs, args...)
			guardedPlan := queryPlan(
				t,
				store.writeDB,
				strings.Replace(
					query,
					"WHERE id IN (",
					"WHERE deleted_slot = 0 AND id IN (",
					1,
				),
				guardedArgs...,
			)
			if nRows >= 2 {
				require.Contains(
					t, guardedPlan, "idx_utxo_deleted",
					"expected the guarded update to fall back to a "+
						"deleted-slot index: %s", guardedPlan,
				)
			}
		})
	}
}

func TestMarkUtxosDeletedQueryRetainsLivenessGuardForConcurrentWriters(t *testing.T) {
	t.Parallel()
	query, args := markUtxosDeletedQuery([]int64{1, 2}, 10, true)
	require.Equal(
		t,
		"UPDATE utxo SET deleted_slot = ? WHERE deleted_slot = 0 AND id IN (?,?)",
		query,
	)
	require.Equal(t, []any{int64(10), int64(1), int64(2)}, args)
}

// The DISTINCT-bearing statements this package used to run in the rollback
// sweep. Kept here so the plan test can show, in the same run, that the two
// forms return the same rows and that only the DISTINCT form abandons the
// slot index.
const (
	legacyDistinctAddedAfterSlotQuery = "SELECT DISTINCT credential_tag, " +
		"staking_key FROM utxo WHERE added_slot > ?"
	legacyDistinctDeletedAfterSlotQuery = "SELECT DISTINCT credential_tag, " +
		"staking_key FROM utxo WHERE deleted_slot > ?"
)

// newMigratedSQLiteStore opens a SQLite store carrying the real migrated
// schema, so the utxo indexes under test (idx_utxo_added_slot,
// idx_utxo_deleted_staking_amount, idx_utxo_staking_deleted_amount) are the
// production ones rather than a hand-rolled CREATE TABLE. The query planner
// reads the schema and sqlite_stat1, not the storage backend, so the
// in-memory database this package already uses for store tests produces the
// same plans as a file-backed one.
func newMigratedSQLiteStore(tb testing.TB) *Store {
	tb.Helper()
	db, err := OpenDB(
		"sqlite",
		fmt.Sprintf(
			"file:rollback_scan_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
		"sqlite",
		false,
	)
	require.NoError(tb, err)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(tb, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(tb, err)
	require.NoError(tb, store.Start(context.Background()))
	tb.Cleanup(func() { require.NoError(tb, store.Close()) })
	return store
}

// seedRollbackUtxos inserts n utxo rows spread over stakeCredentials distinct
// stake credentials. Rows below rolledBackFrom are "old" (added and, for half
// of them, spent long before the rollback point); the last rollbackRows rows
// sit above it and are the only ones a rollback sweep has to look at, half of
// them also spent above it so both sweep statements have rows to return. This
// is the shape that makes the bug visible: a large settled table whose rows
// are almost all irrelevant to the rollback, and a tiny window that is not.
func seedRollbackUtxos(
	tb testing.TB,
	store *Store,
	n int,
	stakeCredentials int,
	rolledBackFrom int64,
	rollbackRows int,
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
		addedSlot := rolledBackFrom - int64(n-i)
		deletedSlot := int64(0)
		if i >= n-rollbackRows {
			addedSlot = rolledBackFrom + int64(i-(n-rollbackRows)) + 1
			if i%2 == 0 {
				// Spent after the rollback point, so the rollback has to
				// un-spend it: this is what SetUtxosNotDeletedAfterSlot
				// looks for.
				deletedSlot = addedSlot
			}
		} else if i%2 == 0 {
			// A settled spend, well below the rollback point.
			deletedSlot = addedSlot + 1
		}
		_, err := stmt.Exec(
			txID,
			i%4,
			key,
			int64(cred%2),
			addedSlot,
			deletedSlot,
			"1000000",
		)
		require.NoError(tb, err)
	}
	require.NoError(tb, stmt.Close())
	require.NoError(tb, tx.Commit())
}

// analyzeStore populates sqlite_stat1 for the fixture. A node only runs
// ANALYZE at the points added by (after a Mithril import, before
// API-mode backfill), so a producer's utxo table is normally queried without
// current stats -- which is the state in which the DISTINCT plan goes wrong.
func analyzeStore(tb testing.TB, store *Store) {
	tb.Helper()
	_, err := store.writeDB.Exec("ANALYZE")
	require.NoError(tb, err)
}

// queryPlan returns the flattened EXPLAIN QUERY PLAN output for query.
func queryPlan(tb testing.TB, db *sql.DB, query string, args ...any) string {
	tb.Helper()
	rows, err := db.Query("EXPLAIN QUERY PLAN "+query, args...)
	require.NoError(tb, err)
	defer func() { require.NoError(tb, rows.Close()) }()
	var lines []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(tb, rows.Scan(&id, &parent, &notUsed, &detail))
		lines = append(lines, detail)
	}
	require.NoError(tb, rows.Err())
	return strings.Join(lines, "\n")
}

// TestRollbackStakeRefQueriesUseSlotIndexes pins the SQLite query plans of
// the two statements the rollback sweep runs over utxo (from
// DeleteUtxosAfterSlot and SetUtxosNotDeletedAfterSlot).
//
// With SQL DISTINCT over (credential_tag, staking_key), SQLite prefers an
// index that already supplies that ordering -- idx_utxo_staking_deleted_amount
// -- because it can then skip the temp B-tree, and it full-scans it. That
// index carries no added_slot column, so for the added_slot statement the scan
// is not even covering: every entry costs a row lookup just to evaluate the
// slot predicate. The result is a pass over the entire utxo table on every
// rollback, whatever the rollback depth -- minutes on a producer with a
// multi-million-row table, during which the ledger holds its async DB
// transaction and no block can be applied.
//
// Without DISTINCT the planner uses the index that answers the predicate and
// only visits the rolled-back window, and it does so whether or not
// sqlite_stat1 has been populated. That stats independence is the reason to
// dedupe in Go rather than to rely on ANALYZE: a long-running node's utxo
// stats are stale or absent ( runs ANALYZE only around a Mithril import),
// and the MySQL and Postgres stores have their own planners.
//
// The assertion is on the plan rather than on elapsed time so it is
// deterministic; BenchmarkRollbackStakeRefQueries carries the timings.
func TestRollbackStakeRefQueriesUseSlotIndexes(t *testing.T) {
	t.Parallel()
	const rolledBackFrom = int64(2_656_808)

	for _, stats := range []struct {
		name    string
		analyze bool
	}{
		{name: "without_planner_stats"},
		{name: "with_planner_stats", analyze: true},
	} {
		t.Run(stats.name, func(t *testing.T) {
			t.Parallel()
			store := newMigratedSQLiteStore(t)
			seedRollbackUtxos(t, store, 20_000, 64, rolledBackFrom, 40)
			if stats.analyze {
				analyzeStore(t, store)
			}

			for _, tc := range []struct {
				name        string
				query       string
				legacyQuery string
				wantSearch  string
			}{
				{
					name:        "added_slot",
					query:       utxoStakeRefsAddedAfterSlotQuery,
					legacyQuery: legacyDistinctAddedAfterSlotQuery,
					wantSearch:  "idx_utxo_added_slot (added_slot>?)",
				},
				{
					name:        "deleted_slot",
					query:       utxoStakeRefsDeletedAfterSlotQuery,
					legacyQuery: legacyDistinctDeletedAfterSlotQuery,
					wantSearch:  "(deleted_slot>?)",
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					plan := queryPlan(
						t,
						store.writeDB,
						tc.query,
						rolledBackFrom,
					)
					legacyPlan := queryPlan(
						t,
						store.writeDB,
						tc.legacyQuery,
						rolledBackFrom,
					)
					t.Logf("plan without DISTINCT: %s", plan)
					t.Logf("plan with DISTINCT:    %s", legacyPlan)

					require.Contains(
						t,
						plan,
						"SEARCH",
						"rollback stake-ref query must range-search: %s",
						plan,
					)
					require.Contains(
						t,
						plan,
						tc.wantSearch,
						"rollback stake-ref query must drive off the slot "+
							"predicate: %s",
						plan,
					)
					require.NotContains(
						t,
						plan,
						"SCAN",
						"rollback stake-ref query must not scan the table or "+
							"an index: %s",
						plan,
					)
					require.NotContains(
						t,
						plan,
						"idx_utxo_staking_deleted_amount",
						"rollback stake-ref query must not fall back to the "+
							"stake-ordered index: %s",
						plan,
					)
				})
			}
		})
	}
}

// TestRollbackStakeRefsDedupeMatchesSQLDistinct proves the Go-side dedupe
// returns exactly the set SQL DISTINCT returned, on a fixture holding repeated
// credentials, two credential tags sharing a staking key, and rows with a NULL
// or empty staking key (which queryStakeRefs drops).
func TestRollbackStakeRefsDedupeMatchesSQLDistinct(t *testing.T) {
	t.Parallel()
	const rolledBackFrom = int64(100)
	store := newMigratedSQLiteStore(t)

	keyA := make([]byte, 28)
	keyA[0] = 0xAA
	keyB := make([]byte, 28)
	keyB[0] = 0xBB

	rows := []struct {
		stakingKey    []byte
		credentialTag int64
		addedSlot     int64
		deletedSlot   int64
	}{
		{keyA, 0, 101, 0},
		{keyA, 0, 102, 0}, // repeat of (0, keyA)
		{keyB, 0, 103, 0},
		{keyA, 1, 104, 0},     // same key, other credential tag
		{keyB, 0, 105, 0},     // repeat of (0, keyB)
		{nil, 0, 106, 0},      // NULL staking key: dropped
		{[]byte{}, 0, 107, 0}, // empty staking key: dropped
		{keyA, 0, 50, 0},      // below the rollback point: excluded
		{keyB, 1, 60, 101},    // deleted above the rollback point
		{keyA, 1, 61, 102},    // deleted above the rollback point
		{keyA, 1, 62, 50},     // deleted below: excluded
	}
	tx, err := store.writeDB.Begin()
	require.NoError(t, err)
	stmt, err := tx.Prepare(
		"INSERT INTO utxo (tx_id, output_idx, staking_key, credential_tag, " +
			"added_slot, deleted_slot, amount) VALUES (?, ?, ?, ?, ?, ?, ?)",
	)
	require.NoError(t, err)
	for i, row := range rows {
		_, err := stmt.Exec(
			[]byte{byte(i)},
			0,
			row.stakingKey,
			row.credentialTag,
			row.addedSlot,
			row.deletedSlot,
			"1000000",
		)
		require.NoError(t, err)
	}
	require.NoError(t, stmt.Close())
	require.NoError(t, tx.Commit())

	ctx := context.Background()
	for _, tc := range []struct {
		name        string
		query       string
		legacyQuery string
		want        []models.StakeCredentialRef
	}{
		{
			name:        "added_slot",
			query:       utxoStakeRefsAddedAfterSlotQuery,
			legacyQuery: legacyDistinctAddedAfterSlotQuery,
			want: []models.StakeCredentialRef{
				models.NewStakeCredentialRef(0, keyA),
				models.NewStakeCredentialRef(0, keyB),
				models.NewStakeCredentialRef(1, keyA),
			},
		},
		{
			name:        "deleted_slot",
			query:       utxoStakeRefsDeletedAfterSlotQuery,
			legacyQuery: legacyDistinctDeletedAfterSlotQuery,
			want: []models.StakeCredentialRef{
				models.NewStakeCredentialRef(1, keyA),
				models.NewStakeCredentialRef(1, keyB),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := queryStakeRefsDeduped(
				ctx,
				store.writeDB,
				tc.query,
				rolledBackFrom,
			)
			require.NoError(t, err)
			require.ElementsMatch(t, tc.want, got)
			require.Len(t, got, len(tc.want), "dedupe left a duplicate")

			legacy, err := queryStakeRefs(
				ctx,
				store.writeDB,
				tc.legacyQuery,
				rolledBackFrom,
			)
			require.NoError(t, err)
			require.ElementsMatch(
				t,
				legacy,
				got,
				"Go dedupe must return the same set as SQL DISTINCT",
			)
		})
	}
}

// TestRollbackSweepStillTruncatesUtxos exercises DeleteUtxosAfterSlot end to
// end: the rollback still removes exactly the rows added above the slot and
// leaves settled rows alone, which is the behaviour the plan change must not
// disturb.
func TestRollbackSweepStillTruncatesUtxos(t *testing.T) {
	t.Parallel()
	const rolledBackFrom = uint64(200)
	store := newMigratedSQLiteStore(t)
	seedRollbackUtxos(t, store, 200, 8, int64(rolledBackFrom), 25)

	var above int
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM utxo WHERE added_slot > ?", rolledBackFrom,
	).Scan(&above))
	require.Equal(t, 25, above, "fixture must have rows above the slot")

	require.NoError(t, store.DeleteUtxosAfterSlot(rolledBackFrom, nil))

	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM utxo WHERE added_slot > ?", rolledBackFrom,
	).Scan(&above))
	require.Zero(
		t,
		above,
		"rollback must delete every utxo added after the slot",
	)

	var total int
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM utxo",
	).Scan(&total))
	require.Equal(t, 175, total, "rollback must not touch settled rows")
}

// BenchmarkRollbackStakeRefQueries is the secondary, timing-based evidence:
// it runs the production statement and the DISTINCT statement it replaces
// against the same seeded table, so the ratio between them shows the cost of
// the abandoned index directly.
func BenchmarkRollbackStakeRefQueries(b *testing.B) {
	for _, n := range []int{100_000, 500_000} {
		const rolledBackFrom = int64(4_000_000)
		store := newMigratedSQLiteStore(b)
		seedRollbackUtxos(b, store, n, 512, rolledBackFrom, 40)
		ctx := context.Background()
		for _, tc := range []struct {
			name  string
			query string
		}{
			{
				name:  "added_slot/no_distinct",
				query: utxoStakeRefsAddedAfterSlotQuery,
			},
			{
				name:  "added_slot/distinct",
				query: legacyDistinctAddedAfterSlotQuery,
			},
			{
				name:  "deleted_slot/no_distinct",
				query: utxoStakeRefsDeletedAfterSlotQuery,
			},
			{
				name:  "deleted_slot/distinct",
				query: legacyDistinctDeletedAfterSlotQuery,
			},
		} {
			b.Run(fmt.Sprintf("n=%d/%s", n, tc.name), func(b *testing.B) {
				for b.Loop() {
					if _, err := queryStakeRefs(
						ctx,
						store.writeDB,
						tc.query,
						rolledBackFrom,
					); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// namedQueryFromSource returns the body of a named sqlc query, read from the
// .sql file sqlc generates the store from. Reading the shipped source rather
// than restating the SQL in the test means the plan assertions below cannot
// drift away from the statement the node actually runs.
func namedQueryFromSource(tb testing.TB, name string) string {
	tb.Helper()
	source, err := os.ReadFile(
		filepath.Join("queries", "sqlite", "operational.sql"),
	)
	require.NoError(tb, err)
	marker := "-- name: " + name + " :"
	start := strings.Index(string(source), marker)
	require.GreaterOrEqual(tb, start, 0, "query %q not found in source", name)
	// Skip the "-- name:" line itself, then take everything up to the
	// statement terminator.
	body := string(source)[start:]
	body = body[strings.Index(body, "\n")+1:]
	end := strings.Index(body, ";")
	require.GreaterOrEqual(tb, end, 0, "query %q is unterminated", name)
	return strings.TrimSpace(body[:end])
}

// withOrderByIDDesc rewrites a statement's trailing ORDER BY to the
// "ORDER BY id DESC" GetUtxosAddedAfterSlot used to carry, so the tests can
// show in the same run that ordering by the rowid alone is what costs the
// table scan. It is deliberately not a check on the shipped text: the plan
// assertion is the contract, so reverting the statement has to fail on the
// plan, not on a string comparison.
func withOrderByIDDesc(tb testing.TB, query string) string {
	tb.Helper()
	idx := strings.LastIndex(query, "ORDER BY")
	require.GreaterOrEqual(tb, idx, 0, "statement has no ORDER BY: %s", query)
	return query[:idx] + "ORDER BY id DESC"
}

// TestGetUtxosAddedAfterSlotUsesSlotIndex pins the SQLite query plan of the
// other utxo statement the rollback sweep runs: UtxosDeleteRolledback calls
// GetUtxosAddedAfterSlot to collect the blobs to drop, immediately before
// DeleteUtxosAfterSlot.
//
// The statement used to end in "ORDER BY id DESC". id is the rowid, so SQLite
// produced that order by walking the table backwards -- a full SCAN, and a
// descending one, so readahead does not help -- instead of range-searching
// idx_utxo_added_slot. Like the DISTINCT sweep queries, the cost then tracks
// the size of the utxo table rather than the depth of the rollback.
//
// Ordering by added_slot first makes the requested order the reverse of
// idx_utxo_added_slot's own order ((added_slot, rowid), and id is the rowid),
// so the range search serves it directly with no sorter -- with or without
// planner statistics, which is what the two sub-tests check.
func TestGetUtxosAddedAfterSlotUsesSlotIndex(t *testing.T) {
	t.Parallel()
	const rolledBackFrom = int64(2_656_808)
	query := namedQueryFromSource(t, "GetUtxosAddedAfterSlot")
	legacyQuery := withOrderByIDDesc(t, query)

	for _, stats := range []struct {
		name    string
		analyze bool
	}{
		{name: "without_planner_stats"},
		{name: "with_planner_stats", analyze: true},
	} {
		t.Run(stats.name, func(t *testing.T) {
			t.Parallel()
			store := newMigratedSQLiteStore(t)
			seedRollbackUtxos(t, store, 20_000, 64, rolledBackFrom, 40)
			if stats.analyze {
				analyzeStore(t, store)
			}

			plan := queryPlan(t, store.writeDB, query, rolledBackFrom)
			legacyPlan := queryPlan(
				t,
				store.writeDB,
				legacyQuery,
				rolledBackFrom,
			)
			t.Logf("plan ordered by added_slot: %s", plan)
			t.Logf("plan ordered by id:         %s", legacyPlan)

			require.Contains(
				t,
				plan,
				"SEARCH utxo USING INDEX idx_utxo_added_slot (added_slot>?)",
				"blob-collect query must range-search the slot index: %s",
				plan,
			)
			require.NotContains(
				t,
				plan,
				"SCAN",
				"blob-collect query must not scan the table: %s",
				plan,
			)
			require.NotContains(
				t,
				plan,
				"TEMP B-TREE",
				"the requested order is the index order, so no sorter is "+
					"needed: %s",
				plan,
			)
		})
	}
}

// TestGetUtxosAddedAfterSlotReturnsSameRows proves the reordering changed only
// the order, not the set: the shipped statement returns exactly the ids the
// old "ORDER BY id DESC" statement returned, and returns them newest first.
func TestGetUtxosAddedAfterSlotReturnsSameRows(t *testing.T) {
	t.Parallel()
	const rolledBackFrom = int64(2_656_808)
	store := newMigratedSQLiteStore(t)
	seedRollbackUtxos(t, store, 2_000, 16, rolledBackFrom, 60)

	query := namedQueryFromSource(t, "GetUtxosAddedAfterSlot")
	legacyQuery := withOrderByIDDesc(t, query)

	type row struct {
		id        int64
		addedSlot int64
	}
	read := func(q string) []row {
		rows, err := store.writeDB.Query(q, rolledBackFrom)
		require.NoError(t, err)
		defer func() { require.NoError(t, rows.Close()) }()
		cols, err := rows.Columns()
		require.NoError(t, err)
		idIdx, slotIdx := -1, -1
		for i, c := range cols {
			switch c {
			case "id":
				idIdx = i
			case "added_slot":
				slotIdx = i
			}
		}
		require.GreaterOrEqual(t, idIdx, 0)
		require.GreaterOrEqual(t, slotIdx, 0)
		var out []row
		for rows.Next() {
			cells := make([]any, len(cols))
			for i := range cells {
				// The fixture intentionally leaves several selected columns NULL.
				// NullString accepts NULL and is sufficient for the columns whose
				// values this comparison does not inspect.
				cells[i] = new(sql.NullString)
			}
			var id, slot sql.NullInt64
			cells[idIdx] = &id
			cells[slotIdx] = &slot
			require.NoError(t, rows.Scan(cells...))
			out = append(out, row{id: id.Int64, addedSlot: slot.Int64})
		}
		require.NoError(t, rows.Err())
		return out
	}

	got := read(query)
	legacy := read(legacyQuery)
	require.NotEmpty(t, got, "fixture must return rows above the slot")
	require.ElementsMatch(
		t,
		legacy,
		got,
		"reordering must not change which rows are returned",
	)

	for i := 1; i < len(got); i++ {
		prev, cur := got[i-1], got[i]
		require.True(
			t,
			prev.addedSlot > cur.addedSlot ||
				(prev.addedSlot == cur.addedSlot && prev.id > cur.id),
			"rows must be newest first: %+v before %+v",
			prev,
			cur,
		)
		require.Greater(
			t,
			cur.addedSlot,
			rolledBackFrom,
			"only rows above the rollback point may be returned",
		)
	}
}

// BenchmarkGetUtxosAddedAfterSlot measures the blob-collect statement in both
// forms against the same seeded table, the timing counterpart to the plan
// assertion above.
func BenchmarkGetUtxosAddedAfterSlot(b *testing.B) {
	for _, n := range []int{100_000, 500_000} {
		const rolledBackFrom = int64(4_000_000)
		store := newMigratedSQLiteStore(b)
		seedRollbackUtxos(b, store, n, 512, rolledBackFrom, 40)
		query := namedQueryFromSource(b, "GetUtxosAddedAfterSlot")
		legacyQuery := withOrderByIDDesc(b, query)
		for _, tc := range []struct {
			name  string
			query string
		}{
			{name: "order_by_added_slot", query: query},
			{name: "order_by_id", query: legacyQuery},
		} {
			b.Run(fmt.Sprintf("n=%d/%s", n, tc.name), func(b *testing.B) {
				for b.Loop() {
					rows, err := store.writeDB.Query(tc.query, rolledBackFrom)
					if err != nil {
						b.Fatal(err)
					}
					count := 0
					for rows.Next() {
						count++
					}
					if err := rows.Err(); err != nil {
						b.Fatal(err)
					}
					if err := rows.Close(); err != nil {
						b.Fatal(err)
					}
					if count != 40 {
						b.Fatalf("got %d rows, want 40", count)
					}
				}
			})
		}
	}
}

// legacyUtxoStakeRefsQuery rebuilds the OR-of-pairs statement
// queryUtxoStakeRefs used to run: a per-(tx_id,
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
// off that index instead and visits the whole table. The
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

// TestDedupeUtxoIDs proves GetUtxosByRefs' input deduplication removes
// repeated (Hash, Idx) pairs, including a repeat that would otherwise land
// in a different 400-ref chunk, while preserving order of first occurrence
// and leaving distinct refs (including a same-hash-different-index pair)
// untouched.
func TestDedupeUtxoIDs(t *testing.T) {
	hashA := []byte{0x01, 0x02, 0x03}
	hashB := []byte{0x04, 0x05, 0x06}

	ids := []models.UtxoId{
		{Hash: hashA, Idx: 0},
		{Hash: hashB, Idx: 0},
		{Hash: hashA, Idx: 0}, // duplicate of the first
		{Hash: hashA, Idx: 1}, // same hash, different index: distinct
	}

	got := dedupeUtxoIDs(ids)
	require.Equal(t, []models.UtxoId{
		{Hash: hashA, Idx: 0},
		{Hash: hashB, Idx: 0},
		{Hash: hashA, Idx: 1},
	}, got)
}

// TestDedupeUtxoIDs_CrossChunkDuplicate proves a duplicate ref is removed
// even when the two occurrences would fall into different 400-ref chunks
// inside GetUtxosByRefs.
func TestDedupeUtxoIDs_CrossChunkDuplicate(t *testing.T) {
	dup := models.UtxoId{Hash: []byte{0xAA}, Idx: 42}

	ids := make([]models.UtxoId, 0, 401)
	ids = append(ids, dup)
	for i := range 399 {
		ids = append(ids, models.UtxoId{
			Hash: []byte{byte(i), byte(i >> 8)},
			Idx:  uint32(i),
		})
	}
	// Placed at index 400, past the first 400-ref chunk boundary.
	ids = append(ids, dup)

	got := dedupeUtxoIDs(ids)
	require.Len(t, got, 400, "cross-chunk duplicate should be removed")

	count := 0
	for _, id := range got {
		if id.Idx == dup.Idx && string(id.Hash) == string(dup.Hash) {
			count++
		}
	}
	require.Equal(t, 1, count, "duplicate ref must appear exactly once")
}

// TestAddUtxosRejectsOverflowAssetWithoutMutation covers the acceptance
// criterion that a rejected conversion must not mutate metadata: given a
// UTxO carrying a native-asset amount that overflows uint64,
// AddUtxos must fail and leave the utxo/asset tables empty, not insert a
// row with a silently wrapped amount.
func TestAddUtxosRejectsOverflowAssetWithoutMutation(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)

	var policyId lcommon.Blake2b224
	policyId[0] = 0xee
	overflowAmount := new(big.Int).SetUint64(math.MaxUint64)
	overflowAmount.Add(overflowAmount, big.NewInt(1))
	multiAsset := lcommon.NewMultiAsset[lcommon.MultiAssetTypeOutput](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeOutput{
			policyId: {
				cbor.NewByteString([]byte("asset")): overflowAmount,
			},
		},
	)

	utxo := ledger.Utxo{
		Id: shelley.NewShelleyTransactionInput(
			"0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
			0,
		),
		Output: &mary.MaryTransactionOutput{
			OutputAmount: mary.MaryTransactionOutputValue{
				Amount: 1_000_000,
				Assets: &multiAsset,
			},
		},
	}

	err := store.AddUtxos(
		[]models.UtxoSlot{{Utxo: utxo, Slot: 1}},
		nil,
	)
	require.Error(t, err)

	var utxoCount, assetCount int
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM utxo",
	).Scan(&utxoCount))
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM asset",
	).Scan(&assetCount))
	require.Equal(t, 0, utxoCount)
	require.Equal(t, 0, assetCount)
}

// TestGetUtxosByAddressRequiresPositiveMaxResults proves maxResults is a
// required, explicit bound: callers cannot opt into an unbounded query by
// passing zero or a negative value.
func TestGetUtxosByAddressRequiresPositiveMaxResults(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	pattern := models.UtxoAddressPattern{PaymentPart: []byte("payment")}

	for _, maxResults := range []int{0, -1} {
		_, err := store.GetUtxosByAddress(
			[]models.UtxoAddressPattern{pattern},
			maxResults,
			nil,
		)
		require.Error(t, err)
	}
}

// TestGetUtxosByAddressEnforcesMaxResults proves a broad query that would
// return more than maxResults candidate rows fails with
// models.ErrTooManyUtxoResults instead of silently returning a truncated
// (and therefore wrong) answer, and that raising the bound to cover the
// true result size succeeds.
func TestGetUtxosByAddressEnforcesMaxResults(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)

	paymentKey := []byte("shared-payment-key-28-bytes-")
	const utxoCount = 5
	for i := range utxoCount {
		txId := make([]byte, 32)
		txId[31] = byte(i)
		require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
			TxId:       txId,
			OutputIdx:  0,
			PaymentKey: paymentKey,
			AddedSlot:  uint64(i + 1),
			Amount:     100,
		}))
	}
	pattern := models.UtxoAddressPattern{PaymentPart: paymentKey}

	_, err := store.GetUtxosByAddress(
		[]models.UtxoAddressPattern{pattern},
		utxoCount-1,
		nil,
	)
	require.ErrorIs(t, err, models.ErrTooManyUtxoResults)

	got, err := store.GetUtxosByAddress(
		[]models.UtxoAddressPattern{pattern},
		utxoCount,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, got, utxoCount)
}

// TestGetUtxosByAddressDetectsOverflowAcrossOverlappingChunks proves the
// per-chunk SQL LIMIT stays sound when a physical UTxO can be matched by
// more than one chunk's OR-expression. GetUtxosByAddress deduplicates
// candidates by (tx id, output index) across chunks, so an earlier fix
// shrunk each later chunk's limit by the deduplicated count already
// collected (maxResults - len(ret) + 1); that is unsound here: a chunk
// full of already-seen duplicates could leave no budget left to see that
// same chunk's own, still-unseen matches, silently returning an
// incomplete answer instead of detecting the overflow. The fix uses a
// fixed maxResults+1 limit on every chunk instead.
//
// This forces multiple, overlapping chunks the same way
// TestUtxosByAddressManyZeroArgBranches does: a run of Byron patterns
// whose payment and staking hash both decode as zero falls back to a
// zero-argument branch (see AppendUtxoAddressPatternOrBranch), so
// GetUtxosByAddress's chunking -- which flushes on branch count as well
// as bind-argument count -- splits before the parameter limit would ever
// be reached. Every such branch matches the one "shared" UTxO seeded
// below regardless of which chunk it lands in, while three additional,
// genuinely distinct UTxOs are matched only by their own PaymentPart
// pattern in the final chunk alongside a run of that same filler.
func TestGetUtxosByAddressDetectsOverflowAcrossOverlappingChunks(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)

	// The shared UTxO: no payment/staking key, matched by every
	// zero-argument filler branch.
	sharedTxId := make([]byte, 32)
	sharedTxId[31] = 0xee
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:      sharedTxId,
		OutputIdx: 0,
		AddedSlot: 1,
		Amount:    1,
	}))

	// Three additional, genuinely distinct UTxOs.
	const distinctCount = 3
	distinctKeys := make([][]byte, distinctCount)
	for i := range distinctKeys {
		key := bytes.Repeat([]byte{byte(i + 1)}, lcommon.AddressHashSize)
		distinctKeys[i] = key
		txId := make([]byte, 32)
		txId[31] = byte(i + 1)
		require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
			TxId:       txId,
			OutputIdx:  0,
			PaymentKey: key,
			AddedSlot:  uint64(i + 2),
			Amount:     100,
		}))
	}

	// Enough zero-argument filler branches to force at least one
	// branch-count-triggered chunk flush (paramLimit/2 == 499 for
	// SQLite), followed by the three distinct-address patterns so they
	// land in a later chunk alongside more of the same filler.
	zeroPayment := bytes.Repeat([]byte{0x00}, lcommon.AddressHashSize)
	const fillerCount = 600
	patterns := make(
		[]models.UtxoAddressPattern, 0, fillerCount+distinctCount,
	)
	for i := range fillerCount {
		payload := make([]byte, 4)
		binary.BigEndian.PutUint32(payload, uint32(i)+1)
		derivationPath, err := cbor.Encode(payload)
		require.NoError(t, err)
		addr, err := lcommon.NewByronAddressFromParts(
			0, zeroPayment, lcommon.ByronAddressAttributes{Payload: derivationPath},
		)
		require.NoError(t, err)
		addrBytes, err := addr.Bytes()
		require.NoError(t, err)
		patterns = append(
			patterns,
			models.UtxoAddressPattern{ExactAddress: addrBytes},
		)
	}
	for _, key := range distinctKeys {
		patterns = append(
			patterns,
			models.UtxoAddressPattern{PaymentPart: key},
		)
	}

	// 4 genuinely distinct UTxOs match (the shared one plus the 3
	// PaymentPart-only ones); a maxResults of 2 must be reported as
	// exceeded, not silently answered with an incomplete 2-row result.
	_, err := store.GetUtxosByAddress(patterns, 2, nil)
	require.ErrorIs(t, err, models.ErrTooManyUtxoResults)

	// Raising the bound to cover the true count must return every
	// distinct UTxO exactly once: no unique match dropped by chunking.
	got, err := store.GetUtxosByAddress(patterns, distinctCount+1, nil)
	require.NoError(t, err)
	require.Len(t, got, distinctCount+1)
}

// TestGetUtxosByAddressWithOrderingSkipAssets proves SkipAssets omits a
// row's native assets from the result without affecting which rows are
// returned. Callers that only need row identity or ordering (an
// exact-address candidate scan, a reference-only lookup) use this to avoid
// paying for asset joins that would be immediately discarded.
func TestGetUtxosByAddressWithOrderingSkipAssets(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)

	var policyId lcommon.Blake2b224
	policyId[0] = 0xaa
	multiAsset := lcommon.NewMultiAsset[lcommon.MultiAssetTypeOutput](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeOutput{
			policyId: {
				cbor.NewByteString([]byte("token")): big.NewInt(5),
			},
		},
	)
	utxo := ledger.Utxo{
		Id: shelley.NewShelleyTransactionInput(
			"0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
			0,
		),
		Output: &mary.MaryTransactionOutput{
			OutputAmount: mary.MaryTransactionOutputValue{
				Amount: 1_000_000,
				Assets: &multiAsset,
			},
		},
	}
	require.NoError(
		t,
		store.AddUtxos([]models.UtxoSlot{{Utxo: utxo, Slot: 1}}, nil),
	)

	withAssets, err := store.GetUtxosByAddressWithOrdering(
		&models.UtxoWithOrderingQuery{MatchAllAddresses: true},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, withAssets, 1)
	require.Len(t, withAssets[0].Assets, 1)

	skipped, err := store.GetUtxosByAddressWithOrdering(
		&models.UtxoWithOrderingQuery{
			MatchAllAddresses: true,
			SkipAssets:        true,
		},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, skipped, 1)
	require.Empty(t, skipped[0].Assets)
	// Row identity is unaffected by SkipAssets.
	require.Equal(t, withAssets[0].TxId, skipped[0].TxId)
}

// utxoForInsertCacheTest builds a minimal, valid models.Utxo for exercising
// insertUtxoModel directly: distinct txSeed/outputIdx pairs target distinct
// rows, and the same pair can be reused deliberately to exercise the ON
// CONFLICT DO NOTHING branch.
func utxoForInsertCacheTest(
	txSeed byte,
	outputIdx uint32,
	amount uint64,
) *models.Utxo {
	txID := make([]byte, 32)
	txID[31] = txSeed
	paymentKey := bytes.Repeat([]byte{txSeed}, lcommon.AddressHashSize)
	return &models.Utxo{
		TxId:       txID,
		OutputIdx:  outputIdx,
		PaymentKey: paymentKey,
		AddedSlot:  1,
		Amount:     types.Uint64(amount),
	}
}

// insertUtxoInTxn runs insertUtxoModel inside its own write transaction.
// This is simpler than production, not representative of it: production
// applies many outputs within one shared write transaction --
// LedgerDeltaBatch.apply (ledger/delta.go) applies a whole block batch under
// the single txn ledger/state.go passes it, and genesis import
// (ledger/chainsync.go) inserts the entire Byron and Shelley UTxO set inside
// one txn.Do -- so a caller here that wants to observe the per-transaction
// Tx-scoped statement retention this cache introduces must call
// insertUtxoModel repeatedly against one shared transaction instead; see
// TestInsertUtxoModelBoundsTxScopedStatementRetentionInOneTransaction.
func insertUtxoInTxn(
	t *testing.T,
	store *Store,
	utxo *models.Utxo,
	ignoreConflict bool,
) {
	t.Helper()
	err := store.withWriteTransaction(
		nil,
		func(db queryer, ctx context.Context) error {
			return store.insertUtxoModel(ctx, db, utxo, ignoreConflict)
		},
	)
	require.NoError(t, err)
}

// TestInsertUtxoModelReusesCachedStatementAcrossTransactions proves
// insertUtxoModel's move onto the hot-statement cache (queryRowCached) does
// not change its behavior: distinct inserts still get distinct ids, a
// repeated (tx_id, output_idx) under ignoreConflict still resolves to the
// existing row's id via the ON CONFLICT DO NOTHING + fallback SELECT branch,
// and the stored row round-trips correctly through GetUtxo -- while the
// cached *sql.Stmt for insertUtxoQueryIgnoreConflict is the same object
// across independent write transactions. This exercises independent
// transactions for simplicity; it is not the production access pattern --
// see insertUtxoInTxn's doc comment, and
// TestInsertUtxoModelBoundsTxScopedStatementRetentionInOneTransaction for the
// real multi-output-per-transaction shape.
func TestInsertUtxoModelReusesCachedStatementAcrossTransactions(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	first := utxoForInsertCacheTest(1, 0, 5_000_000)
	insertUtxoInTxn(t, store, first, true)
	require.NotZero(t, first.ID)

	store.stmtMu.Lock()
	cachedBefore := store.stmts[insertUtxoQueryIgnoreConflict]
	store.stmtMu.Unlock()
	require.NotNil(
		t,
		cachedBefore,
		"expected insertUtxoQueryIgnoreConflict to be cached on SQLite",
	)

	second := utxoForInsertCacheTest(2, 0, 7)
	insertUtxoInTxn(t, store, second, true)
	require.NotZero(t, second.ID)
	require.NotEqual(t, first.ID, second.ID)

	store.stmtMu.Lock()
	cachedAfter := store.stmts[insertUtxoQueryIgnoreConflict]
	store.stmtMu.Unlock()
	require.Same(
		t,
		cachedBefore,
		cachedAfter,
		"expected the same cached *sql.Stmt across independent write transactions",
	)

	// Re-inserting the same (tx_id, output_idx) under ignoreConflict must
	// take the ON CONFLICT DO NOTHING branch and resolve to the existing
	// row's id via the fallback SELECT, not error and not create a second
	// row -- exactly like before this query went through the cache.
	dup := utxoForInsertCacheTest(1, 0, 999)
	insertUtxoInTxn(t, store, dup, true)
	require.Equal(
		t,
		first.ID,
		dup.ID,
		"expected ON CONFLICT DO NOTHING to resolve to the existing row's id",
	)

	// Round-trip through the public read path: the row the cached statement
	// wrote back is a correct, complete row, not just "an insert succeeded".
	got, err := store.GetUtxo(first.TxId, first.OutputIdx, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, types.Uint64(5_000_000), got.Amount)

	got2, err := store.GetUtxo(second.TxId, second.OutputIdx, nil)
	require.NoError(t, err)
	require.NotNil(t, got2)
	require.Equal(t, types.Uint64(7), got2.Amount)
}

// TestImportUtxosReusesCachedStatementAcrossTransactions proves the snapshot
// importer uses the same cached insert as the ordinary UTxO path. It also
// exercises the ON CONFLICT DO NOTHING + fallback lookup that assigns the
// existing row ID, preserving idempotent imports.
func TestImportUtxosReusesCachedStatementAcrossTransactions(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	first := *utxoForInsertCacheTest(21, 0, 5_000_000)
	require.NoError(t, store.ImportUtxos([]models.Utxo{first}, nil))
	gotFirst, err := store.GetUtxo(first.TxId, first.OutputIdx, nil)
	require.NoError(t, err)
	require.NotNil(t, gotFirst)

	store.stmtMu.Lock()
	cachedBefore := store.stmts[insertUtxoQueryIgnoreConflict]
	store.stmtMu.Unlock()
	require.NotNil(
		t,
		cachedBefore,
		"expected importer insert to use the cached statement on SQLite",
	)

	second := *utxoForInsertCacheTest(22, 0, 7)
	require.NoError(t, store.ImportUtxos([]models.Utxo{second}, nil))
	gotSecond, err := store.GetUtxo(second.TxId, second.OutputIdx, nil)
	require.NoError(t, err)
	require.NotNil(t, gotSecond)
	require.NotEqual(t, gotFirst.ID, gotSecond.ID)

	store.stmtMu.Lock()
	cachedAfter := store.stmts[insertUtxoQueryIgnoreConflict]
	store.stmtMu.Unlock()
	require.Same(
		t,
		cachedBefore,
		cachedAfter,
		"expected the importer to reuse the same cached statement",
	)

	duplicate := first
	duplicate.Amount = 999
	require.NoError(t, store.ImportUtxos([]models.Utxo{duplicate}, nil))
	gotDuplicate, err := store.GetUtxo(first.TxId, first.OutputIdx, nil)
	require.NoError(t, err)
	require.NotNil(t, gotDuplicate)
	require.Equal(t, gotFirst.ID, gotDuplicate.ID)
	require.Equal(
		t,
		types.Uint64(5_000_000),
		gotDuplicate.Amount,
		"conflicting import must preserve the existing UTxO row",
	)
}

func TestImportUtxosDeferredRewardLiveStakeRefreshRebuildsCorrectly(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	utxo := *utxoForInsertCacheTest(23, 0, 5_000_000)
	utxo.CredentialTag = 0
	utxo.StakingKey = bytes.Repeat([]byte{0x23}, lcommon.AddressHashSize)

	require.NoError(t, store.ImportUtxosDeferredRewardLiveStakeRefresh(
		[]models.Utxo{utxo},
		nil,
	))
	var aggregateCount int
	require.NoError(t, store.writeDB.QueryRowContext(
		context.Background(),
		"SELECT COUNT(*) FROM reward_live_stake",
	).Scan(&aggregateCount))
	require.Zero(t, aggregateCount,
		"deferred import must not refresh the aggregate per batch")

	require.NoError(t, store.RebuildRewardLiveStake(utxo.AddedSlot, nil))
	var utxoStake string
	require.NoError(t, store.writeDB.QueryRowContext(
		context.Background(),
		"SELECT utxo_stake FROM reward_live_stake WHERE credential_tag = 0 AND staking_key = ?",
		utxo.StakingKey,
	).Scan(&utxoStake))
	require.Equal(t, "5000000", utxoStake)
}

// TestImportUtxosBoundsTxScopedStatementRetentionInOneTransaction exercises
// the production shape: one import batch keeps a write transaction open while
// it inserts many outputs. Reusing one Tx-scoped derivative keeps database/sql
// from retaining one prepared statement per imported output.
func TestImportUtxosBoundsTxScopedStatementRetentionInOneTransaction(
	t *testing.T,
) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()

	const outputCount = 2_000
	utxos := make([]models.Utxo, outputCount)
	for i := range outputCount {
		utxos[i] = *utxoForBenchmarkIteration(uint64(i + 1))
	}

	txn := store.Transaction(ctx)
	sqlTransaction, ok := txn.(*sqlTxn)
	require.True(t, ok)
	require.NoError(t, sqlTransaction.beginErr)
	require.NoError(t, store.ImportUtxos(utxos, txn))

	retained := retainedTxStmtCount(t, sqlTransaction.tx)
	require.NoError(t, txn.Commit())
	require.LessOrEqual(
		t,
		retained,
		1,
		"expected one Tx-scoped importer statement for %d outputs, got %d",
		outputCount,
		retained,
	)
}

// TestMergeStakeCredentialDeltasSumsPerCredential proves
// mergeStakeCredentialDeltas adds every occurrence's delta for a credential
// (unlike mergeStakeCredentialRefs, which keeps only the first), and that
// order follows first occurrence.
func TestMergeStakeCredentialDeltasSumsPerCredential(t *testing.T) {
	t.Parallel()

	a := models.NewStakeCredentialRef(0, []byte("credential-a"))
	b := models.NewStakeCredentialRef(0, []byte("credential-b"))

	t.Run("all empty", func(t *testing.T) {
		t.Parallel()
		got := mergeStakeCredentialDeltas(nil, []stakeCredentialDelta{}, nil)
		require.Empty(t, got)
	})

	t.Run(
		"sums overlapping deltas across and within slices",
		func(t *testing.T) {
			t.Parallel()
			got := mergeStakeCredentialDeltas(
				[]stakeCredentialDelta{{ref: a, delta: 5}, {ref: b, delta: -2}},
				[]stakeCredentialDelta{{ref: a, delta: -3}},
				[]stakeCredentialDelta{{ref: a, delta: 10}, {ref: b, delta: 1}},
			)
			byKey := make(map[string]int64, len(got))
			for _, d := range got {
				byKey[d.ref.MapKey()] = d.delta
			}
			require.Equal(t, int64(12), byKey[a.MapKey()]) // 5 - 3 + 10
			require.Equal(t, int64(-1), byKey[b.MapKey()]) // -2 + 1
			require.Len(t, got, 2)
		},
	)
}

// TestQueryUtxoStakeConsumedDeltasNegatesAmounts proves
// queryUtxoStakeConsumedDeltas reports the exact negative delta for each
// spent input's credential, grouping and summing when several consumed
// inputs share a credential.
func TestQueryUtxoStakeConsumedDeltasNegatesAmounts(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	refA := models.NewStakeCredentialRef(0, credentialKeyForIndex(0))
	refB := models.NewStakeCredentialRef(0, credentialKeyForIndex(1))

	seedCredentialUtxos(t, store, 0, refA, []uint64{4_000_000, 6_000_000}, nil)
	seedCredentialUtxos(t, store, 1, refB, []uint64{9_000_000}, nil)

	ids := []models.UtxoId{
		{Hash: utxoTxIDForGroup(0, 0), Idx: 0},
		{Hash: utxoTxIDForGroup(0, 1), Idx: 0},
		{Hash: utxoTxIDForGroup(1, 0), Idx: 0},
	}
	deltas, err := store.queryUtxoStakeConsumedDeltas(ctx, store.writeDB, ids)
	require.NoError(t, err)

	byKey := make(map[string]int64, len(deltas))
	for _, d := range deltas {
		byKey[d.ref.MapKey()] = d.delta
	}
	require.Equal(t, int64(-10_000_000), byKey[refA.MapKey()])
	require.Equal(t, int64(-9_000_000), byKey[refB.MapKey()])
	require.Len(t, deltas, 2)
}

// utxoTxIDForGroup reproduces seedCredentialUtxos' tx_id derivation from
// (group, index) so a test can look its seeded rows back up by UtxoId.
func utxoTxIDForGroup(group, index int) []byte {
	txID := make([]byte, 32)
	txID[0] = byte(group >> 24)
	txID[1] = byte(group >> 16)
	txID[2] = byte(group >> 8)
	txID[3] = byte(group)
	txID[4] = byte(index >> 24)
	txID[5] = byte(index >> 16)
	txID[6] = byte(index >> 8)
	txID[7] = byte(index)
	return txID
}

func TestExactAddressOrderingUsesPaymentIndex(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		0,
		bytes.Repeat([]byte{1}, 28),
		nil,
	)
	require.NoError(t, err)
	pattern, err := models.ExactUtxoAddressPattern(address)
	require.NoError(t, err)
	predicate, args, err := utxoOrderingPredicate(
		&models.UtxoWithOrderingQuery{
			AddressPatterns: []models.UtxoAddressPattern{pattern},
		},
		true,
	)
	require.NoError(t, err)
	plan := queryPlan(
		t,
		store.writeDB,
		"SELECT utxo.id FROM utxo WHERE "+predicate,
		args...)
	require.Contains(t, plan, "idx_utxo_payment_key")
	require.NotContains(t, plan, "idx_utxo_deleted_payment_script")
	require.NotContains(t, plan, "SCAN utxo")
}

// TestGetUtxosByAddressAsOfSelectsRowsLiveAtSlot checks GetUtxosByAddressAsOf
// against GetUtxosByRefsAsOf's predicate: a row counts when it was added at
// or before atSlot and spent strictly after it, or never.
func TestGetUtxosByAddressAsOfSelectsRowsLiveAtSlot(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)

	key := bytes.Repeat([]byte{0x5A}, lcommon.AddressHashSize)
	for i, r := range []struct{ added, deleted uint64 }{
		{100, 0},     // included
		{100, 3_000}, // spent after atSlot: included
		{100, 2_000}, // spent at atSlot: excluded
		{3_000, 0},   // added after atSlot: excluded
	} {
		txId := make([]byte, 32)
		txId[31] = byte(i + 1)
		require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
			TxId:        txId,
			OutputIdx:   0,
			PaymentKey:  key,
			AddedSlot:   r.added,
			DeletedSlot: r.deleted,
			Amount:      types.Uint64(i + 1),
		}))
	}
	patterns := []models.UtxoAddressPattern{{PaymentPart: key}}

	got, err := store.GetUtxosByAddressAsOf(patterns, 2_000, 10, nil)
	require.NoError(t, err)
	amounts := make([]uint64, 0, len(got))
	for _, u := range got {
		amounts = append(amounts, uint64(u.Amount))
	}
	require.ElementsMatch(t, []uint64{1, 2}, amounts)

	live, err := store.GetUtxosByAddress(patterns, 10, nil)
	require.NoError(t, err)
	liveAmounts := make([]uint64, 0, len(live))
	for _, u := range live {
		liveAmounts = append(liveAmounts, uint64(u.Amount))
	}
	require.ElementsMatch(
		t, []uint64{1, 4}, liveAmounts,
		"live: the never-spent row and the one added later",
	)

	_, err = store.GetUtxosByAddressAsOf(patterns, math.MaxUint64, 10, nil)
	require.Error(t, err, "a slot above math.MaxInt64 must be rejected")
}

// TestGetUtxosByAddressAsOfCountsSlotArgsInChunkBudget fills a chunk to the
// last bind parameter SQLite allows: 248 base-address branches of four
// arguments and three enterprise branches of two reach 998 address arguments
// unless the two slot arguments are reserved, which with the LIMIT argument
// would exceed the 999-variable limit.
func TestGetUtxosByAddressAsOfCountsSlotArgsInChunkBudget(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	limit := store.dialect.ParameterLimit()
	require.Equal(t, 999, limit)
	limitSQLiteVariableNumber(t, store.readDB, limit)

	hash := func(prefix byte, i int) []byte {
		h := make([]byte, lcommon.AddressHashSize)
		h[0] = prefix
		binary.BigEndian.PutUint32(h[1:], uint32(i))
		return h
	}
	var patterns []models.UtxoAddressPattern
	add := func(addr lcommon.Address) {
		addrBytes, err := addr.Bytes()
		require.NoError(t, err)
		patterns = append(
			patterns,
			models.UtxoAddressPattern{ExactAddress: addrBytes},
		)
	}
	for i := range 248 {
		addr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeKeyKey,
			lcommon.AddressNetworkTestnet,
			hash(0x01, i),
			hash(0x02, i),
		)
		require.NoError(t, err)
		add(addr)
	}
	for i := range 3 {
		addr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeKeyNone,
			lcommon.AddressNetworkTestnet,
			hash(0x03, i),
			nil,
		)
		require.NoError(t, err)
		add(addr)
	}

	_, err := store.GetUtxosByAddressAsOf(patterns, 1_000, 10, nil)
	require.NoError(t, err)
}
