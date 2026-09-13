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
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	_ "github.com/glebarez/go-sqlite"
	"github.com/stretchr/testify/require"
)

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
// ANALYZE at the points added by #2367 (after a Mithril import, before
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
// stats are stale or absent (#2367 runs ANALYZE only around a Mithril import),
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
	require.Zero(t, above, "rollback must delete every utxo added after the slot")

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
