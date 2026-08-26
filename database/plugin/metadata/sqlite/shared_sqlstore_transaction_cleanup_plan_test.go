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

package sqlite

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// transactionWitnessCleanupIndexes names the index that must answer
// TransactionWitnessCleanupSQL for each witness table.
//
// Named here rather than in the store: nothing in the statement mentions an
// index, since which one answers it is the planner's choice, and that choice
// is what these tests check.
var transactionWitnessCleanupIndexes = map[string]string{
	"key_witness":     "idx_key_witness_transaction_id",
	"witness_scripts": "idx_witness_scripts_transaction_id",
	"redeemer":        "idx_redeemer_transaction_id",
	"plutus_data":     "idx_plutus_data_transaction_id",
}

// newAPIModeSQLStore opens an API-mode store on its own data directory. Only
// API mode writes the witness tables at all, so it is the only mode where the
// cleanup deletes run.
func newAPIModeSQLStore(t *testing.T) (*sqlstore.Store, *sql.DB) {
	t.Helper()
	store, writeDB, _, err := openSQLStore(
		Config{DataDir: t.TempDir()},
		metadata.ProviderDependencies{StorageMode: types.StorageModeAPI},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() {
		// Logged rather than asserted: require calls FailNow, which stops
		// the remaining cleanup callbacks and leaks the t.TempDir removal
		// registered before this one.
		if err := store.Close(); err != nil {
			t.Logf("closing store: %v", err)
		}
	})
	return store, writeDB
}

// queryPlan returns the planner's description of stmt, one node per line.
//
// The statement carries a bound parameter, so a value is supplied even though
// EXPLAIN QUERY PLAN never runs the DELETE it describes.
func queryPlan(t *testing.T, db *sql.DB, stmt string, args ...any) string {
	t.Helper()
	rows, err := db.Query("EXPLAIN QUERY PLAN "+stmt, args...)
	require.NoError(t, err)
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notUsed, &detail))
		plan.WriteString(detail)
		plan.WriteString("\n")
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, plan.String(), "the planner must describe %q", stmt)
	return plan.String()
}

// seedWitnessRows adds one transaction, and one row in each witness table, for
// every slot in [from, to).
//
// Written as set-based SQL rather than through SetTransaction so the planner
// tests can prepare representative witness tables without spending the whole
// test budget on inserts. The columns and foreign keys are the ones the store
// writes; slot doubles as the seed ordinal so each call can extend the prior
// range.
func seedWitnessRows(t *testing.T, db *sql.DB, from, to int) {
	t.Helper()
	_, err := db.Exec(`
WITH RECURSIVE seq(n) AS (
    SELECT ?
    UNION ALL
    SELECT n + 1 FROM seq WHERE n + 1 < ?
)
INSERT INTO "transaction" (
    hash, block_hash, slot, type, fee, collateral_fee, ttl, block_index, valid
)
SELECT CAST(n AS BLOB), CAST(n AS BLOB), n, 0, '0', '0', '0', 0, TRUE
FROM seq`, from, to)
	require.NoError(t, err)
	for _, statement := range []string{
		`INSERT INTO key_witness (vkey, signature, transaction_id, type)
SELECT hash, hash, id, 0 FROM "transaction" WHERE slot >= ? AND slot < ?`,
		`INSERT INTO witness_scripts (script_hash, transaction_id, type)
SELECT hash, id, 0 FROM "transaction" WHERE slot >= ? AND slot < ?`,
		`INSERT INTO redeemer (
    data, transaction_id, ex_units_memory, ex_units_cpu, "index", tag
)
SELECT hash, id, 0, 0, 0, 0 FROM "transaction" WHERE slot >= ? AND slot < ?`,
		`INSERT INTO plutus_data (data, transaction_id)
SELECT hash, id FROM "transaction" WHERE slot >= ? AND slot < ?`,
	} {
		_, err := db.Exec(statement, from, to)
		require.NoError(t, err)
	}
	// Mirror node.RunPlannerStats, which Mithril runs immediately before
	// backfill: the plan asserted below has to be the plan the planner picks
	// with statistics present, not the one it picks in their absence.
	_, err = db.Exec("ANALYZE")
	require.NoError(t, err)
}

// TestTransactionWitnessCleanupStaysIndexedAfterDeferredIndexDrop covers issue
// #3253.
//
// Mithril drops the deferred-index manifest before API-mode historical
// backfill, and backfill then calls SetTransaction for every transaction it
// replays. Each of those calls clears the four witness tables by
// transaction_id first. With the manifest deferring the transaction_id index on
// three of those tables, each delete became a full scan of a table that grows
// with every transaction written, so per-transaction cost rose with the row
// count already present: measured on preview, backfill fell from 3311 to 9
// blocks/sec and its own ETA climbed from 30m to 177h.
//
// The plan is asserted rather than the index's existence: an index the planner
// does not choose is a write cost with no read benefit, and it is EXPLAINed
// from the store's own exported statement so the plan pinned here is the plan
// of the delete that actually runs.
func TestTransactionWitnessCleanupStaysIndexedAfterDeferredIndexDrop(
	t *testing.T,
) {
	t.Parallel()
	store, db := newAPIModeSQLStore(t)
	seedWitnessRows(t, db, 0, 2000)

	requireWitnessCleanupIndexed(t, db, "before deferring indexes")
	require.NoError(t, store.DropDeferredIndexes())
	requireWitnessCleanupIndexed(t, db, "after DropDeferredIndexes")
}

// TestRetainedIndexesResidentAfterCriticalRebuild covers the repair path a
// node takes when a prior bulk-load cycle was interrupted.
//
// serve calls RepairCriticalDeferredIndexes before it clears sync_status, and
// Mithril sync calls BuildCritical before it clears its own. Both return with
// the store about to accept API writes, and neither waits for the lazy
// remainder that background maintenance finishes later. A database an older
// binary's manifest had dropped a retained transaction_id index from therefore
// has to be repaired by the critical rebuild too: otherwise every
// SetTransaction between that point and the full rebuild clears its witness
// tables with the full scan the retained set exists to prevent.
//
// The state under test is that database, reproduced by dropping the retained
// set out from under a pending marker.
func TestRetainedIndexesResidentAfterCriticalRebuild(t *testing.T) {
	t.Parallel()
	store, db := newAPIModeSQLStore(t)
	seedWitnessRows(t, db, 0, 2000)

	require.NoError(t, store.DropDeferredIndexes())
	dropRetainedIndexes(t, db)
	requireWitnessCleanupScans(t, db)

	require.NoError(t, store.BuildCriticalDeferredIndexes())
	requireRetainedIndexesResident(t, db, "after BuildCriticalDeferredIndexes")
	requireWitnessCleanupIndexed(t, db, "after BuildCriticalDeferredIndexes")

	// The critical rebuild owns only the critical subset, so the marker has
	// to survive it for background maintenance to finish the rest.
	pending, err := store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.True(
		t,
		pending,
		"BuildCriticalDeferredIndexes must leave the marker for the lazy "+
			"remainder",
	)
}

// dropRetainedIndexes removes every deferred.Retained index, reproducing what
// a binary whose manifest still deferred them leaves on disk.
//
// The names are manifest constants, not input, and SQLite takes no bound
// parameter in DDL.
func dropRetainedIndexes(t *testing.T, db *sql.DB) {
	t.Helper()
	require.NotEmpty(t, deferred.Retained)
	for _, index := range deferred.Retained {
		_, err := db.Exec("DROP INDEX IF EXISTS " + index.Name)
		require.NoError(t, err, "dropping retained index %s", index.Name)
	}
}

// requireRetainedIndexesResident asserts that every deferred.Retained index is
// present in the schema.
func requireRetainedIndexesResident(t *testing.T, db *sql.DB, when string) {
	t.Helper()
	for _, index := range deferred.Retained {
		var found int
		require.NoError(t, db.QueryRow(
			`SELECT COUNT(*) FROM sqlite_master
WHERE type = 'index' AND name = ?`,
			index.Name,
		).Scan(&found))
		require.Equal(
			t,
			1,
			found,
			"%s: retained index %s must be resident whenever the store "+
				"can serve writes",
			when,
			index.Name,
		)
	}
}

// requireWitnessCleanupScans asserts the negative case the repair has to fix:
// with the retained set absent, the idempotency deletes are full scans. Without
// it a rebuild that restored nothing would still pass the assertions above if
// the indexes had never been missing.
func requireWitnessCleanupScans(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, table := range sqlstore.TransactionWitnessTables() {
		plan := queryPlan(
			t,
			db,
			sqlstore.TransactionWitnessCleanupSQL(table),
			1,
		)
		require.Contains(
			t,
			plan,
			"SCAN "+table,
			"the simulated interrupted cycle must leave the %s "+
				"idempotency delete unindexed:\n%s",
			table,
			plan,
		)
	}
}

// requireWitnessCleanupIndexed asserts that every witness-table idempotency
// delete resolves transaction_id through its index, quoting the plan and the
// caller's stage description when it does not.
func requireWitnessCleanupIndexed(t *testing.T, db *sql.DB, when string) {
	t.Helper()
	for _, table := range sqlstore.TransactionWitnessTables() {
		index, ok := transactionWitnessCleanupIndexes[table]
		require.True(
			t,
			ok,
			"table %q has no expected cleanup index; a new witness "+
				"table needs its transaction_id index classified for "+
				"bulk load",
			table,
		)
		plan := queryPlan(
			t,
			db,
			sqlstore.TransactionWitnessCleanupSQL(table),
			1,
		)
		// SQLite reports a covering index as "USING COVERING INDEX",
		// so the index name and the equality it resolves are matched
		// rather than one literal spelling of the whole node.
		require.Contains(
			t,
			plan,
			"SEARCH "+table+" USING",
			"%s: the %s idempotency delete must be an indexed search:\n%s",
			when, table, plan,
		)
		require.Contains(
			t,
			plan,
			"INDEX "+index+" (transaction_id=?)",
			"%s: the %s idempotency delete must resolve transaction_id "+
				"through %s:\n%s",
			when, table, index, plan,
		)
		require.NotContains(
			t,
			plan,
			"SCAN "+table,
			"%s: the %s idempotency delete must not scan the table:\n%s",
			when, table, plan,
		)
	}
}

// preChangeDeferredWitnessIndexes names the witness transaction_id indexes a
// binary shipped before issue #3253 still carried in its deferred-index
// manifest, and therefore dropped at the start of every bulk-load cycle.
var preChangeDeferredWitnessIndexes = []string{
	"idx_key_witness_transaction_id",
	"idx_witness_scripts_transaction_id",
	"idx_redeemer_transaction_id",
}

// seedPreChangeDeferredCycle leaves the store in the state a binary whose
// manifest still deferred these three indexes leaves on disk when its cycle is
// interrupted: the indexes dropped, and the durable recovery marker still set.
//
// Dropping the indexes directly is the whole point. The schema migration that
// created them is already recorded complete, so its
// CREATE INDEX IF NOT EXISTS never runs again, and a manifest that no longer
// names them cannot rebuild them either.
func seedPreChangeDeferredCycle(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, index := range preChangeDeferredWitnessIndexes {
		_, err := db.Exec("DROP INDEX IF EXISTS " + index)
		require.NoError(t, err)
		require.False(
			t,
			sqliteIndexExists(t, db, index),
			"%s must be absent for this to test the upgrade path",
			index,
		)
	}
	_, err := db.Exec(
		`INSERT INTO sync_state (sync_key, value) VALUES (?, ?)
		 ON CONFLICT (sync_key) DO UPDATE SET value = excluded.value`,
		deferred.SyncStateKey,
		deferred.SyncStateValue,
	)
	require.NoError(t, err)
}

// TestRetainedIndexesRepairPreChangeDeferredCycle covers the upgrade path for
// issue #3253.
//
// Taking the three witness transaction_id indexes out of the manifest fixes
// databases the fixed binary bootstraps itself, but not one already on disk.
// A binary whose manifest still held them dropped them before backfill and
// rebuilds them only in the full rebuild, and #3253's own reporter ran that
// backfill for hours across restarts, so an interrupted cycle is the expected
// state rather than a corner case. On such a database the newer manifest can
// no longer name the indexes to rebuild them and migration v1 is recorded
// complete, so without the repair the full scans this fix removes become
// permanent.
//
// Both entry points a restarted node takes are covered: another bulk-load
// cycle, and the pending-marker repair a plain serve runs.
func TestRetainedIndexesRepairPreChangeDeferredCycle(t *testing.T) {
	t.Parallel()
	for name, repair := range map[string]func(*sqlstore.Store) error{
		"next bulk-load cycle": (*sqlstore.Store).DropDeferredIndexes,
		"pending-marker repair": (*sqlstore.Store).
			BuildDeferredIndexes,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store, db := newAPIModeSQLStore(t)
			seedWitnessRows(t, db, 0, 500)
			seedPreChangeDeferredCycle(t, db)

			require.NoError(t, repair(store))

			for _, index := range preChangeDeferredWitnessIndexes {
				require.True(
					t,
					sqliteIndexExists(t, db, index),
					"%s must be restored: nothing else recreates an index "+
						"the manifest no longer names",
					index,
				)
			}
			requireWitnessCleanupIndexed(t, db, "after "+name)
		})
	}
}

// TestBuildDeferredIndexesKeepsMarkerUntilRetainedIndexesExist pins the
// ordering the repair depends on: the durable marker asserts that every index
// an older manifest may have dropped is back, so it may not be cleared while
// one of them is still missing.
func TestBuildDeferredIndexesKeepsMarkerUntilRetainedIndexesExist(
	t *testing.T,
) {
	t.Parallel()
	store, db := newAPIModeSQLStore(t)
	seedPreChangeDeferredCycle(t, db)

	pending, err := store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.True(t, pending, "the seeded cycle must look interrupted")

	require.NoError(t, store.BuildCriticalDeferredIndexes())
	pending, err = store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.True(
		t,
		pending,
		"the critical rebuild must leave the marker for the full rebuild",
	)

	require.NoError(t, store.BuildDeferredIndexes())
	for _, index := range preChangeDeferredWitnessIndexes {
		require.True(t, sqliteIndexExists(t, db, index))
	}
	pending, err = store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.False(
		t,
		pending,
		"the marker must clear once the full manifest and the retained "+
			"indexes are present",
	)
}
