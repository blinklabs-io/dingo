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
	"bytes"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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
// Written as set-based SQL rather than through SetTransaction so a cardinality
// sweep can reach a table size where a full scan is distinguishable from an
// index descent without spending the whole test budget on inserts. The columns
// and foreign keys are the ones the store writes; slot doubles as the seed
// ordinal so each step can extend the previous one.
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

// witnessCleanupSweep returns the median duration of one full witness-cleanup
// pass at each cardinality in steps.
//
// Every delete targets a transaction_id that owns no rows, so the pass removes
// nothing and the table size stays fixed across repetitions. That makes each
// measurement the cost of locating the (absent) rows and nothing else: a
// bounded number of b-tree descents when the predicate column is indexed, a
// full pass over the table when it is not.
func witnessCleanupSweep(
	t *testing.T,
	db *sql.DB,
	steps []int,
	repetitions int,
) []time.Duration {
	t.Helper()
	tables := sqlstore.TransactionWitnessTables()
	statements := make([]*sql.Stmt, 0, len(tables))
	for _, table := range tables {
		stmt, err := db.Prepare(sqlstore.TransactionWitnessCleanupSQL(table))
		require.NoError(t, err)
		defer stmt.Close()
		statements = append(statements, stmt)
	}
	medians := make([]time.Duration, 0, len(steps))
	seeded := 0
	for _, step := range steps {
		seedWitnessRows(t, db, seeded, step)
		seeded = step
		samples := make([]time.Duration, 0, repetitions)
		for i := range repetitions {
			// An id past every seeded transaction: present in no witness
			// table, so the delete matches nothing at any cardinality.
			missing := step*2 + i
			start := time.Now()
			for _, stmt := range statements {
				_, err := stmt.Exec(missing)
				require.NoError(t, err)
			}
			samples = append(samples, time.Since(start))
		}
		slices.Sort(samples)
		medians = append(medians, samples[len(samples)/2])
	}
	return medians
}

// TestTransactionWitnessCleanupCostFlatAcrossCardinality is the cardinality
// sweep behind issue #3253: per-transaction cleanup cost must not grow with the
// number of transactions already written.
//
// The absolute durations are not asserted -- a shared machine makes any
// wall-clock threshold a coin flip. What is asserted is the growth ratio across
// an eightfold increase in rows, measured against a control store in the same
// process that additionally drops the three indexes this fix retains. The
// control both reproduces the reported decay and proves the measurement can
// see it, so a flat result from the fixed store is a real result rather than a
// sweep too small to distinguish the two.
func TestTransactionWitnessCleanupCostFlatAcrossCardinality(t *testing.T) {
	t.Parallel()
	steps := []int{500, 1000, 2000, 4000}
	const repetitions = 21

	fixedStore, fixedDB := newAPIModeSQLStore(t)
	require.NoError(t, fixedStore.DropDeferredIndexes())

	controlStore, controlDB := newAPIModeSQLStore(t)
	require.NoError(t, controlStore.DropDeferredIndexes())
	for table, index := range transactionWitnessCleanupIndexes {
		if table == "plutus_data" {
			// Retained by the manifest all along, and the control for the
			// three that were not: leaving it indexed keeps the two stores
			// different in exactly the three indexes under test.
			continue
		}
		_, err := controlDB.Exec("DROP INDEX IF EXISTS " + index)
		require.NoError(t, err)
	}

	fixed := witnessCleanupSweep(t, fixedDB, steps, repetitions)
	control := witnessCleanupSweep(t, controlDB, steps, repetitions)
	t.Logf("rows=%v fixed=%v control=%v", steps, fixed, control)

	ratio := func(medians []time.Duration) float64 {
		return float64(medians[len(medians)-1]) / float64(medians[0])
	}
	fixedRatio := ratio(fixed)
	growth := float64(slices.Max(steps)) / float64(slices.Min(steps))

	// Both stores execute the same four statements per pass, so both pay the
	// same cardinality-independent cost per pass: statement preparation and
	// one commit per table, which no index changes. The fixed store's median
	// is that cost plus a bounded number of b-tree descents, which is what
	// makes it a usable estimate of the floor. The control's growth is
	// therefore measured on the difference -- the scan component, the only
	// term the row count drives. Dividing the raw medians instead mixes the
	// floor into both ends of the ratio and understates the decay whenever
	// the floor is large, which on a loaded machine it is.
	scanCost := func(step int) float64 {
		return float64(control[step] - fixed[step])
	}
	// Read the sensitivity check at the largest cardinality, where the scan
	// term is widest and clears the timing floor by the largest margin. At the
	// smallest cardinality it asks a 500-row scan to be measurable while the
	// other sweeps in this package compete for the same CPU, and the two
	// sweeps are run one after the other rather than interleaved, so the
	// difference there has been observed going negative on an unchanged store.
	require.Positive(
		t,
		scanCost(len(control)-1),
		"the control must be slower than the fixed store once the tables are "+
			"large, or the two stores are not differing in the three indexes "+
			"under test: fixed %v, control %v",
		fixed, control,
	)
	// Growth of the control's own medians. Dividing the scan-cost difference
	// by its value at the smallest cardinality put a floor-dominated, possibly
	// negative term in the denominator, which inverts the ratio rather than
	// understating it.
	controlRatio := float64(control[len(control)-1]) / float64(control[0])

	// Bounded at 2x rather than near-linear: this ratio is taken on the raw
	// control medians, which carry the cardinality-independent floor at both
	// ends and so understate the scan growth. A store whose cleanup cost did
	// not track its row count sits near 1x; the observed range here is 3.4x to
	// 4.4x across an eightfold spread, which straddles growth/2 and is what
	// makes that threshold the wrong one for a floor-inclusive ratio.
	require.Greater(
		t,
		controlRatio,
		2.0,
		"control (three transaction_id indexes dropped) must show the "+
			"reported decay, or this sweep is too small to measure "+
			"anything: rows grew %.0fx, control cost grew %.1fx (fixed %v, "+
			"control %v)",
		growth, controlRatio, fixed, control,
	)
	require.Less(
		t,
		fixedRatio,
		3.0,
		"cleanup cost must stay flat as rows grow: rows grew %.0fx, cost "+
			"grew %.1fx (fixed %v)",
		growth, fixedRatio, fixed,
	)
	require.Less(
		t,
		fixedRatio*2,
		controlRatio,
		"the retained indexes must measurably flatten the curve: fixed "+
			"grew %.1fx, control grew %.1fx",
		fixedRatio, controlRatio,
	)
}

// TestSetTransactionCostFlatAfterDeferredIndexDrop widens the sweep above from
// the witness cleanup to the whole API-mode write path, on the schema Mithril
// backfill actually runs against: every deferred index dropped, statistics
// analyzed, transactions arriving one after another.
//
// The narrow sweep proves the three retained indexes fix the deletes they were
// added for. This one is the class check behind it: no predicate the
// per-transaction write path filters on may be answered by an index the
// manifest defers, and the only way to see that is to write transactions
// through the store and watch the cost as the tables grow.
//
// The tables are grown with the same set-based seeding the narrow sweep uses,
// and only a fixed window of transactions is timed at each cardinality. Cost
// here is a function of the rows already present, not of how they arrived, so
// seeding buys the eightfold spread that makes the difference visible without
// spending the test budget writing rows nobody measures.
func TestSetTransactionCostFlatAfterDeferredIndexDrop(t *testing.T) {
	t.Parallel()
	steps := []int{2000, 4000, 8000, 16000}
	// Timed transactions per cardinality. Odd, so the median is a sample.
	const timed = 121
	// Slots well clear of the seeded range, which seedWitnessRows selects by
	// slot when it attaches witness rows.
	const timedSlotBase = 1_000_000

	fixedStore, fixedDB := newAPIModeSQLStore(t)
	require.NoError(t, fixedStore.DropDeferredIndexes())

	controlStore, controlDB := newAPIModeSQLStore(t)
	require.NoError(t, controlStore.DropDeferredIndexes())
	for _, index := range preChangeDeferredWitnessIndexes {
		_, err := controlDB.Exec("DROP INDEX IF EXISTS " + index)
		require.NoError(t, err)
	}

	fixed := setTransactionSweep(
		t, fixedStore, fixedDB, steps, timed, timedSlotBase, "fixed",
	)
	control := setTransactionSweep(
		t, controlStore, controlDB, steps, timed, timedSlotBase, "control",
	)
	t.Logf("rows=%v fixed=%v control=%v", steps, fixed, control)

	growth := float64(slices.Max(steps)) / float64(slices.Min(steps))
	controlLast := control[len(control)-1]

	// The discriminator is the cost regime at the largest cardinality, not a
	// growth ratio of the fixed store. The fixed store's per-transaction cost
	// has no trend across these steps -- it alternates around a floor set by
	// commit and page-cache behavior -- so its own first-to-last ratio is a
	// ratio of two noise samples and lands either side of 1 from run to run.
	//
	// Both terms are read at the same step so both carry whatever contention
	// the run is under. This package runs several timing sweeps in parallel,
	// and taking the fixed store's worst step instead imports the single most
	// contended phase of the run into a comparison against a control measured
	// under different conditions, which is enough on its own to invert the
	// result.
	require.Greater(
		t,
		float64(controlLast),
		float64(fixed[len(fixed)-1])*3,
		"the control must reach a different cost regime once the tables are "+
			"large, or the two stores are not differing in the three indexes "+
			"under test: fixed %v, control %v",
		fixed, control,
	)
	// Sensitivity: the sweep must be wide enough for the control to show the
	// decay reported in #3253. Without this, a control that was slow at every
	// cardinality for an unrelated reason would satisfy the check above. The
	// bound is modest because the control's smallest step is the most
	// floor-dominated point in the sweep; the separation asserted above is the
	// strong half of this pair.
	require.Greater(
		t,
		float64(controlLast),
		float64(control[0])*1.5,
		"the control must show the reported decay across the sweep, or this "+
			"sweep is too small to measure anything: rows grew %.0fx "+
			"(control %v)",
		growth, control,
	)
	// No bound is asserted on the fixed store's own spread across the sweep.
	// That spread tracks how much of the run each step shared with the other
	// parallel sweeps in this package, not the row count: it has been observed
	// falling from 3.8ms at the smallest cardinality to 0.9ms at the largest,
	// the opposite direction to the effect under test. A store that did track
	// its row count would fail the separation check above, which is where that
	// claim belongs.
}

// setTransactionSweep times a fixed window of SetTransaction calls at each
// cardinality in steps and returns the median per step. The first window is
// preceded by an untimed warmup so the smallest cardinality does not carry
// process warmup the later steps have already paid; charging that cost to the
// smallest row count alone is enough to move a raw median ratio either side of
// 1 on an otherwise unchanged store.
func setTransactionSweep(
	t *testing.T,
	store *sqlstore.Store,
	db *sql.DB,
	steps []int,
	timed int,
	slotBase int,
	tag string,
) []time.Duration {
	t.Helper()
	const warmup = 16
	blockHash := bytes.Repeat([]byte{0x5d}, 32)
	seeded := 0
	written := 0
	write := func() time.Duration {
		transaction := newTestWitnessTransaction(
			fmt.Sprintf("deferred-index-sweep-%s-%07d", tag, written),
		)
		point := ocommon.Point{
			Slot: uint64(slotBase + written),
			Hash: blockHash,
		}
		start := time.Now()
		require.NoError(t, store.SetTransaction(
			transaction, point, 0, nil, true, nil,
		))
		elapsed := time.Since(start)
		written++
		return elapsed
	}
	medians := make([]time.Duration, 0, len(steps))
	for i, step := range steps {
		seedWitnessRows(t, db, seeded, step)
		seeded = step
		if i == 0 {
			for range warmup {
				write()
			}
		}
		samples := make([]time.Duration, 0, timed)
		for range timed {
			samples = append(samples, write())
		}
		slices.Sort(samples)
		medians = append(medians, samples[len(samples)/2])
	}
	return medians
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
