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
		require.NoError(t, store.Close())
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

	assertIndexed := func(t *testing.T, when string) {
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

	assertIndexed(t, "before deferring indexes")
	require.NoError(t, store.DropDeferredIndexes())
	assertIndexed(t, "after DropDeferredIndexes")
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
	controlRatio := ratio(control)
	growth := float64(slices.Max(steps)) / float64(slices.Min(steps))

	require.Greater(
		t,
		controlRatio,
		growth/2,
		"control (three transaction_id indexes dropped) must show the "+
			"reported decay, or this sweep is too small to measure "+
			"anything: rows grew %.0fx, cost grew %.1fx (fixed %v, "+
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

	store, db := newAPIModeSQLStore(t)
	require.NoError(t, store.DropDeferredIndexes())

	blockHash := bytes.Repeat([]byte{0x5d}, 32)
	seeded := 0
	written := 0
	medians := make([]time.Duration, 0, len(steps))
	for _, step := range steps {
		seedWitnessRows(t, db, seeded, step)
		seeded = step
		samples := make([]time.Duration, 0, timed)
		for range timed {
			transaction := newTestWitnessTransaction(
				fmt.Sprintf("deferred-index-sweep-%07d", written),
			)
			point := ocommon.Point{
				Slot: uint64(timedSlotBase + written),
				Hash: blockHash,
			}
			start := time.Now()
			require.NoError(t, store.SetTransaction(
				transaction, point, 0, nil, true, nil,
			))
			samples = append(samples, time.Since(start))
			written++
		}
		slices.Sort(samples)
		medians = append(medians, samples[len(samples)/2])
	}
	t.Logf("rows=%v median SetTransaction=%v", steps, medians)

	growth := float64(slices.Max(steps)) / float64(slices.Min(steps))
	ratio := float64(medians[len(medians)-1]) / float64(medians[0])
	require.Less(
		t,
		ratio,
		1.5,
		"per-transaction write cost must not track the rows already "+
			"written: rows grew %.0fx, median cost grew %.1fx (%v)",
		growth, ratio, medians,
	)
}
