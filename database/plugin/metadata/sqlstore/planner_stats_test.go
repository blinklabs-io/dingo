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
	"database/sql/driver"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"modernc.org/sqlite"
)

// newFilePlannerStatsStore mirrors production's file-backed topology: a
// single-connection WAL write pool and a separate read-only pool.
func newFilePlannerStatsStore(
	tb testing.TB,
	reg prometheus.Registerer,
) *Store {
	tb.Helper()
	uri := "file:" + tb.TempDir() + "/metadata.sqlite"
	writeDB, err := OpenDB(
		"sqlite",
		uri+"?_txlock=immediate&_pragma=busy_timeout(30000)&_pragma=synchronous(OFF)",
		"sqlite",
		false,
	)
	require.NoError(tb, err)
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)
	_, err = writeDB.Exec("PRAGMA journal_mode=WAL")
	require.NoError(tb, err)
	readDB, err := OpenDB(
		"sqlite",
		uri+"?mode=ro&_pragma=busy_timeout(30000)",
		"sqlite",
		false,
	)
	require.NoError(tb, err)
	readDB.SetMaxOpenConns(4)
	readDB.SetMaxIdleConns(4)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(tb, err)
	store, err := New(Config{
		WriteDB:         writeDB,
		ReadDB:          readDB,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
		PromRegistry:    reg,
	})
	require.NoError(tb, err)
	require.NoError(tb, store.Start(context.Background()))
	tb.Cleanup(func() { require.NoError(tb, store.Close()) })
	return store
}

func stat1Rows(tb testing.TB, db *sql.DB) (int, bool) {
	tb.Helper()
	var present int
	require.NoError(tb, db.QueryRow(
		"SELECT COUNT(*) FROM sqlite_schema WHERE name = 'sqlite_stat1'",
	).Scan(&present))
	if present == 0 {
		return 0, false
	}
	var rows int
	require.NoError(tb, db.QueryRow(
		"SELECT COUNT(*) FROM sqlite_stat1",
	).Scan(&rows))
	return rows, true
}

// statsSensitiveSpendSQL is the row-value form of the batched spend UPDATE.
// Its plan depends on planner statistics: without them SQLite drives it from
// a deleted_slot-leading index, with them from tx_id_output_idx. It is kept
// here as a fixed probe so these tests keep observing the statistics refresh
// whatever shape the production query takes.
const statsSensitiveSpendSQL = `UPDATE utxo
SET deleted_slot = ?, spent_at_tx_id = ?
WHERE deleted_slot = 0 AND spent_at_tx_id IS NULL
  AND (tx_id, output_idx) IN ((?,?),(?,?))
RETURNING tx_id, output_idx`

// consumeBatchPlan returns the plan statsSensitiveSpendSQL gets on conn.
func consumeBatchPlan(tb testing.TB, conn *sql.Conn) string {
	tb.Helper()
	zero := make([]byte, 32)
	rows, err := conn.QueryContext(
		context.Background(),
		"EXPLAIN QUERY PLAN "+statsSensitiveSpendSQL,
		int64(1), zero, zero, 0, zero, 1,
	)
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

// poolPlans opens every connection the pool allows at once, so each pooled
// connection reports its own plan, then returns them all.
func poolPlans(tb testing.TB, db *sql.DB, n int) []string {
	tb.Helper()
	conns := make([]*sql.Conn, 0, n)
	defer func() {
		for _, conn := range conns {
			require.NoError(tb, conn.Close())
		}
	}()
	plans := make([]string, 0, n)
	for range n {
		conn, err := db.Conn(context.Background())
		require.NoError(tb, err)
		conns = append(conns, conn)
		plans = append(plans, consumeBatchPlan(tb, conn))
	}
	return plans
}

func TestOptimizePlannerStatsCreatesStat1OnGenesisStyleDatabase(t *testing.T) {
	t.Parallel()
	store := newFilePlannerStatsStore(t, nil)
	seedStakeRefLookupUtxos(t, store, 300, 16)
	_, exists := stat1Rows(t, store.writeDB)
	require.False(t, exists, "fixture must start without sqlite_stat1")

	result, err := store.OptimizePlannerStatsContext(
		context.Background(), PlannerStatsTriggerEpoch,
	)
	require.NoError(t, err)
	require.True(t, result.Supported)
	require.True(t, result.Changed)
	rows, exists := stat1Rows(t, store.writeDB)
	require.True(t, exists, "sqlite_stat1 must exist after the first run")
	require.Positive(t, rows)
	require.Equal(t, rows, result.Stat1Rows)

	again, err := store.OptimizePlannerStatsContext(
		context.Background(), PlannerStatsTriggerEpoch,
	)
	require.NoError(t, err)
	require.True(t, again.Supported)
	require.False(t, again.Changed, "a repeat run must find nothing to do")
}

func TestOptimizePlannerStatsRefreshesPooledReadConnections(t *testing.T) {
	t.Parallel()
	store := newFilePlannerStatsStore(t, nil)
	seedStakeRefLookupUtxos(t, store, 50_000, 256)

	// Park every read connection in the idle pool holding pre-statistics
	// schema state.
	stale := poolPlans(t, store.readDB, 4)
	for _, plan := range stale {
		require.Contains(
			t,
			plan,
			"idx_utxo_deleted",
			"fixture must show the trap",
		)
	}

	result, err := store.OptimizePlannerStatsContext(
		context.Background(), PlannerStatsTriggerEpoch,
	)
	require.NoError(t, err)
	require.True(t, result.Changed)

	for i, plan := range poolPlans(t, store.readDB, 4) {
		require.Contains(
			t,
			plan,
			"tx_id_output_idx",
			"read connection %d: %s",
			i,
			plan,
		)
		require.NotContains(
			t,
			plan,
			"idx_utxo_deleted",
			"read connection %d",
			i,
		)
	}
}

func TestOptimizePlannerStatsRefreshesAfterUpdateOnlyRun(t *testing.T) {
	t.Parallel()
	store := newFilePlannerStatsStore(t, nil)
	seedStakeRefLookupUtxos(t, store, 200, 16)
	first, err := store.OptimizePlannerStatsContext(
		context.Background(), PlannerStatsTriggerStartup,
	)
	require.NoError(t, err)
	require.True(t, first.Changed)
	_ = poolPlans(t, store.readDB, 4)

	// Grow the table far enough that SQLite re-analyzes it: sqlite_stat1
	// already exists, so this run creates no schema object.
	tx, err := store.writeDB.Begin()
	require.NoError(t, err)
	_, err = tx.Exec(
		"WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM c " +
			"WHERE i < 50000) INSERT INTO utxo (tx_id, output_idx, " +
			"staking_key, credential_tag, added_slot, deleted_slot, amount) " +
			"SELECT randomblob(32), i % 4, randomblob(28), 0, i, 0, '1' FROM c",
	)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	second, err := store.OptimizePlannerStatsContext(
		context.Background(), PlannerStatsTriggerEpoch,
	)
	require.NoError(t, err)
	require.True(t, second.Changed, "growth must change the statistics")
	for i, plan := range poolPlans(t, store.readDB, 4) {
		require.Contains(
			t,
			plan,
			"tx_id_output_idx",
			"read connection %d: %s",
			i,
			plan,
		)
	}
}

func TestOptimizePlannerStatsReloadsSharedCacheStatistics(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	require.Same(t, store.readDB, store.writeDB)
	seedStakeRefLookupUtxos(t, store, 50_000, 256)
	store.writeDB.SetMaxIdleConns(2)

	// Hold two connections so the pool has idle ones carrying old state.
	stale := poolPlans(t, store.writeDB, 2)
	for _, plan := range stale {
		require.Contains(t, plan, "idx_utxo_deleted")
	}

	result, err := store.OptimizePlannerStatsContext(
		context.Background(), PlannerStatsTriggerEpoch,
	)
	require.NoError(t, err)
	require.True(t, result.Changed)

	for i, plan := range poolPlans(t, store.writeDB, 2) {
		require.Contains(
			t,
			plan,
			"tx_id_output_idx",
			"connection %d: %s",
			i,
			plan,
		)
	}
	// The in-memory database dies with its last connection, so the refresh
	// must not have dropped them all.
	var live int
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM utxo",
	).Scan(&live))
	require.Equal(t, 50_000, live)
}

func TestOptimizePlannerStatsRepreparesHotStatements(t *testing.T) {
	t.Parallel()
	store := newFilePlannerStatsStore(t, nil)
	seedStakeRefLookupUtxos(t, store, 300, 16)
	query := consumeUtxosBatchQuery(2, false)
	before, ok := store.lookupCachedStmt(query)
	require.True(t, ok, "hot statement must be cached at start")

	changed, err := store.OptimizePlannerStatsContext(
		context.Background(), PlannerStatsTriggerEpoch,
	)
	require.NoError(t, err)
	require.True(t, changed.Changed)
	after, ok := store.lookupCachedStmt(query)
	require.True(t, ok, "cache must be repopulated, not left empty")
	require.NotSame(
		t,
		before,
		after,
		"changed statistics must replace the cached statement",
	)

	unchanged, err := store.OptimizePlannerStatsContext(
		context.Background(), PlannerStatsTriggerEpoch,
	)
	require.NoError(t, err)
	require.False(t, unchanged.Changed)
	same, ok := store.lookupCachedStmt(query)
	require.True(t, ok)
	require.Same(
		t,
		after,
		same,
		"an unchanged run must keep the cached statement",
	)
}

var plannerProbeSequence atomic.Int64

// SQLite re-plans a statement that was prepared before sqlite_stat1 existed
// once the connection that prepared it runs the analysis, so the reprepare
// hook is a defence for plans on other connections rather than a requirement
// on the write connection. The probe is a SQL function evaluated for every
// row the plan visits: the trap plan visits all live UTxOs, the good plan one.
func TestPreparedStatementReplansAfterOptimizeOnSameConnection(t *testing.T) {
	t.Parallel()
	// A uniquely named function keeps the process-wide registration from
	// colliding with other tests.
	name := fmt.Sprintf("planner_probe_%d", plannerProbeSequence.Add(1))
	var visited atomic.Int64
	sqlite.MustRegisterScalarFunction(
		name, 0,
		func(*sqlite.FunctionContext, []driver.Value) (driver.Value, error) {
			visited.Add(1)
			return int64(1), nil
		},
	)
	store := newFilePlannerStatsStore(t, nil)
	seedStakeRefLookupUtxos(t, store, 50_000, 256)
	ctx := context.Background()
	stmt, err := store.writeDB.PrepareContext(
		ctx,
		"SELECT COUNT(*) FROM utxo WHERE deleted_slot = 0 AND "+
			"spent_at_tx_id IS NULL AND (tx_id, output_idx) IN "+
			"((?, ?), (?, ?)) AND "+name+"() = 1",
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stmt.Close() })
	run := func() int64 {
		visited.Store(0)
		zero := make([]byte, 32)
		var n int
		require.NoError(t, stmt.QueryRowContext(ctx, zero, 0, zero, 1).Scan(&n))
		return visited.Load()
	}
	require.Greater(t, run(), int64(10_000), "stale plan must scan the table")

	_, err = store.OptimizePlannerStatsContext(ctx, PlannerStatsTriggerEpoch)
	require.NoError(t, err)
	require.LessOrEqual(t, run(), int64(2), "prepared statement must re-plan")
}

func TestOptimizePlannerStatsSkipsDuringBulkLoad(t *testing.T) {
	t.Parallel()
	store := newFilePlannerStatsStore(t, nil)
	seedStakeRefLookupUtxos(t, store, 300, 16)
	require.NoError(t, store.SetBulkLoadPragmas())
	result, err := store.OptimizePlannerStatsContext(
		context.Background(), PlannerStatsTriggerEpoch,
	)
	require.NoError(t, err)
	require.True(t, result.Skipped)
	require.False(t, result.Changed)
	_, exists := stat1Rows(t, store.writeDB)
	require.False(t, exists)
	require.NoError(t, store.RestoreNormalPragmas())
}

func TestOptimizePlannerStatsMetrics(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	store := newFilePlannerStatsStore(t, reg)
	seedStakeRefLookupUtxos(t, store, 300, 16)
	for _, trigger := range []string{
		PlannerStatsTriggerStartup, PlannerStatsTriggerEpoch,
	} {
		_, err := store.OptimizePlannerStatsContext(
			context.Background(),
			trigger,
		)
		require.NoError(t, err)
	}
	families, err := reg.Gather()
	require.NoError(t, err)
	got := map[string]float64{}
	for _, family := range families {
		if family.GetName() != "dingo_database_sql_planner_stats_runs_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			key := ""
			for _, label := range metric.GetLabel() {
				key += label.GetName() + "=" + label.GetValue() + ","
			}
			got[key] = metric.GetCounter().GetValue()
		}
	}
	require.Equal(t, map[string]float64{
		"result=changed,trigger=startup,": 1,
		"result=unchanged,trigger=epoch,": 1,
	}, got)
}

type recordingExecer struct{ statements []string }

func (r *recordingExecer) ExecContext(
	_ context.Context,
	query string,
	_ ...any,
) (sql.Result, error) {
	r.statements = append(r.statements, query)
	return driver.RowsAffected(0), nil
}

func TestNonSQLiteDialectsAreLeftUntouchedByOptimize(t *testing.T) {
	t.Parallel()
	for name, dialect := range map[string]Dialect{
		"postgres": PostgresDialect(),
		"mysql":    MySQLDialect(),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			exec := &recordingExecer{}
			supported, err := dialect.OptimizePlannerStats(
				context.Background(),
				exec,
			)
			require.NoError(t, err)
			require.False(t, supported)
			require.Empty(t, exec.statements, "no statement may be issued")
		})
	}
	sqliteExec := &recordingExecer{}
	supported, err := SQLiteDialect().OptimizePlannerStats(context.Background(), sqliteExec)
	require.NoError(t, err)
	require.True(t, supported)
	require.Len(t, sqliteExec.statements, 1)
	require.Contains(t, sqliteExec.statements[0], "PRAGMA optimize")

	// The full-ANALYZE path Mithril and backfill use is unchanged.
	pg := &recordingExecer{}
	require.NoError(
		t,
		PostgresDialect().UpdatePlannerStats(context.Background(), pg),
	)
	require.Equal(t, []string{"ANALYZE"}, pg.statements)
	my := &recordingExecer{}
	require.NoError(
		t,
		MySQLDialect().UpdatePlannerStats(context.Background(), my),
	)
	require.Empty(t, my.statements)
	lite := &recordingExecer{}
	require.NoError(
		t,
		SQLiteDialect().UpdatePlannerStats(context.Background(), lite),
	)
	require.Equal(t, []string{"ANALYZE"}, lite.statements)
}
