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
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// PlannerStatsTriggerStartup labels the run made before block processing.
	PlannerStatsTriggerStartup = types.PlannerStatsTriggerStartup
	// PlannerStatsTriggerEpoch labels a run made after an epoch rollover.
	PlannerStatsTriggerEpoch = types.PlannerStatsTriggerEpoch
	// PlannerStatsTriggerResume labels the run made when a deferral of the
	// epoch runs ends.
	PlannerStatsTriggerResume = types.PlannerStatsTriggerResume

	// plannerStatsGrowthNumerator and plannerStatsGrowthDenominator express
	// the growth threshold, 25%, as a fraction, and plannerStatsMinGrowthRows
	// is the least a table must grow by. A pass re-analyzes every table and
	// holds the only write connection, yet
	// between epochs most tables barely change and the statistics barely
	// move. An epoch pass therefore runs only once some table's highest rowid
	// has grown by the fraction, and by the minimum, since the last completed
	// pass. The measure is per table and not the file size: a large database
	// whose transaction and certificate tables were empty when the last pass
	// ran must be re-analyzed as they fill, though the file grows by little.
	// The minimum stops a table that was empty from forcing a pass on its
	// first few rows.
	plannerStatsGrowthNumerator   = 1
	plannerStatsGrowthDenominator = 4
	plannerStatsMinGrowthRows     = 1000

	// readConnMaxLifetime bounds how long a pooled read connection keeps the
	// statistics it loaded when a refresh could not reach it, for example a
	// connection checked out across the refresh. database/sql only retires an
	// expired connection when it is idle or returned, never mid-use.
	readConnMaxLifetime = 5 * time.Minute
)

// sqlitePlannerStatsMetrics holds the planner-statistics collectors. The
// zero value is a no-op, matching a Store built without a Prometheus
// registry.
type sqlitePlannerStatsMetrics struct {
	runs     *prometheus.CounterVec
	duration *prometheus.HistogramVec
	errors   prometheus.Counter
}

func newPlannerStatsMetrics(
	reg prometheus.Registerer,
) sqlitePlannerStatsMetrics {
	if reg == nil {
		return sqlitePlannerStatsMetrics{}
	}
	metrics := sqlitePlannerStatsMetrics{
		runs: registerOrReuse(reg, prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: sqlMetricNamePrefix + "planner_stats_runs_total",
				Help: "Incremental planner-statistics runs by trigger " +
					"(startup, epoch, resume, anchor) and result " +
					"(changed, unchanged, skipped, error).",
			}, []string{"trigger", "result"},
		)),
		duration: registerOrReuse(reg, prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: sqlMetricNamePrefix + "planner_stats_duration_seconds",
				Help: "Wall time of incremental planner-statistics runs " +
					"by trigger, including the connection refresh.",
				Buckets: []float64{
					0.01, 0.1, 1, 5, 15, 60, 180, 600, 1000,
				},
			}, []string{"trigger"},
		)),
		errors: registerOrReuse(reg, prometheus.NewCounter(
			prometheus.CounterOpts{
				Name: sqlMetricNamePrefix + "planner_stats_errors_total",
				Help: "Failed incremental planner-statistics runs.",
			},
		)),
	}
	// Export every series at zero, so a rate or alert over an outcome that
	// has not happened yet reads zero instead of no data.
	for _, trigger := range []string{
		PlannerStatsTriggerStartup, PlannerStatsTriggerEpoch,
		PlannerStatsTriggerResume, types.PlannerStatsTriggerAnchor,
	} {
		for _, result := range []string{
			"changed", "unchanged", "skipped", "error",
		} {
			metrics.runs.WithLabelValues(trigger, result)
		}
	}
	return metrics
}

func registerOrReuse[C prometheus.Collector](
	reg prometheus.Registerer,
	collector C,
) C {
	if err := reg.Register(collector); err != nil {
		if already, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); ok {
			if existing, ok := already.ExistingCollector.(C); ok {
				return existing
			}
		}
		panic(err)
	}
	return collector
}

func (m sqlitePlannerStatsMetrics) observe(
	trigger string,
	result string,
	elapsed time.Duration,
) {
	if m.runs == nil {
		return
	}
	m.runs.WithLabelValues(trigger, result).Inc()
	m.duration.WithLabelValues(trigger).Observe(elapsed.Seconds())
	if result == "error" {
		m.errors.Inc()
	}
}

// OptimizePlannerStatsContext runs the backend's incremental planner
// statistics maintenance (a bounded ANALYZE on SQLite). When the stored
// statistics changed it then refreshes the read pool and the cached hot
// statements so no connection keeps planning with the old numbers. Backends
// without an incremental form report Supported=false and are left alone.
//
// The run holds one write connection for its duration and so pauses
// writers; it never runs inside a transaction. It is skipped while a bulk
// load owns the database. An epoch run is also skipped until a table has grown
// by plannerStatsGrowthNumerator/plannerStatsGrowthDenominator, and at least
// plannerStatsMinGrowthRows, since the last completed run; every other
// trigger always executes. The table sizes of the last run are kept in memory
// only: a restart runs a startup pass anyway, so nothing needs to survive
// one.
func (s *Store) OptimizePlannerStatsContext(
	ctx context.Context,
	trigger string,
) (types.PlannerStatsResult, error) {
	var result types.PlannerStatsResult
	if s.closed.Load() || !s.ready.Load() {
		return result, errors.New("sqlstore: store is not ready")
	}
	// Before the bulk-mode check and the fingerprint: the fingerprint reads
	// SQLite's catalog, which other backends do not have.
	if !s.dialect.HasIncrementalPlannerStats() {
		return result, nil
	}
	s.scheduledWorkMu.Lock()
	defer s.scheduledWorkMu.Unlock()
	started := time.Now()
	if s.bulkMode {
		result.Supported = true
		result.Skipped = true
		s.plannerStats.observe(trigger, "skipped", time.Since(started))
		return result, nil
	}
	sizes, err := s.sqliteTableSizes(ctx)
	if err != nil {
		return s.plannerStatsFailed(trigger, started, result, err)
	}
	if trigger == PlannerStatsTriggerEpoch &&
		s.plannerStatsSizes != nil &&
		!s.plannerStatsGrown(sizes) {
		result.Supported = true
		result.Skipped = true
		s.plannerStats.observe(trigger, "skipped", time.Since(started))
		return result, nil
	}
	before, _, err := s.plannerStatsFingerprint(ctx)
	if err != nil {
		return s.plannerStatsFailed(trigger, started, result, err)
	}
	// One connection for the whole pass: SQLite's optimize sets and restores
	// a per-connection limit around ANALYZE, which a pooled writeDB (the
	// in-memory topology) would otherwise spread over several connections.
	conn, err := s.writeDB.Conn(ctx)
	if err != nil {
		return s.plannerStatsFailed(trigger, started, result, err)
	}
	supported, err := s.dialect.OptimizePlannerStats(ctx, conn)
	if cerr := conn.Close(); err == nil {
		err = cerr
	}
	if !supported {
		return result, err
	}
	result.Supported = true
	if err != nil {
		return s.plannerStatsFailed(trigger, started, result, err)
	}
	after, rows, err := s.plannerStatsFingerprint(ctx)
	if err != nil {
		return s.plannerStatsFailed(trigger, started, result, err)
	}
	// The sizes read before the pass, not after: growth during the pass
	// counts toward the next one.
	s.plannerStatsSizes = sizes
	result.Stat1Rows = rows
	result.Changed = before != after
	if result.Changed {
		if err := s.refreshReadPool(ctx); err != nil {
			return s.plannerStatsFailed(trigger, started, result, err)
		}
		s.reprepareHotStatements(ctx)
	}
	result.Duration = time.Since(started)
	outcome := "unchanged"
	if result.Changed {
		outcome = "changed"
	}
	s.plannerStats.observe(trigger, outcome, result.Duration)
	return result, nil
}

// sqliteTableSizes returns the highest rowid of every rowid table. Each is a
// descent of one b-tree path, so reading all of them costs nothing next to a
// pass, unlike a row count. It overstates a table that has had rows deleted
// from below its top, which can only start a pass early, never withhold one.
func (s *Store) sqliteTableSizes(
	ctx context.Context,
) (map[string]int64, error) {
	tables, err := s.sqliteRowidTables(ctx)
	if err != nil {
		return nil, err
	}
	sizes := make(map[string]int64, len(tables))
	for _, table := range tables {
		var top int64
		if err := s.writeDB.QueryRowContext(
			ctx,
			`SELECT COALESCE(MAX(rowid), 0) FROM "`+
				strings.ReplaceAll(table, `"`, `""`)+`"`,
		).Scan(&top); err != nil {
			return nil, fmt.Errorf("read size of %s: %w", table, err)
		}
		sizes[table] = top
	}
	return sizes, nil
}

func (s *Store) sqliteRowidTables(ctx context.Context) ([]string, error) {
	rows, err := s.writeDB.QueryContext(
		ctx,
		"SELECT name FROM sqlite_schema WHERE type = 'table' "+
			"AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' "+
			"AND sql NOT LIKE 'CREATE VIRTUAL%' "+
			"AND sql NOT LIKE '%WITHOUT ROWID%'",
	)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan table name: %w", err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	return tables, nil
}

// plannerStatsGrown reports whether any table has outgrown the sizes
// recorded by the last completed pass. A table whose size fell, after a
// rollback or a vacuum that renumbers rowids, lowers its recorded size so
// that growth is counted from the new floor. Callers hold scheduledWorkMu.
func (s *Store) plannerStatsGrown(sizes map[string]int64) bool {
	grown := false
	for table, top := range sizes {
		recorded := s.plannerStatsSizes[table]
		if top < recorded {
			s.plannerStatsSizes[table] = top
			continue
		}
		growth := top - recorded
		if growth >= plannerStatsMinGrowthRows &&
			growth*plannerStatsGrowthDenominator >=
				recorded*plannerStatsGrowthNumerator {
			grown = true
		}
	}
	return grown
}

// HasPlannerStats reports whether sqlite_stat1 holds any rows. Backends
// without an incremental form report false, so callers keep their full
// refresh.
func (s *Store) HasPlannerStats(ctx context.Context) (bool, error) {
	if s.closed.Load() || !s.ready.Load() {
		return false, errors.New("sqlstore: store is not ready")
	}
	if !s.dialect.HasIncrementalPlannerStats() {
		return false, nil
	}
	_, rows, err := s.plannerStatsFingerprint(ctx)
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (s *Store) plannerStatsFailed(
	trigger string,
	started time.Time,
	result types.PlannerStatsResult,
	err error,
) (types.PlannerStatsResult, error) {
	result.Duration = time.Since(started)
	s.plannerStats.observe(trigger, "error", result.Duration)
	return result, fmt.Errorf("planner statistics: %w", err)
}

// plannerStatsFingerprint folds sqlite_stat1 into a 64-bit value plus its row
// count. A database that has never been analyzed has no such table and
// fingerprints as empty.
func (s *Store) plannerStatsFingerprint(
	ctx context.Context,
) (uint64, int, error) {
	var present int
	if err := s.writeDB.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM sqlite_schema WHERE name = 'sqlite_stat1'",
	).Scan(&present); err != nil {
		return 0, 0, fmt.Errorf("probe sqlite_stat1: %w", err)
	}
	if present == 0 {
		return 0, 0, nil
	}
	rows, err := s.writeDB.QueryContext(
		ctx,
		"SELECT tbl, COALESCE(idx, ''), COALESCE(stat, '') "+
			"FROM sqlite_stat1 ORDER BY tbl, idx, stat",
	)
	if err != nil {
		return 0, 0, fmt.Errorf("read sqlite_stat1: %w", err)
	}
	defer rows.Close()
	hash := fnv.New64a()
	count := 0
	for rows.Next() {
		var tbl, idx, stat string
		if err := rows.Scan(&tbl, &idx, &stat); err != nil {
			return 0, 0, fmt.Errorf("scan sqlite_stat1: %w", err)
		}
		// NUL separators keep ("ab","c") and ("a","bc") distinct.
		_, _ = hash.Write([]byte(tbl + "\x00" + idx + "\x00" + stat + "\x00"))
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("read sqlite_stat1: %w", err)
	}
	return hash.Sum64(), count, nil
}

// refreshReadPool makes every pooled read connection pick up statistics that
// just changed. A connection keeps the statistics it loaded when it opened,
// and only the creation of sqlite_stat1 invalidates other connections'
// schemas, so later updates are otherwise invisible to them.
//
// A file-backed store has a separate read pool: dropping its idle connections
// makes the next reader open one that loads the new statistics. Connections
// checked out right now are retired when returned. When readDB is writeDB
// (in-memory, shared-cache) idle connections must never reach zero, because
// the database is destroyed when its last connection closes; there the
// statistics are reloaded in place through a held connection, and the shared
// cache hands the reloaded schema to the others.
func (s *Store) refreshReadPool(ctx context.Context) error {
	if s.readDB != s.writeDB {
		idle := s.readDB.Stats().MaxOpenConnections
		if idle <= 0 {
			idle = 2
		}
		s.readDB.SetMaxIdleConns(0)
		s.readDB.SetMaxIdleConns(idle)
		return nil
	}
	conn, err := s.writeDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("hold connection for statistics reload: %w", err)
	}
	defer conn.Close()
	return reloadStatistics(ctx, conn)
}

func reloadStatistics(ctx context.Context, conn *sql.Conn) error {
	if _, err := conn.ExecContext(ctx, "ANALYZE sqlite_schema"); err != nil {
		return fmt.Errorf("reload statistics: %w", err)
	}
	return nil
}

// reprepareHotStatements replaces the cached statements with freshly prepared
// ones. The replacements are built before the swap so callers never see an
// empty cache while a prepare waits for the write connection, and a canceled
// context leaves the existing cache in place. It must not be called from a
// goroutine holding a write transaction: preparing against writeDB needs the
// connection that transaction holds.
func (s *Store) reprepareHotStatements(ctx context.Context) {
	if s.lazyHotStatements {
		// Drop the cache; borrowStmt prepares against the new statistics on
		// next use. Failures are forgotten because the schema may differ.
		s.stmtMu.Lock()
		idle := s.retireStatementsLocked(s.stmts)
		s.stmts = nil
		s.stmtFailed = nil
		s.stmtMu.Unlock()
		for _, stmt := range idle {
			_ = stmt.Close()
		}
		return
	}
	fresh := s.buildHotStatements(ctx)
	if ctx.Err() != nil || s.closed.Load() {
		for _, stmt := range fresh {
			_ = stmt.Close()
		}
		return
	}
	s.stmtMu.Lock()
	idle := s.retireStatementsLocked(s.stmts)
	s.stmts = fresh
	s.stmtMu.Unlock()
	for _, stmt := range idle {
		_ = stmt.Close()
	}
}
