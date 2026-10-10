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
	"time"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// PlannerStatsTriggerStartup labels the run made before block processing.
	PlannerStatsTriggerStartup = types.PlannerStatsTriggerStartup
	// PlannerStatsTriggerEpoch labels a run made after an epoch rollover.
	PlannerStatsTriggerEpoch = types.PlannerStatsTriggerEpoch

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
	return sqlitePlannerStatsMetrics{
		runs: registerOrReuse(reg, prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: sqlMetricNamePrefix + "planner_stats_runs_total",
				Help: "Incremental planner-statistics runs by trigger " +
					"(startup, epoch) and result (changed, unchanged, " +
					"skipped, error).",
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
// statistics maintenance (PRAGMA optimize on SQLite). When the stored
// statistics changed it then refreshes the read pool and the cached hot
// statements so no connection keeps planning with the old numbers. Backends
// without an incremental form report Supported=false and are left alone.
//
// The run is a single statement on the write connection and so pauses
// writers for its duration; it never runs inside a transaction. It is
// skipped while a bulk load owns the database.
func (s *Store) OptimizePlannerStatsContext(
	ctx context.Context,
	trigger string,
) (types.PlannerStatsResult, error) {
	var result types.PlannerStatsResult
	if s.closed.Load() || !s.ready.Load() {
		return result, errors.New("sqlstore: store is not ready")
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
	before, _, err := s.plannerStatsFingerprint(ctx)
	if err != nil {
		return s.plannerStatsFailed(trigger, started, result, err)
	}
	supported, err := s.dialect.OptimizePlannerStats(ctx, s.writeDB)
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
	fresh := s.buildHotStatements(ctx)
	if ctx.Err() != nil || s.closed.Load() {
		for _, stmt := range fresh {
			_ = stmt.Close()
		}
		return
	}
	s.stmtMu.Lock()
	old := s.stmts
	s.stmts = fresh
	s.stmtMu.Unlock()
	for _, stmt := range old {
		_ = stmt.Close()
	}
}
