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
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

const sqlMetricNamePrefix = "dingo_database_sql_"

// newSQLOperationsCounter registers dingo_database_sql_operations_total
// against reg, or returns nil (a documented no-op sentinel Store checks
// before wrapping any queryer -- see instrumentedQueryer) when reg is nil.
// Two Store instances sharing one registry (as some test harnesses do)
// reuse the same collector instead of panicking on a duplicate
// registration, the same accommodation database/plugin/blob/badger's own
// metrics.go makes for its GC collectors.
func newSQLOperationsCounter(
	reg prometheus.Registerer,
) *prometheus.CounterVec {
	if reg == nil {
		return nil
	}
	counter := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: sqlMetricNamePrefix + "operations_total",
		Help: "Total SQL statements issued against the metadata store, " +
			"classified by leading SQL keyword (insert/update/delete/select/" +
			"other). Counted at Store's single query chokepoint " +
			"(instrumentedQueryer), so this covers every domain query, not " +
			"only the hand-picked hot statements prepared_stmt.go caches.",
	}, []string{"op"})
	if err := reg.Register(counter); err != nil {
		var already prometheus.AlreadyRegisteredError
		if errors.As(err, &already) {
			if existing, ok := already.ExistingCollector.(*prometheus.CounterVec); ok {
				return existing
			}
		}
		panic(err)
	}
	return counter
}

// newSQLPoolMetrics registers six database/sql connection-pool metrics for
// one pool (writeDB or readDB) against reg, labeled pool="write"|"read" via
// ConstLabels, or does nothing (the same documented no-op accommodation
// newSQLOperationsCounter uses) when reg is nil.
//
// All six are backed by statsFn -- s.WritePoolStats or s.ReadPoolStats,
// which just return sql.DB.Stats() -- sampled fresh on every scrape by a
// GaugeFunc/CounterFunc callback rather than any state Store tracks or
// updates itself. This mirrors the pull-based pattern
// database/plugin/metadata/sqlite/metrics.go's registerSQLiteFileMetrics
// already uses for dingo_database_sql_wal_bytes/disk_bytes, rather than a
// background ticker like Store's own maintenance/checkpoint tickers:
// sql.DB.Stats() is an in-memory read with no I/O or lock contention of its
// own, so there is nothing to amortize by sampling less often than every
// scrape, and a ticker would only add a second place these numbers could go
// stale between scrapes.
//
// dingo_database_sql_pool_wait_count_total and
// dingo_database_sql_pool_wait_duration_seconds_total map sql.DBStats'
// WaitCount/WaitDuration -- cumulative since the pool was opened, per
// database/sql's documented semantics -- as Prometheus counters via
// CounterFunc, which (like GaugeFunc) takes its value from a callback
// instead of being Add()'d to directly; database/sql, not this package, is
// what guarantees they never decrease. The other four
// (OpenConnections/InUse/Idle/MaxOpenConnections) are point-in-time values
// and become GaugeFuncs.
//
// The write pool is the one this exists for: every current provider caps
// it at SetMaxOpenConns(1) (see sqlite.openSQLStore; postgres/mysql size
// theirs the same way) because the underlying store allows only one writer
// regardless of connection count, so any second concurrent write path --
// for example ledger block-apply racing persistDeferredHeaderValidation's
// per-block durability write -- can only ever queue behind it. A sustained
// rate() of pool="write"'s wait_duration_seconds_total approaching 1.0
// means callers are effectively serialized on that single connection. The
// read pool is registered identically for consistency and as a future
// comparison point, even though it is not the pool this specific finding
// is about.
func newSQLPoolMetrics(
	reg prometheus.Registerer,
	pool string,
	statsFn func() sql.DBStats,
) {
	if reg == nil {
		return
	}
	constLabels := prometheus.Labels{"pool": pool}
	safeRegisterGaugeFunc(
		reg,
		sqlMetricNamePrefix+"pool_open_connections",
		"Current number of established database/sql connections (in use "+
			"plus idle) in this pool, from sql.DBStats.OpenConnections. "+
			"Sampled live on each scrape. Labeled pool=\"write\"|\"read\".",
		constLabels,
		func() float64 { return float64(statsFn().OpenConnections) },
	)
	safeRegisterGaugeFunc(
		reg,
		sqlMetricNamePrefix+"pool_in_use_connections",
		"Current number of connections in this pool currently checked "+
			"out and in use, from sql.DBStats.InUse. Sampled live on each "+
			"scrape. Labeled pool=\"write\"|\"read\".",
		constLabels,
		func() float64 { return float64(statsFn().InUse) },
	)
	safeRegisterGaugeFunc(
		reg,
		sqlMetricNamePrefix+"pool_idle_connections",
		"Current number of idle (open but unused) connections in this "+
			"pool, from sql.DBStats.Idle. Sampled live on each scrape. "+
			"Labeled pool=\"write\"|\"read\".",
		constLabels,
		func() float64 { return float64(statsFn().Idle) },
	)
	safeRegisterGaugeFunc(
		reg,
		sqlMetricNamePrefix+"pool_max_open_connections",
		"Configured SetMaxOpenConns limit for this pool, from "+
			"sql.DBStats.MaxOpenConnections. Every current provider caps "+
			"the write pool at 1 -- the underlying store allows only one "+
			"writer regardless of connection count -- so "+
			"pool=\"write\" is expected to read 1. Labeled "+
			"pool=\"write\"|\"read\".",
		constLabels,
		func() float64 { return float64(statsFn().MaxOpenConnections) },
	)
	safeRegisterCounterFunc(
		reg,
		sqlMetricNamePrefix+"pool_wait_count_total",
		"Cumulative number of times a caller had to wait for a "+
			"connection from this pool, from sql.DBStats.WaitCount. For "+
			"pool=\"write\" (capped at one connection; see "+
			"dingo_database_sql_pool_max_open_connections' help) this "+
			"counts every time one write path queued behind another "+
			"already holding that connection. Labeled "+
			"pool=\"write\"|\"read\".",
		constLabels,
		func() float64 { return float64(statsFn().WaitCount) },
	)
	safeRegisterCounterFunc(
		reg,
		sqlMetricNamePrefix+"pool_wait_duration_seconds_total",
		"Cumulative time callers have spent waiting for a connection "+
			"from this pool, from sql.DBStats.WaitDuration. For "+
			"pool=\"write\", a sustained rate() of this over a window "+
			"approaching 1.0 means the single write connection is "+
			"saturated -- callers are effectively serialized behind it. "+
			"Labeled pool=\"write\"|\"read\".",
		constLabels,
		func() float64 { return statsFn().WaitDuration.Seconds() },
	)
}

// safeRegisterGaugeFunc registers a GaugeFunc with constLabels and silently
// keeps whatever collector is already registered under name on a duplicate
// registration, the same accommodation newSQLOperationsCounter documents
// for a registry shared by more than one Store (as some test harnesses
// use). A gauge, not a counter, has no cumulative state to lose by falling
// back to the existing collector instead of panicking -- the same
// reasoning database/plugin/metadata/sqlite/metrics.go's own
// safeRegisterGaugeFunc (unlabeled, since that package has only one store)
// already applies to dingo_database_sql_wal_bytes/disk_bytes.
func safeRegisterGaugeFunc(
	reg prometheus.Registerer,
	name, help string,
	constLabels prometheus.Labels,
	fn func() float64,
) {
	gauge := prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name:        name,
			Help:        help,
			ConstLabels: constLabels,
		},
		fn,
	)
	if err := reg.Register(gauge); err != nil {
		var already prometheus.AlreadyRegisteredError
		if !errors.As(err, &already) {
			panic(err)
		}
	}
}

// safeRegisterCounterFunc is safeRegisterGaugeFunc's CounterFunc
// counterpart, for the two cumulative sql.DBStats fields
// (WaitCount/WaitDuration) that fn must keep monotonically non-decreasing --
// database/sql, not this function, is what guarantees that.
func safeRegisterCounterFunc(
	reg prometheus.Registerer,
	name, help string,
	constLabels prometheus.Labels,
	fn func() float64,
) {
	counter := prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name:        name,
			Help:        help,
			ConstLabels: constLabels,
		},
		fn,
	)
	if err := reg.Register(counter); err != nil {
		var already prometheus.AlreadyRegisteredError
		if !errors.As(err, &already) {
			panic(err)
		}
	}
}

// classifySQLOp returns the metadata store's best-effort operation label for
// a query. Every sqlc-generated query embeds a leading "-- name: X :verb"
// comment (see internal/query/{sqlite,postgres,mysql}/*.sql.go, generated
// from the "-- name:" annotation sqlc requires on every query in
// internal/query/*.sql); this walks past that comment (and any further
// leading comment lines) before checking the real statement's leading
// keyword. A CTE (WITH ...), PRAGMA, or DDL statement is reported as
// "other" rather than guessed at -- a wrong label would be worse than an
// honest catch-all, and none of dingo's own hot paths are CTEs today.
func classifySQLOp(query string) string {
	q := query
	for {
		q = strings.TrimSpace(q)
		rest, ok := strings.CutPrefix(q, "--")
		if !ok {
			break
		}
		idx := strings.IndexByte(rest, '\n')
		if idx < 0 {
			// A comment with no trailing newline is the entire remaining
			// text: there is no statement left to classify.
			return "other"
		}
		q = rest[idx+1:]
	}
	for _, op := range [...]string{"INSERT", "UPDATE", "DELETE", "SELECT"} {
		if len(q) >= len(op) && strings.EqualFold(q[:len(op)], op) {
			return strings.ToLower(op)
		}
	}
	return "other"
}

// countingQueryer increments counter, labeled by classifySQLOp, for every
// statement it executes, then delegates to the wrapped queryer unchanged.
// It is applied only by Store.instrumentedQueryer, which is the single
// place every call site obtains a queryer from -- see that method's doc
// comment for why that matters for coverage, and stmtForQueryer's and
// requireAccountBaselineTransaction's doc comments (prepared_stmt.go,
// account.go) for two places that must unwrap this type explicitly to keep
// working once it sits between them and the *sql.Tx they check for.
type countingQueryer struct {
	queryer
	counter *prometheus.CounterVec
}

func (q countingQueryer) ExecContext(
	ctx context.Context,
	query string,
	args ...any,
) (sql.Result, error) {
	q.counter.WithLabelValues(classifySQLOp(query)).Inc()
	return q.queryer.ExecContext(ctx, query, args...)
}

func (q countingQueryer) QueryContext(
	ctx context.Context,
	query string,
	args ...any,
) (*sql.Rows, error) {
	q.counter.WithLabelValues(classifySQLOp(query)).Inc()
	return q.queryer.QueryContext(ctx, query, args...)
}

func (q countingQueryer) QueryRowContext(
	ctx context.Context,
	query string,
	args ...any,
) *sql.Row {
	q.counter.WithLabelValues(classifySQLOp(query)).Inc()
	return q.queryer.QueryRowContext(ctx, query, args...)
}

// PrepareContext is deliberately not counted (and not overridden here): it
// compiles a statement without executing it, and its only caller
// (prepareHotStatements) prepares the same fixed, small statement list once
// at Start. Counting there would misrepresent one-time preparation cost as
// query volume.
//
// A call served by the hot-statement cache never reaches this type at all:
// queryRowCached/execCached (prepared_stmt.go) resolve straight to a
// *sql.Stmt via stmtForQueryer -- which unwraps past countingQueryer
// entirely, by design, to reach the real *sql.Tx/*sql.DB -- and call
// QueryRowContext/ExecContext on that *sql.Stmt directly, never on this
// wrapper. queryRowCached and execCached count those calls explicitly
// instead, so a hot statement is still counted exactly once, just not by
// this type.
