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
	"time"

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

// newSQLQueryDurationHistogram registers dingo_database_sql_query_duration_seconds
// against reg, or returns nil (the same documented no-op sentinel
// newSQLOperationsCounter uses) when reg is nil. See that function's doc
// comment for the duplicate-registration accommodation this mirrors.
func newSQLQueryDurationHistogram(
	reg prometheus.Registerer,
) *prometheus.HistogramVec {
	if reg == nil {
		return nil
	}
	histogram := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: sqlMetricNamePrefix + "query_duration_seconds",
		Help: "Wall-clock duration of each SQL statement issued against " +
			"the metadata store, labeled by classifySQLStatement's op " +
			"classification and, when known, the sqlc-generated query " +
			"name. Counted at the same chokepoint as " +
			"dingo_database_sql_operations_total, including the " +
			"hot-statement cache's cached calls (queryRowCached/execCached " +
			"in prepared_stmt.go). For a multi-row SELECT dispatched " +
			"through QueryContext, this measures dispatch latency only -- " +
			"database/sql returns *sql.Rows before the driver produces any " +
			"rows, so the observation is recorded before the caller's " +
			"Next()/Scan() loop does any work -- not the full " +
			"query-plus-iteration time. ExecContext, QueryRowContext, and " +
			"the cached-statement path all block until the statement " +
			"completes, so their observations do reflect completion.",
		// 100us to ~1.6s: SQL statements against this store range from a
		// sub-millisecond point lookup to a multi-block delta-batch write
		// during from-genesis sync; the default Prometheus buckets (5ms to
		// 10s) are both too coarse below 5ms -- where most single-statement
		// point queries land -- and reach far past any single statement
		// this store issues without benefiting from the extra range.
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 15),
	}, []string{"op", "query"})
	if err := reg.Register(histogram); err != nil {
		var already prometheus.AlreadyRegisteredError
		if errors.As(err, &already) {
			if existing, ok := already.ExistingCollector.(*prometheus.HistogramVec); ok {
				return existing
			}
		}
		panic(err)
	}
	return histogram
}

// classifySQLStatement returns the metadata store's best-effort operation
// label for a query and, when the query carries one, the sqlc-generated
// query name embedded in its leading "-- name: X :verb" comment. Every
// sqlc-generated query embeds that comment (see
// internal/query/{sqlite,postgres,mysql}/*.sql.go, generated from the
// "-- name:" annotation sqlc requires on every query in
// internal/query/*.sql); this walks past that comment (and any further
// leading comment lines) before checking the real statement's leading
// keyword. A CTE (WITH ...), PRAGMA, or DDL statement is classified "other"
// rather than guessed at -- a wrong label would be worse than an honest
// catch-all, and none of dingo's own hot paths are CTEs today. name is
// "unknown" when no "-- name:" comment is present -- a hand-written query
// (a PRAGMA, a schema-inspection SELECT in a test) rather than a
// sqlc-generated one.
//
// name is safe to use as a Prometheus label despite being derived from
// query text: sqlc query names are not user input or raw SQL text, they are
// a small, fixed set fully determined by the "-- name:" annotations checked
// into internal/query/*.sql at build time (dozens of entries, one per
// generated query function), so this label's cardinality is bounded by the
// codebase, not by anything a caller or attacker controls.
func classifySQLStatement(query string) (op, name string) {
	name = "unknown"
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
			return "other", name
		}
		if name == "unknown" {
			if parsed, ok := parseSQLCQueryName(rest[:idx]); ok {
				name = parsed
			}
		}
		q = rest[idx+1:]
	}
	for _, op := range [...]string{"INSERT", "UPDATE", "DELETE", "SELECT"} {
		if len(q) >= len(op) && strings.EqualFold(q[:len(op)], op) {
			return strings.ToLower(op), name
		}
	}
	return "other", name
}

// parseSQLCQueryName extracts X from a sqlc "name: X :verb" comment line
// (the leading "--" already stripped by classifySQLStatement's caller), or
// reports ok=false when line is not that annotation -- for example a plain
// leading comment with no sqlc annotation at all.
func parseSQLCQueryName(line string) (name string, ok bool) {
	rest, ok := strings.CutPrefix(strings.TrimSpace(line), "name:")
	if !ok {
		return "", false
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return "", false
	}
	return fields[0], true
}

// countingQueryer increments counter and observes duration (labeled by
// classifySQLStatement's op/name classification) for every statement it
// executes, then delegates to the wrapped queryer unchanged. It is applied
// only by Store.instrumentedQueryer, which is the single place every call
// site obtains a queryer from -- see that method's doc comment for why
// that matters for coverage, and stmtForQueryer's and
// requireAccountBaselineTransaction's doc comments (prepared_stmt.go,
// account.go) for two places that must unwrap this type explicitly to keep
// working once it sits between them and the *sql.Tx they check for.
//
// counter and duration are both nil or both non-nil in practice -- Store
// derives them from the same Config.PromRegistry and instrumentedQueryer
// only constructs a countingQueryer when at least one is set -- but each
// method checks its own field before use rather than assuming that
// coupling, so a future caller that wires only one of the two cannot panic
// on the other's nil pointer.
type countingQueryer struct {
	queryer
	counter  *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func (q countingQueryer) ExecContext(
	ctx context.Context,
	query string,
	args ...any,
) (sql.Result, error) {
	op, name := classifySQLStatement(query)
	if q.counter != nil {
		q.counter.WithLabelValues(op).Inc()
	}
	if q.duration == nil {
		return q.queryer.ExecContext(ctx, query, args...)
	}
	start := time.Now()
	result, err := q.queryer.ExecContext(ctx, query, args...)
	q.duration.WithLabelValues(op, name).Observe(time.Since(start).Seconds())
	return result, err
}

// QueryContext observes dispatch latency, not query-plus-iteration time: for
// a multi-row SELECT, database/sql returns *sql.Rows as soon as the driver
// has dispatched the statement, before it has produced any rows, so
// Observe below runs before the caller's own Next()/Scan() loop -- where a
// :many query's real cost lives -- does any work. Measured in-tree, a
// 300000-row SELECT through Store recorded a 0.000111s observation here
// while the caller's row iteration took 260ms, landing every such query in
// this histogram's 100-200us bucket regardless of how many rows it actually
// read. Timing through Close() instead is not available here: the queryer
// interface (store.go) and every sqlc-generated DBTX interface this wraps
// (internal/query/{sqlite,postgres,mysql}/db.go, generated code) both
// declare QueryContext's return type as the concrete *sql.Rows, so there is
// no wrapper type available to attach completion timing to without changing
// those generated interfaces at all three of this codebase's 40 :many call
// sites. The Help text on dingo_database_sql_query_duration_seconds
// documents this gap; QueryRowContext below and the cached-statement path
// (prepared_stmt.go's queryRowCached/execCached) are not affected, since
// database/sql blocks both until the statement completes.
func (q countingQueryer) QueryContext(
	ctx context.Context,
	query string,
	args ...any,
) (*sql.Rows, error) {
	op, name := classifySQLStatement(query)
	if q.counter != nil {
		q.counter.WithLabelValues(op).Inc()
	}
	if q.duration == nil {
		return q.queryer.QueryContext(ctx, query, args...)
	}
	start := time.Now()
	rows, err := q.queryer.QueryContext(ctx, query, args...)
	q.duration.WithLabelValues(op, name).Observe(time.Since(start).Seconds())
	return rows, err
}

func (q countingQueryer) QueryRowContext(
	ctx context.Context,
	query string,
	args ...any,
) *sql.Row {
	op, name := classifySQLStatement(query)
	if q.counter != nil {
		q.counter.WithLabelValues(op).Inc()
	}
	if q.duration == nil {
		return q.queryer.QueryRowContext(ctx, query, args...)
	}
	start := time.Now()
	row := q.queryer.QueryRowContext(ctx, query, args...)
	q.duration.WithLabelValues(op, name).Observe(time.Since(start).Seconds())
	return row
}

// PrepareContext is deliberately not counted or timed (and not overridden
// here): it compiles a statement without executing it, and its only caller
// (prepareHotStatements) prepares the same fixed, small statement list once
// at Start. Counting or timing it there would misrepresent one-time
// preparation cost as query volume or query latency.
//
// A call served by the hot-statement cache never reaches this type at all:
// queryRowCached/execCached (prepared_stmt.go) resolve straight to a
// *sql.Stmt via stmtForQueryer -- which unwraps past countingQueryer
// entirely, by design, to reach the real *sql.Tx/*sql.DB -- and call
// QueryRowContext/ExecContext on that *sql.Stmt directly, never on this
// wrapper. queryRowCached and execCached count and time those calls
// explicitly instead, so a hot statement is still counted and timed exactly
// once, just not by this type.
