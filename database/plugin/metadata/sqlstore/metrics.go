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
