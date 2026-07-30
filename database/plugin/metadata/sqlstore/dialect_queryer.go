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
	"regexp"
	"strings"
)

// dialectQueryer keeps the shared business SQL backend-neutral. sqlc's
// generated SQLite package is also used for PostgreSQL/MySQL because the
// result models and parameter ordering are identical; this adapter translates
// only the small set of syntax differences at the database boundary.
type dialectQueryer struct {
	queryer
	dialect string
}

func newDialectQueryer(db queryer, dialect string) queryer {
	if wrapped, ok := db.(dialectQueryer); ok && wrapped.dialect == dialect {
		return db
	}
	return dialectQueryer{queryer: db, dialect: dialect}
}

func (q dialectQueryer) ExecContext(
	ctx context.Context,
	query string,
	args ...any,
) (sql.Result, error) {
	return q.queryer.ExecContext(ctx, q.translate(query), args...)
}

func (q dialectQueryer) QueryContext(
	ctx context.Context,
	query string,
	args ...any,
) (*sql.Rows, error) {
	return q.queryer.QueryContext(ctx, q.translate(query), args...)
}

func (q dialectQueryer) QueryRowContext(
	ctx context.Context,
	query string,
	args ...any,
) *sql.Row {
	if q.dialect != "mysql" || !hasReturningID(query) {
		return q.queryer.QueryRowContext(ctx, q.translate(query), args...)
	}

	base, doNothing, isUpsert := translateMySQLReturning(query)
	result, err := q.queryer.ExecContext(ctx, base, args...)
	if err != nil {
		// QueryRowContext cannot carry an eager error. Return a row whose Scan
		// deterministically reports an execution failure instead.
		return q.queryer.QueryRowContext(
			ctx,
			"SELECT * FROM __dingo_sqlstore_query_error__",
		)
	}
	if doNothing {
		rows, rowsErr := result.RowsAffected()
		if rowsErr == nil && rows == 0 {
			return q.queryer.QueryRowContext(ctx, "SELECT NULL WHERE FALSE")
		}
	}
	if !isUpsert {
		lastID, idErr := result.LastInsertId()
		if idErr == nil && lastID == 0 {
			return q.queryer.QueryRowContext(ctx, "SELECT NULL WHERE FALSE")
		}
	}
	return q.queryer.QueryRowContext(ctx, "SELECT LAST_INSERT_ID()")
}

func (q dialectQueryer) PrepareContext(
	ctx context.Context,
	query string,
) (*sql.Stmt, error) {
	return q.queryer.PrepareContext(ctx, q.translate(query))
}

func (q dialectQueryer) translate(query string) string {
	if q.dialect == "postgres" {
		return rebindPostgresQuery(query)
	}
	if q.dialect == "mysql" {
		if hasReturningID(query) {
			query, _, _ = translateMySQLReturning(query)
		}
		return translateMySQLUpsert(query)
	}
	return query
}

func rebindPostgresQuery(query string) string {
	return PostgresDialect().Rebind(query)
}

var (
	returningIDPattern      = regexp.MustCompile(`(?is)\s+RETURNING\s+id\s*;?\s*$`)
	mysqlIntegerCastPattern = regexp.MustCompile(`(?i)\bAS\s+INTEGER\b`)
)

func hasReturningID(query string) bool {
	return returningIDPattern.MatchString(query)
}

func translateMySQLReturning(query string) (string, bool, bool) {
	base := strings.TrimSpace(returningIDPattern.ReplaceAllString(query, ""))
	base = mysqlIntegerCastPattern.ReplaceAllString(base, "AS SIGNED")
	doNothing := regexp.MustCompile(`(?is)ON\s+CONFLICT(?:\s*\([^)]*\))?\s+DO\s+NOTHING`).MatchString(base)
	isUpsert := regexp.MustCompile(`(?is)ON\s+CONFLICT(?:\s*\([^)]*\))?\s+DO\s+UPDATE`).MatchString(base)
	return translateMySQLUpsertWithID(base, isUpsert), doNothing, isUpsert
}

var (
	mysqlDoNothingPattern = regexp.MustCompile(
		`(?is)ON\s+CONFLICT(?:\s*\([^)]*\))?\s+DO\s+NOTHING`,
	)
	mysqlUpdatePattern = regexp.MustCompile(
		`(?is)ON\s+CONFLICT(?:\s*\([^)]*\))?\s+DO\s+UPDATE\s+SET\s*`,
	)
	mysqlExcludedColumnPattern = regexp.MustCompile(
		`(?i)excluded\.([a-zA-Z_][a-zA-Z0-9_]*)`,
	)
)

func translateMySQLUpsert(query string) string {
	query = mysqlIntegerCastPattern.ReplaceAllString(query, "AS SIGNED")
	return translateMySQLUpsertWithID(query, false)
}

func translateMySQLUpsertWithID(query string, returning bool) string {
	if mysqlDoNothingPattern.MatchString(query) {
		return mysqlDoNothingPattern.ReplaceAllString(
			query,
			"ON DUPLICATE KEY UPDATE id = id",
		)
	}
	if !mysqlUpdatePattern.MatchString(query) {
		return query
	}
	translated := mysqlUpdatePattern.ReplaceAllString(
		query,
		"ON DUPLICATE KEY UPDATE ",
	)
	translated = mysqlExcludedColumnPattern.ReplaceAllString(
		translated,
		"VALUES($1)",
	)
	if returning {
		translated += ", id = LAST_INSERT_ID(id)"
	}
	return translated
}
