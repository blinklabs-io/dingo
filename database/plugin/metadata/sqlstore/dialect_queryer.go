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
	if dialect == "sqlite" {
		return db
	}
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

	base, doNothing := translateMySQLReturning(query)
	// QueryRowContext executes the stripped INSERT directly instead of going
	// through translate(), so apply identifier quoting here as well.  This is
	// required for statements touching reserved names such as `transaction`
	// and `index` when MySQL ANSI_QUOTES is disabled.
	base = translateMySQLReservedIdentifiers(base)
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
	// LastInsertId is returned in the same OK packet as the INSERT/UPDATE.
	// Reading LAST_INSERT_ID() with a second query is unsafe when queryer is a
	// *sql.DB: database/sql may route that query to another pooled connection,
	// whose session state belongs to a different request.  The translated
	// upsert assigns LAST_INSERT_ID(id) on the server, so the result carries the
	// existing row ID as well as newly generated IDs.  Return a one-row query
	// backed by the value already obtained from this execution instead.
	lastID, idErr := result.LastInsertId()
	if idErr != nil {
		return q.queryer.QueryRowContext(
			ctx,
			"SELECT * FROM __dingo_sqlstore_query_error__",
		)
	}
	if lastID == 0 {
		// A zero ID cannot satisfy the public queryReturnedID contract.  This is
		// also the safe result for a driver that does not report an ID for an
		// unusual upsert statement.
		return q.queryer.QueryRowContext(ctx, "SELECT NULL WHERE FALSE")
	}
	return q.queryer.QueryRowContext(ctx, "SELECT ?", lastID)
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
			query, _ = translateMySQLReturning(query)
		}
		return translateMySQLReservedIdentifiers(translateMySQLUpsert(query))
	}
	return query
}

// SQLite and PostgreSQL both accept double-quoted identifiers, while MySQL
// treats double quotes as string delimiters unless ANSI_QUOTES is enabled.
// Keep conversion here so every query path (including generated sqlc queries)
// gets the same behavior without duplicating dialect branches in business
// code. Shared SQL uses double quotes only for identifiers; values use the
// standard single-quoted spelling.
func translateMySQLReservedIdentifiers(query string) string {
	var translated strings.Builder
	translated.Grow(len(query))
	for i := 0; i < len(query); {
		switch query[i] {
		case '\'':
			// Single-quoted strings are values, not identifiers. Copy the
			// complete literal without translating double quotes inside it.
			start := i
			i++
			for i < len(query) {
				if query[i] != '\'' {
					i++
					continue
				}
				i++
				if i < len(query) && query[i] == '\'' {
					i++
					continue
				}
				break
			}
			translated.WriteString(query[start:i])
		case '"':
			i++
			var identifier strings.Builder
			for i < len(query) {
				if query[i] != '"' {
					identifier.WriteByte(query[i])
					i++
					continue
				}
				i++
				if i < len(query) && query[i] == '"' {
					identifier.WriteString("``")
					i++
					continue
				}
				break
			}
			translated.WriteByte('`')
			translated.WriteString(identifier.String())
			translated.WriteByte('`')
		case '-':
			if i+1 < len(query) && query[i+1] == '-' {
				start := i
				i += 2
				for i < len(query) && query[i] != '\n' {
					i++
				}
				translated.WriteString(query[start:i])
			} else {
				translated.WriteByte(query[i])
				i++
			}
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				start := i
				i += 2
				for i+1 < len(query) && (query[i] != '*' || query[i+1] != '/') {
					i++
				}
				if i+1 < len(query) {
					i += 2
				}
				translated.WriteString(query[start:i])
			} else {
				translated.WriteByte(query[i])
				i++
			}
		default:
			translated.WriteByte(query[i])
			i++
		}
	}
	return translated.String()
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

func translateMySQLReturning(query string) (string, bool) {
	base := strings.TrimSpace(returningIDPattern.ReplaceAllString(query, ""))
	base = mysqlIntegerCastPattern.ReplaceAllString(base, "AS SIGNED")
	doNothing := mysqlDoNothingPattern.MatchString(base)
	isUpsert := mysqlUpdatePattern.MatchString(base)
	return translateMySQLUpsertWithID(base, isUpsert), doNothing
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
	mysqlInsertColumnsPattern = regexp.MustCompile(
		`(?is)\bINSERT\s+INTO\s+(?:[\w.]+|` + "`[^`]+`" + `|"[^"]+")\s*\(([^)]*)\)`,
	)
)

func translateMySQLUpsert(query string) string {
	query = mysqlIntegerCastPattern.ReplaceAllString(query, "AS SIGNED")
	return translateMySQLUpsertWithID(query, false)
}

func translateMySQLUpsertWithID(query string, returning bool) string {
	if mysqlDoNothingPattern.MatchString(query) {
		// MySQL has no DO NOTHING form.  Use a no-op assignment on a column
		// that is actually present in the INSERT list; not every metadata
		// table has an `id` column (sync_state is the notable example).
		column := mysqlInsertNoOpColumn(query)
		return mysqlDoNothingPattern.ReplaceAllString(
			query,
			"ON DUPLICATE KEY UPDATE "+column+" = "+column,
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

func mysqlInsertNoOpColumn(query string) string {
	match := mysqlInsertColumnsPattern.FindStringSubmatch(query)
	if len(match) != 2 {
		// All current INSERT statements provide an explicit column list. Keep a
		// syntactically valid fallback for future statements that do not; every
		// metadata table with a duplicate-key path currently has an ID column.
		return "id"
	}
	columns := strings.Split(match[1], ",")
	if len(columns) == 0 || strings.TrimSpace(columns[0]) == "" {
		return "id"
	}
	return strings.TrimSpace(columns[0])
}
