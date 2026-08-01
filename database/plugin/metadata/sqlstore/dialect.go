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

// Package sqlstore contains the shared database/sql metadata store.
package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Dialect is the deliberately small backend capability boundary. Metadata
// orchestration belongs in Store; only SQL mechanics and backend tuning belong
// behind this interface.
type Dialect interface {
	Name() string
	Rebind(string) string
	QuoteIdentifier(string) string
	BooleanLiteral(bool) string
	ParameterLimit() int
	SupportsReturning() bool
	BeginOptions(readOnly bool) *sql.TxOptions
	SetBulkMode(context.Context, Execer) error
	RestoreNormalMode(context.Context, Execer) error
	UpdatePlannerStats(context.Context, Execer) error
	DropIndexSQL(name, table string) string
	CreateIndexSQL(name, table string, columns []string) string
}

// Execer is implemented by *sql.DB, *sql.Conn, and *sql.Tx.
type Execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type dialect struct {
	name              string
	quote             byte
	parameterLimit    int
	supportsReturning bool
	rebind            func(string) string
	beginOptions      func(bool) *sql.TxOptions
	setBulk           func(context.Context, Execer) error
	restore           func(context.Context, Execer) error
	analyze           func(context.Context, Execer) error
}

func (d dialect) Name() string {
	return d.name
}

func (d dialect) Rebind(query string) string {
	return d.rebind(query)
}

func (d dialect) QuoteIdentifier(identifier string) string {
	q := string(d.quote)
	return q + strings.ReplaceAll(identifier, q, q+q) + q
}

func (d dialect) BooleanLiteral(value bool) string {
	if value {
		return "TRUE"
	}
	return "FALSE"
}

func (d dialect) ParameterLimit() int {
	return d.parameterLimit
}

func (d dialect) SupportsReturning() bool {
	return d.supportsReturning
}

func (d dialect) BeginOptions(readOnly bool) *sql.TxOptions {
	return d.beginOptions(readOnly)
}

func (d dialect) SetBulkMode(ctx context.Context, exec Execer) error {
	return d.setBulk(ctx, exec)
}

func (d dialect) RestoreNormalMode(ctx context.Context, exec Execer) error {
	return d.restore(ctx, exec)
}

func (d dialect) UpdatePlannerStats(ctx context.Context, exec Execer) error {
	return d.analyze(ctx, exec)
}

func (d dialect) DropIndexSQL(name, table string) string {
	if d.name == "mysql" {
		return "DROP INDEX " + d.QuoteIdentifier(name) + " ON " + d.QuoteIdentifier(table)
	}
	return "DROP INDEX IF EXISTS " + d.QuoteIdentifier(name)
}

func (d dialect) CreateIndexSQL(name, table string, columns []string) string {
	quoted := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = d.QuoteIdentifier(column)
	}
	if d.name == "mysql" {
		return "CREATE INDEX " + d.QuoteIdentifier(name) + " ON " + d.QuoteIdentifier(table) +
			" (" + strings.Join(quoted, ", ") + ")"
	}
	return "CREATE INDEX IF NOT EXISTS " + d.QuoteIdentifier(name) + " ON " + d.QuoteIdentifier(table) +
		" (" + strings.Join(quoted, ", ") + ")"
}

// SQLiteDialect returns the capabilities used by the pure-Go SQLite driver.
func SQLiteDialect() Dialect {
	return dialect{
		name:              "sqlite",
		quote:             '"',
		parameterLimit:    999,
		supportsReturning: true,
		rebind:            identity,
		beginOptions: func(readOnly bool) *sql.TxOptions {
			return &sql.TxOptions{ReadOnly: readOnly}
		},
		setBulk: execStatements(
			"PRAGMA synchronous = OFF",
			"PRAGMA cache_size = -200000",
			"PRAGMA temp_store = MEMORY",
			"PRAGMA wal_autocheckpoint = 10000",
		),
		restore: execStatements(
			"PRAGMA synchronous = NORMAL",
			"PRAGMA cache_size = -50000",
			"PRAGMA temp_store = DEFAULT",
			"PRAGMA wal_autocheckpoint = 1000",
		),
		analyze: execStatements("ANALYZE"),
	}
}

// PostgresDialect returns PostgreSQL database/sql capabilities.
func PostgresDialect() Dialect {
	return dialect{
		name:              "postgres",
		quote:             '"',
		parameterLimit:    65535,
		supportsReturning: true,
		rebind:            rebindPostgres,
		beginOptions: func(readOnly bool) *sql.TxOptions {
			if !readOnly {
				return nil
			}
			return &sql.TxOptions{
				Isolation: sql.LevelRepeatableRead,
				ReadOnly:  true,
			}
		},
		setBulk: execStatements(
			"SET synchronous_commit = OFF",
			"SET session_replication_role = replica",
		),
		restore: execStatements(
			"SET session_replication_role = DEFAULT",
			"SET synchronous_commit = DEFAULT",
		),
		analyze: execStatements("ANALYZE"),
	}
}

// MySQLDialect returns MySQL database/sql capabilities.
func MySQLDialect() Dialect {
	return dialect{
		name:              "mysql",
		quote:             '`',
		parameterLimit:    65535,
		supportsReturning: false,
		rebind:            identity,
		beginOptions: func(readOnly bool) *sql.TxOptions {
			if !readOnly {
				return nil
			}
			return &sql.TxOptions{
				Isolation: sql.LevelRepeatableRead,
				ReadOnly:  true,
			}
		},
		setBulk: execStatements(
			"SET foreign_key_checks = 0",
			"SET unique_checks = 0",
		),
		restore: execStatements(
			"SET unique_checks = 1",
			"SET foreign_key_checks = 1",
		),
		analyze: func(context.Context, Execer) error {
			// MySQL requires a table list. The store's deferred-index manager
			// supplies it where planner refresh is needed.
			return nil
		},
	}
}

func identity(query string) string {
	return query
}

func execStatements(statements ...string) func(context.Context, Execer) error {
	return func(ctx context.Context, exec Execer) error {
		for _, statement := range statements {
			if _, err := exec.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("%s: %w", statement, err)
			}
		}
		return nil
	}
}

// rebindPostgres replaces anonymous bind parameters while leaving question
// marks in SQL strings, identifiers, and comments untouched.
func rebindPostgres(query string) string {
	var result strings.Builder
	result.Grow(len(query) + 8)
	var parameter int
	for i := 0; i < len(query); {
		switch query[i] {
		case '\'', '"', '`':
			quote := query[i]
			start := i
			i++
			for i < len(query) {
				if query[i] == quote {
					i++
					if i < len(query) && query[i] == quote {
						i++
						continue
					}
					break
				}
				i++
			}
			result.WriteString(query[start:i])
		case '-':
			if i+1 < len(query) && query[i+1] == '-' {
				end := strings.IndexByte(query[i:], '\n')
				if end < 0 {
					result.WriteString(query[i:])
					return result.String()
				}
				end += i + 1
				result.WriteString(query[i:end])
				i = end
				continue
			}
			result.WriteByte(query[i])
			i++
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				end := strings.Index(query[i+2:], "*/")
				if end < 0 {
					result.WriteString(query[i:])
					return result.String()
				}
				end += i + 4
				result.WriteString(query[i:end])
				i = end
				continue
			}
			result.WriteByte(query[i])
			i++
		case '?':
			parameter++
			fmt.Fprintf(&result, "$%d", parameter)
			i++
		default:
			result.WriteByte(query[i])
			i++
		}
	}
	return result.String()
}
