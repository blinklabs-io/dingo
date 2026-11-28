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
	// SupportsReturning reports whether UPDATE and INSERT accept RETURNING.
	SupportsReturning() bool
	Rebind(string) string
	QuoteIdentifier(string) string
	ParameterLimit() int
	BeginOptions(readOnly bool) *sql.TxOptions
	UpdatePlannerStats(context.Context, Execer) error
	// OptimizePlannerStats runs the backend's incremental statistics
	// maintenance. supported is false, with no statement issued, for a
	// backend that has none; UpdatePlannerStats (a full ANALYZE) is unchanged.
	OptimizePlannerStats(context.Context, Execer) (supported bool, err error)
	// HasIncrementalPlannerStats reports whether OptimizePlannerStats does
	// anything on this backend, without issuing a statement.
	HasIncrementalPlannerStats() bool
	DropIndexSQL(name, table string) string
	CreateIndexSQL(name, table string, columns []string) string
	CanDropIndex(name, table string) bool
	UpdateFromJoinSQL(
		target, source, joinCondition string,
		assignments []JoinAssignment,
	) string
}

// JoinAssignment sets an UPDATE target's Column to Expr, an expression that
// typically references a joined source table's column of the same or a
// different name. Column is always unqualified; UpdateFromJoinSQL's
// implementation decides whether and how to qualify it for its engine.
type JoinAssignment struct {
	Column string
	Expr   string
}

// Execer is implemented by *sql.DB, *sql.Conn, and *sql.Tx.
type Execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type dialect struct {
	name           string
	quote          byte
	parameterLimit int
	rebind         func(string) string
	beginOptions   func(bool) *sql.TxOptions
	analyze        func(context.Context, Execer) error
	optimize       func(context.Context, Execer) error
	updateFromJoin func(string, string, string, []JoinAssignment) string
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

func (d dialect) ParameterLimit() int {
	return d.parameterLimit
}

func (d dialect) BeginOptions(readOnly bool) *sql.TxOptions {
	return d.beginOptions(readOnly)
}

func (d dialect) UpdatePlannerStats(ctx context.Context, exec Execer) error {
	return d.analyze(ctx, exec)
}

func (d dialect) OptimizePlannerStats(
	ctx context.Context,
	exec Execer,
) (bool, error) {
	if d.optimize == nil {
		return false, nil
	}
	return true, d.optimize(ctx, exec)
}

func (d dialect) HasIncrementalPlannerStats() bool {
	return d.optimize != nil
}

func (d dialect) DropIndexSQL(name, table string) string {
	if d.name == "mysql" {
		return "DROP INDEX " + d.QuoteIdentifier(
			name,
		) + " ON " + d.QuoteIdentifier(
			table,
		)
	}
	return "DROP INDEX IF EXISTS " + d.QuoteIdentifier(name)
}

// CanDropIndex reports whether a deferred-index cycle may remove an index.
// InnoDB requires an index on every foreign-key child column and refuses to
// drop that index even when foreign_key_checks is disabled. SQLite and
// PostgreSQL do not have that restriction for the child side, so only the
// MySQL dialect needs a protected set here.
func (d dialect) CanDropIndex(name, table string) bool {
	if d.name != "mysql" {
		return true
	}
	return !mysqlForeignKeyIndexes[table][name]
}

// UpdateFromJoinSQL builds "UPDATE target SET ... " restricted to rows that
// join against source on joinCondition, in whichever syntax this dialect's
// engine requires. source names a table or CTE already defined earlier in
// the same statement (typically by a preceding WITH clause); this method
// only builds the final UPDATE clause, not the CTE that defines source.
func (d dialect) UpdateFromJoinSQL(
	target, source, joinCondition string,
	assignments []JoinAssignment,
) string {
	return d.updateFromJoin(target, source, joinCondition, assignments)
}

// standardUpdateFromJoinSQL builds the UPDATE ... SET ... FROM ... WHERE
// <joinCondition> form SQLite (3.33+) and PostgreSQL both accept. Assignment
// targets stay unqualified: both engines reject a table-qualified target
// ("UPDATE t SET t.col = ..." is a syntax error on both), unlike MySQL's
// JOIN form below.
func standardUpdateFromJoinSQL(
	target, source, joinCondition string,
	assignments []JoinAssignment,
) string {
	sets := make([]string, len(assignments))
	for i, a := range assignments {
		sets[i] = a.Column + " = " + a.Expr
	}
	return "UPDATE " + target + " SET " + strings.Join(sets, ", ") +
		" FROM " + source + " WHERE " + joinCondition
}

// mysqlUpdateFromJoinSQL builds MySQL's UPDATE ... JOIN ... ON ... SET ...
// form: MySQL has no UPDATE ... FROM syntax at all. Unlike
// standardUpdateFromJoinSQL, every assignment target is qualified with
// target -- source commonly projects columns with the same names as
// target's own (a "latest known value" CTE joined back onto the table it
// summarizes, for instance), and an unqualified "SET col = ..." is
// ambiguous whenever the joined source carries a same-named column (MySQL
// error 1052).
func mysqlUpdateFromJoinSQL(
	target, source, joinCondition string,
	assignments []JoinAssignment,
) string {
	sets := make([]string, len(assignments))
	for i, a := range assignments {
		sets[i] = target + "." + a.Column + " = " + a.Expr
	}
	return "UPDATE " + target + " JOIN " + source + " ON " + joinCondition +
		" SET " + strings.Join(sets, ", ")
}

func (d dialect) CreateIndexSQL(name, table string, columns []string) string {
	quoted := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = d.QuoteIdentifier(column)
		// MySQL translates SQLite BLOB/TEXT columns to VARBINARY/TEXT.
		// Those types require an explicit key prefix, matching the prefix
		// used by the version-1 schema translator for static indexes.
		if d.name == "mysql" && mysqlDeferredIndexPrefix(table, column) {
			quoted[i] += "(255)"
		}
	}
	if d.name == "mysql" {
		return "CREATE INDEX " + d.QuoteIdentifier(
			name,
		) + " ON " + d.QuoteIdentifier(
			table,
		) +
			" (" + strings.Join(
			quoted,
			", ",
		) + ")"
	}
	return "CREATE INDEX IF NOT EXISTS " + d.QuoteIdentifier(
		name,
	) + " ON " + d.QuoteIdentifier(
		table,
	) +
		" (" + strings.Join(
		quoted,
		", ",
	) + ")"
}

func mysqlDeferredIndexPrefix(table, column string) bool {
	return mysqlDeferredIndexPrefixColumns[table][column]
}

var mysqlDeferredIndexPrefixColumns = map[string]map[string]bool{
	"utxo": {
		"payment_key": true, "staking_key": true, "amount": true,
	},
	"applied_transaction":  {"block_hash": true},
	"pool_opcert_sequence": {"pool_key_hash": true},
	"asset_mint_burn": {
		"policy_id": true, "name": true, "fingerprint": true,
	},
	"address_transaction": {
		"staking_key": true,
		"payment_key": true,
	},
	"utxo_address":               {"address": true},
	"redeemer_script":            {"script_hash": true},
	"certs":                      {"block_hash": true},
	"witness_scripts":            {"script_hash": true},
	"transaction_metadata_label": {"label": true},
}

var mysqlForeignKeyIndexes = map[string]map[string]bool{
	"utxo": {
		"idx_utxo_spent_at_tx_id": true,
	},
	"redeemer":        {"idx_redeemer_transaction_id": true},
	"key_witness":     {"idx_key_witness_transaction_id": true},
	"witness_scripts": {"idx_witness_scripts_transaction_id": true},
}

// SQLiteDialect returns the capabilities used by the pure-Go SQLite driver.
func SQLiteDialect() Dialect {
	return dialect{
		name:           "sqlite",
		quote:          '"',
		parameterLimit: 999,
		rebind:         identity,
		beginOptions: func(readOnly bool) *sql.TxOptions {
			return &sql.TxOptions{ReadOnly: readOnly}
		},
		analyze: execStatements("ANALYZE"),
		// A bounded ANALYZE of every table rather than PRAGMA optimize:
		// optimize re-analyzes a table only once its row count has moved by
		// a large factor, so on tables growing steadily through a sync the
		// statistics lagged far behind and steered queries to indexes that
		// were selective only when the table was small. analysis_limit
		// samples each index instead of reading it whole, which keeps the
		// sole write connection's pause short, and the connection's own
		// limit is restored afterwards so an explicit full ANALYZE stays
		// exact.
		optimize:       sqliteBoundedAnalyze,
		updateFromJoin: standardUpdateFromJoinSQL,
	}
}

// PostgresDialect returns PostgreSQL database/sql capabilities.
func PostgresDialect() Dialect {
	return dialect{
		name:           "postgres",
		quote:          '"',
		parameterLimit: 65535,
		rebind:         rebindPostgres,
		beginOptions: func(readOnly bool) *sql.TxOptions {
			if !readOnly {
				return nil
			}
			return &sql.TxOptions{
				Isolation: sql.LevelRepeatableRead,
				ReadOnly:  true,
			}
		},
		analyze:        execStatements("ANALYZE"),
		updateFromJoin: standardUpdateFromJoinSQL,
	}
}

// MySQLDialect returns MySQL database/sql capabilities.
func MySQLDialect() Dialect {
	return dialect{
		name:           "mysql",
		quote:          '`',
		parameterLimit: 65535,
		rebind:         identity,
		beginOptions: func(readOnly bool) *sql.TxOptions {
			if !readOnly {
				return nil
			}
			return &sql.TxOptions{
				Isolation: sql.LevelRepeatableRead,
				ReadOnly:  true,
			}
		},
		analyze: func(context.Context, Execer) error {
			// MySQL requires a table list. The store's deferred-index manager
			// supplies it where planner refresh is needed.
			return nil
		},
		updateFromJoin: mysqlUpdateFromJoinSQL,
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

// SupportsReturning reports whether UPDATE and INSERT accept a RETURNING
// clause.
func (d dialect) SupportsReturning() bool {
	return d.name != "mysql"
}

// sqliteAnalysisLimit is the rows per index a periodic ANALYZE samples.
const sqliteAnalysisLimit = 1000

// sqliteBoundedAnalyze runs ANALYZE under analysis_limit, then restores the
// connection's previous limit, which it can read only from an executor that
// also queries.
func sqliteBoundedAnalyze(ctx context.Context, exec Execer) error {
	previous := 0
	if q, ok := exec.(interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	}); ok {
		if err := q.QueryRowContext(ctx, "PRAGMA analysis_limit").Scan(&previous); err != nil {
			return err
		}
	}
	limit := sqliteAnalysisLimit
	if previous > 0 && previous < limit {
		limit = previous
	}
	if _, err := exec.ExecContext(ctx, fmt.Sprintf("PRAGMA analysis_limit = %d", limit)); err != nil {
		return err
	}
	_, err := exec.ExecContext(ctx, "ANALYZE")
	// Restore even when ctx was canceled mid-pass: a limit left behind would
	// sample every later explicit ANALYZE on this connection.
	if _, rerr := exec.ExecContext(
		context.WithoutCancel(ctx),
		fmt.Sprintf("PRAGMA analysis_limit = %d", previous),
	); err == nil {
		err = rerr
	}
	return err
}
