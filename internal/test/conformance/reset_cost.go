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

// No build tag: backendResetter and its helpers are dialect-agnostic
// database/sql code with no plugin dependency. The tag this file used to
// carry came from its callers -- when only the dingo_extra_plugins-gated
// Postgres and MySQL managers used it -- and keeping it would have forced the
// SQLite reset path (state_manager_sqlite.go), which every build has, to exist
// only in the tagged configuration. Letting the two configurations diverge
// there is exactly what this package avoids elsewhere; see
// process_cleanup_test.go.

package conformance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// Reset runs once per vector, so anything it does is multiplied by the size of
// the corpus. The corpus is 2,574 Blueprint vectors plus one synthetic
// rollback fixture, and the rollback fixture resets a second time when it
// rolls back, so a replay makes 2,576 Reset calls -- one per vector, not a
// multiple of it. A vector dirties about 12 of the 89 managed tables.
//
// Measured against the pre-change reset path, each external backend paid three
// separate per-vector costs:
//
//  1. a fresh sql.Open plus Close, so a TCP connect and authentication
//     handshake per vector;
//  2. an information_schema query per vector, re-deriving a table list that
//     cannot change (migrations run once, at construction);
//  3. a statement per table per vector, against all 89 rather than the ~12 a
//     vector wrote.
//
// backendResetter removes all three: it holds one admin connection for the
// manager's lifetime, caches the table list after the first successful
// discovery, and clears only the tables that actually hold rows.
//
// What remained after that was the cost of the clearing statement itself, and
// it differs by dialect:
//
//   - PostgreSQL takes every dirty table in one TRUNCATE, so its reset is one
//     round trip whatever the table count.
//   - MySQL has no multi-table TRUNCATE, and InnoDB implements TRUNCATE by
//     dropping and recreating the table's tablespace, so the cost is per table
//     and does not fall when the table holds a handful of rows. Measured on a
//     mysql:8 container with the CI service's settings, 12 TRUNCATEs cost
//     1,687ms against 23ms for the same 12 tables deleted in one transaction.
//     deleteMysqlTables therefore batches DELETEs instead; see its doc
//     comment.
//   - SQLite has no TRUNCATE at all and always deleted, but its reset
//     transaction committed with the connection's default synchronous
//     setting. newSqliteResetter opens that connection with synchronous(0),
//     which took the delete phase of a full replay from 33.3s to 3.9s.
//
// Dropping TRUNCATE on MySQL gives up the AUTO_INCREMENT restart TRUNCATE
// performs and DELETE does not. Nothing in the suite needs it: the PostgreSQL
// backend's TRUNCATE carries no RESTART IDENTITY, so its sequences have never
// restarted between vectors either, and TestRulesConformanceVectorsPostgres
// asserts that backend reproduces the SQLite baseline vector for vector.
// SQLite keeps its AUTOINCREMENT reset because it costs one more DELETE inside
// the transaction it already runs.
type backendResetter struct {
	db *sql.DB

	// listTables discovers the base tables to manage, excluding the
	// migration runner's own schema_migrations bookkeeping.
	listTables func(context.Context, *sql.DB) ([]string, error)

	// qualify fully quotes a bare table name for this dialect.
	qualify func(string) string

	// truncate empties exactly the given already-qualified tables. Callers
	// guarantee a non-empty slice.
	truncate func(context.Context, *sql.DB, []string) error

	// extraDirty reports bare table names that must be truncated even when
	// they hold no rows, because emptiness is not the only state a Reset has
	// to clear. Optional; nil means rows are the only criterion.
	extraDirty func(context.Context, *sql.DB, []string) ([]string, error)

	// probeDirty reports which of the given qualified tables hold rows.
	// Injectable so reset's skip/subset behavior is testable without a
	// server; nil means nonEmptyTables, which is what both real backends
	// use.
	probeDirty func(context.Context, *sql.DB, []string) ([]string, error)

	// mu guards tables, discovered and the prepared probe across the
	// sequential-but-not-guaranteed-single-goroutine Reset calls the
	// harness makes.
	mu         sync.Mutex
	tables     []string
	discovered bool
	probeStmt  *sql.Stmt
	probeSQL   string
}

// probeStatement returns the prepared form of the non-empty probe, compiling
// it at most once per distinct table list.
//
// Preparing matters because the probe's text is one UNION ALL branch per
// managed table -- 89 of them here -- and every Reset re-sent it. Almost all
// of its cost is compiling that text, not running it: measured over 200 calls
// against a migrated conformance database, 1.33ms ad hoc against 0.07ms
// prepared, and 31.78ms against 2.20ms under -race, where the compiler is
// instrumented Go in modernc.org/sqlite.
//
// The table list is discovered once and cannot change afterwards, so in
// practice this compiles exactly one statement; the text is still compared so
// a caller that somehow probes a different set gets a correct statement rather
// than a stale one.
func (r *backendResetter) probeStatement(
	ctx context.Context,
	qualified []string,
) (*sql.Stmt, error) {
	query := nonEmptyTablesQuery(qualified)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.probeStmt != nil && r.probeSQL == query {
		return r.probeStmt, nil
	}
	stmt, err := r.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("prepare non-empty table probe: %w", err)
	}
	if r.probeStmt != nil {
		_ = r.probeStmt.Close()
	}
	r.probeStmt, r.probeSQL = stmt, query
	return stmt, nil
}

// reset empties every non-empty managed table. It is the wipeMetadata hook.
func (r *backendResetter) reset(ctx context.Context) error {
	tables, err := r.cachedTables(ctx)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		// Nothing migrated yet (Reset can be called before any construction
		// has run migrations against this schema/database).
		return nil
	}

	qualified := make([]string, len(tables))
	for i, table := range tables {
		qualified[i] = r.qualify(table)
	}

	dirty, err := r.dirtyTables(ctx, qualified)
	if err != nil {
		return err
	}
	if r.extraDirty != nil {
		extra, err := r.extraDirty(ctx, r.db, tables)
		if err != nil {
			return err
		}
		dirty = mergeQualified(dirty, extra, r.qualify)
	}
	if len(dirty) == 0 {
		// The common case for a vector that wrote nothing, and for the first
		// vector of a run. No DDL at all.
		return nil
	}
	return r.truncate(ctx, r.db, dirty)
}

// dirtyTables reports which managed tables hold rows, through the injected
// probe when a test supplied one and through the prepared statement
// otherwise.
func (r *backendResetter) dirtyTables(
	ctx context.Context,
	qualified []string,
) ([]string, error) {
	if r.probeDirty != nil {
		return r.probeDirty(ctx, r.db, qualified)
	}
	if len(qualified) == 0 {
		return nil, nil
	}
	// The statement is cached for the resetter's lifetime and closed by
	// Close, so it deliberately outlives this call.
	//nolint:sqlclosecheck
	stmt, err := r.probeStatement(ctx, qualified)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("probe non-empty tables: %w", err)
	}
	return scanNonEmptyTables(rows, qualified)
}

// cachedTables returns the managed table list, discovering it at most once.
//
// An empty result is deliberately not cached: Reset can run before migrations
// have created anything, and caching that would leave the resetter permanently
// convinced the schema is empty.
func (r *backendResetter) cachedTables(ctx context.Context) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.discovered {
		return r.tables, nil
	}
	tables, err := r.listTables(ctx, r.db)
	if err != nil {
		return nil, err
	}
	if len(tables) == 0 {
		return nil, nil
	}
	r.tables, r.discovered = tables, true
	return r.tables, nil
}

// Close releases the prepared probe and the long-lived admin connection.
func (r *backendResetter) Close() error {
	err := r.closeProbe()
	if r.db == nil {
		return err
	}
	return errors.Join(err, r.db.Close())
}

// closeProbe releases the prepared probe statement, leaving the connection
// open. Split out so a caller holding a resetter over a connection it owns
// itself -- the dialect probe tests -- can release the statement without
// closing that connection.
func (r *backendResetter) closeProbe() error {
	r.mu.Lock()
	stmt := r.probeStmt
	r.probeStmt, r.probeSQL = nil, ""
	r.mu.Unlock()
	if stmt == nil {
		return nil
	}
	return stmt.Close()
}

// nonEmptyTablesQuery renders the probe that reports which of qualified
// currently holds at least one row, in a single round trip.
//
// The query is one UNION ALL of EXISTS probes, selecting each table's index
// rather than its name so no identifier ever has to survive being embedded in
// a string literal. Asking per table instead would trade the per-table
// statement this exists to avoid for a per-table SELECT, which is cheaper but
// still O(tables) round trips; this stays at one regardless of schema size.
//
// EXISTS stops at the first row, so a probe against a large table is no more
// expensive than against a small one.
//
// The index must be single-quoted: that makes it a SQL string literal, where
// double quotes would be an identifier reference in PostgreSQL and select a
// column named after the number. Both PostgreSQL and MySQL 8 accept a
// FROM-less `SELECT ... WHERE ...`, so no dummy FROM is needed; the MySQL
// restriction on that shape applies to 5.x, and this repository's services
// pin mysql:8.
func nonEmptyTablesQuery(qualified []string) string {
	var query strings.Builder
	for i, table := range qualified {
		if i > 0 {
			query.WriteString(" UNION ALL ")
		}
		// The literal is a decimal index this function generated, never
		// caller or operator input.
		query.WriteString("SELECT '")
		query.WriteString(strconv.Itoa(i))
		query.WriteString("' AS i WHERE EXISTS ")
		query.WriteString("(SELECT 1 FROM ")
		query.WriteString(table)
		query.WriteString(")")
	}

	return query.String()
}

// scanNonEmptyTables maps the probe's returned indexes back to table names.
func scanNonEmptyTables(
	rows *sql.Rows,
	qualified []string,
) ([]string, error) {
	defer rows.Close()

	var dirty []string
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan non-empty table index: %w", err)
		}
		idx, err := strconv.Atoi(raw)
		if err != nil || idx < 0 || idx >= len(qualified) {
			return nil, fmt.Errorf(
				"non-empty table probe returned unusable index %q",
				raw,
			)
		}
		dirty = append(dirty, qualified[idx])
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("probe non-empty tables: %w", err)
	}
	return dirty, nil
}

// mergeQualified adds the qualified form of each extra bare table name to
// dirty, skipping any already present, so a table reported by both the row
// probe and an extra criterion is truncated once.
func mergeQualified(
	dirty []string,
	extra []string,
	qualify func(string) string,
) []string {
	if len(extra) == 0 {
		return dirty
	}
	seen := make(map[string]struct{}, len(dirty))
	for _, table := range dirty {
		seen[table] = struct{}{}
	}
	for _, table := range extra {
		qualified := qualify(table)
		if _, ok := seen[qualified]; ok {
			continue
		}
		seen[qualified] = struct{}{}
		dirty = append(dirty, qualified)
	}
	return dirty
}
