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

//go:build dingo_extra_plugins

package conformance

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// Reset runs once per vector, so anything it does is multiplied by the size of
// the corpus (~315 vectors). Measured against the pre-change reset path, each
// external backend paid three separate per-vector costs, and the eight-replay
// baseline run spent 4m40s of CPU across 55 minutes of wall clock -- 8.4%,
// i.e. almost entirely blocked on database round trips rather than on vector
// work:
//
//  1. a fresh sql.Open plus Close, so a TCP connect and authentication
//     handshake per vector;
//  2. an information_schema query per vector, re-deriving a table list that
//     cannot change (migrations run once, at construction);
//  3. for MySQL, one TRUNCATE statement per table per vector. At 84 tables
//     that is ~26,460 implicit-commit DDL statements per replay. PostgreSQL
//     already batched into a single multi-table TRUNCATE.
//
// backendResetter removes all three: it holds one admin connection for the
// manager's lifetime, caches the table list after the first successful
// discovery, and truncates only the tables that actually hold rows.
//
// Truncating only non-empty tables is what makes the cost proportional to what
// a vector wrote rather than to the schema size, and it deliberately keeps
// TRUNCATE rather than switching to a single batched DELETE transaction:
// TRUNCATE resets AUTO_INCREMENT and DELETE does not, and 78 of the 84 tables
// carry such a column. Restoring those counters would mean an ALTER TABLE per
// table, putting the per-table DDL straight back.
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

	// probeDirty reports which of the given qualified tables hold rows.
	// Injectable so reset's skip/subset behavior is testable without a
	// server; nil means nonEmptyTables, which is what both real backends
	// use.
	probeDirty func(context.Context, *sql.DB, []string) ([]string, error)

	// mu guards tables and discovered across the sequential-but-not-
	// guaranteed-single-goroutine Reset calls the harness makes.
	mu         sync.Mutex
	tables     []string
	discovered bool
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

	probe := r.probeDirty
	if probe == nil {
		probe = nonEmptyTables
	}
	dirty, err := probe(ctx, r.db, qualified)
	if err != nil {
		return err
	}
	if len(dirty) == 0 {
		// The common case for a vector that wrote nothing, and for the first
		// vector of a run. No DDL at all.
		return nil
	}
	return r.truncate(ctx, r.db, dirty)
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

// Close releases the long-lived admin connection.
func (r *backendResetter) Close() error {
	if r.db == nil {
		return nil
	}
	return r.db.Close()
}

// nonEmptyTables returns the subset of qualified that currently holds at least
// one row, in a single round trip.
//
// The query is one UNION ALL of EXISTS probes, selecting each table's index
// rather than its name so no identifier ever has to survive being embedded in
// a string literal. Asking per table instead would trade the per-table
// TRUNCATE this exists to avoid for a per-table SELECT, which is cheaper but
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
func nonEmptyTables(
	ctx context.Context,
	db *sql.DB,
	qualified []string,
) ([]string, error) {
	if len(qualified) == 0 {
		return nil, nil
	}

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

	rows, err := db.QueryContext(ctx, query.String())
	if err != nil {
		return nil, fmt.Errorf("probe non-empty tables: %w", err)
	}
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
