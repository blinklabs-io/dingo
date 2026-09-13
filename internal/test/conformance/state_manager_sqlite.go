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

package conformance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	badgerblob "github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	// Registers the "sqlite" driver used for the resetter's own connection.
	_ "github.com/glebarez/go-sqlite"
)

// sqliteMetadataFileName is the file the sqlite metadata provider creates
// inside the backend's data directory. It must match
// database/plugin/metadata/sqlite's dstPath.
const sqliteMetadataFileName = "metadata.sqlite"

// sqliteMetadataPath returns the metadata database path inside dataDir.
func sqliteMetadataPath(dataDir string) string {
	return filepath.Join(dataDir, sqliteMetadataFileName)
}

// sqliteResetFileURI converts an OS path to the file URI form SQLite expects.
// It mirrors sqliteFileURI in database/plugin/metadata/sqlite, which is
// unexported; the resetter must address exactly the file the metadata store
// opened, so the two constructions have to agree.
//
// Three things a bare "file:"+ToSlash concatenation gets wrong, all of which
// this package would hit: a relative path is not resolved, a Windows volume
// needs the leading slash that makes "C:/x" into "/C:/x" (the untagged
// Windows CI job builds this package), and a path containing a URI-reserved
// character -- a '?' or a space in TMPDIR -- would otherwise run into the
// '?'-terminated pragma string and misparse.
//
// TestSqliteResetPreservesMigrationLedger is the cross-platform guard that
// this really does open the store's own file: it reads schema_migrations back
// through the resetter's connection, which only exists if both addressed the
// same database.
func sqliteResetFileURI(databasePath string) string {
	if !filepath.IsAbs(databasePath) {
		if absolutePath, err := filepath.Abs(databasePath); err == nil {
			databasePath = absolutePath
		}
	}
	path := filepath.ToSlash(databasePath)
	if filepath.VolumeName(databasePath) != "" &&
		!strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return (&url.URL{Scheme: "file", Path: path}).String()
}

// newSqliteResetter builds the wipeMetadata hook for the local SQLite
// backend, replacing the close/RemoveAll/reopen path Reset previously took.
//
// That path was correct but re-ran the full migration set once per vector.
// Measured on this package under -race, Reset averaged 712ms and a CPU profile
// attributed 76.9% of it to migrations.Run/execDDL -- roughly 260 DDL
// statements (80 CREATE TABLE, 180 CREATE INDEX) per vector, ~315 vectors per
// replay, two replays per process. Emptying the tables in place over one
// long-lived connection removes that work while leaving the schema, and
// therefore schema_migrations, intact.
//
// SQLite differs from the two remote dialects in three ways that this
// constructor has to absorb:
//
//  1. There is no TRUNCATE, so each dirty table takes a DELETE. That is why
//     the dirty-only probe matters more here than for PostgreSQL, which
//     batches every table into one statement regardless.
//  2. DELETE does not reset AUTOINCREMENT, and the recreate path did, so
//     sqlite_sequence has to be cleared for the same tables. A table can also
//     hold an advanced sequence while holding no rows, which the row probe
//     cannot see; sqliteAdvancedAutoIncrementTables reports those, the same
//     role mysqlAdvancedAutoIncrementTables plays for MySQL.
//  3. SQLite has no CASCADE on DELETE, so this connection disables foreign
//     keys rather than ordering the deletes. Correct here because the whole
//     managed set is emptied together; a partial reset is never issued.
func newSqliteResetter(databasePath string) (*backendResetter, error) {
	// foreign_keys(0) rather than the provider's (1): see the doc comment.
	// busy_timeout leads because github.com/glebarez/go-sqlite applies
	// _pragma options in order and anything ahead of it runs with no busy
	// handler installed.
	dsn := sqliteResetFileURI(databasePath) +
		"?_pragma=busy_timeout(30000)" +
		"&_pragma=foreign_keys(0)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf(
			"open sqlite reset connection at %q: %w",
			databasePath,
			err,
		)
	}
	// One connection, held for the manager's lifetime. Reset is the only
	// user and a second pooled connection would not share the
	// foreign_keys pragma reliably.
	db.SetMaxOpenConns(1)

	return &backendResetter{
		db:         db,
		listTables: listSqliteConformanceTables,
		qualify:    sqliteQuoteIdentifier,
		truncate:   truncateSqliteTables,
		extraDirty: sqliteAdvancedAutoIncrementTables,
	}, nil
}

// listSqliteConformanceTables returns the managed base tables: everything in
// this file except SQLite's own sqlite_* bookkeeping (sqlite_sequence in
// particular, which truncateSqliteTables maintains rather than empties) and
// the migration runner's schema_migrations ledger. Emptying that ledger would
// make the next construction re-run every migration, which is the cost this
// path exists to avoid.
func listSqliteConformanceTables(
	ctx context.Context,
	db *sql.DB,
) ([]string, error) {
	rows, err := db.QueryContext(
		ctx,
		`SELECT name FROM sqlite_master
		 WHERE type = 'table'
		   AND name NOT LIKE 'sqlite_%'
		   AND name <> 'schema_migrations'
		 ORDER BY name`,
	)
	if err != nil {
		return nil, fmt.Errorf("list sqlite conformance tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan sqlite table name: %w", err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list sqlite conformance tables: %w", err)
	}
	return tables, nil
}

// sqliteQuoteIdentifier renders a bare table name as a quoted identifier,
// doubling any embedded quote. SQLite has no schema qualifier to add here: the
// resetter's connection is opened directly against this backend's own file.
func sqliteQuoteIdentifier(table string) string {
	return `"` + strings.ReplaceAll(table, `"`, `""`) + `"`
}

// truncateSqliteTables empties the given already-qualified tables and clears
// their AUTOINCREMENT counters, in one transaction so a failure part-way
// cannot leave some vectors' rows behind.
//
// sqlite_sequence only exists once at least one table has been declared
// AUTOINCREMENT, so its absence is not an error; the DELETE against it is
// skipped rather than allowed to fail.
func truncateSqliteTables(
	ctx context.Context,
	db *sql.DB,
	qualified []string,
) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin sqlite reset transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	for _, table := range qualified {
		// #nosec G202 -- table is a sqlite_master name that
		// sqliteQuoteIdentifier has already quoted; never caller input.
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			return fmt.Errorf("delete from sqlite table %s: %w", table, err)
		}
	}

	hasSequence, err := sqliteSequenceExists(ctx, tx)
	if err != nil {
		return err
	}
	if hasSequence {
		// Match on the bare name, so strip the quoting applied above.
		names := make([]any, len(qualified))
		placeholders := make([]string, len(qualified))
		for i, table := range qualified {
			names[i] = sqliteUnquoteIdentifier(table)
			placeholders[i] = "?"
		}
		// #nosec G202 -- the concatenated text is a generated list of
		// "?" placeholders; every table name is a bound parameter.
		if _, err := tx.ExecContext(
			ctx,
			"DELETE FROM sqlite_sequence WHERE name IN ("+
				strings.Join(placeholders, ",")+")",
			names...,
		); err != nil {
			return fmt.Errorf("reset sqlite autoincrement counters: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit sqlite reset transaction: %w", err)
	}
	return nil
}

// sqliteUnquoteIdentifier reverses sqliteQuoteIdentifier.
func sqliteUnquoteIdentifier(qualified string) string {
	trimmed := strings.TrimSuffix(strings.TrimPrefix(qualified, `"`), `"`)
	return strings.ReplaceAll(trimmed, `""`, `"`)
}

// sqliteSequenceExists reports whether the sqlite_sequence table is present.
// It takes the narrow QueryRowContext interface so the same probe serves both
// the pooled connection and an open transaction.
func sqliteSequenceExists(
	ctx context.Context,
	q sqliteRowQuerier,
) (bool, error) {
	var name string
	err := q.QueryRowContext(
		ctx,
		`SELECT name FROM sqlite_master
		 WHERE type = 'table' AND name = 'sqlite_sequence'`,
	).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("probe sqlite_sequence: %w", err)
	}
	return true, nil
}

// sqliteRowQuerier is the single-row query surface shared by *sql.DB and
// *sql.Tx.
type sqliteRowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// sqliteAdvancedAutoIncrementTables reports managed tables whose
// AUTOINCREMENT counter has advanced, so a table emptied by an earlier reset
// still gets its sequence cleared. Without this, the dirty-only row probe
// would skip such a table and the next insert would continue from the previous
// vector's high water mark rather than from 1, which the close-and-reopen path
// this replaces always gave. It is the SQLite counterpart to
// mysqlAdvancedAutoIncrementTables.
func sqliteAdvancedAutoIncrementTables(
	ctx context.Context,
	db *sql.DB,
	tables []string,
) ([]string, error) {
	if len(tables) == 0 {
		return nil, nil
	}
	hasSequence, err := sqliteSequenceExists(ctx, db)
	if err != nil {
		return nil, err
	}
	if !hasSequence {
		return nil, nil
	}
	rows, err := db.QueryContext(
		ctx,
		`SELECT name FROM sqlite_sequence WHERE seq > 0`,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"list sqlite advanced autoincrement tables: %w",
			err,
		)
	}
	defer rows.Close()

	managed := make(map[string]struct{}, len(tables))
	for _, table := range tables {
		managed[table] = struct{}{}
	}
	var advanced []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan sqlite_sequence name: %w", err)
		}
		if _, ok := managed[name]; ok {
			advanced = append(advanced, name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"list sqlite advanced autoincrement tables: %w",
			err,
		)
	}
	return advanced, nil
}

// installSqliteResetHooks points the manager's Reset at the in-place path:
// truncate the dirty metadata tables over a held connection, then empty the
// local Badger blob store, instead of closing the backend, deleting the data
// directory, and reopening it.
//
// Both hooks are required together. database.New checks that the blob store's
// commit timestamp agrees with the metadata store's, so emptying one side and
// leaving the other advanced would produce exactly the mismatched pairing
// state_manager_postgres.go describes.
func installSqliteResetHooks(m *DingoStateManager, dataDir string) error {
	resetter, err := newSqliteResetter(sqliteMetadataPath(dataDir))
	if err != nil {
		return err
	}
	m.wipeMetadata = func() error {
		return resetter.reset(context.Background())
	}
	m.wipeBlob = func() error {
		return dropSqliteBackendBlobs(m)
	}
	m.closeExtra = resetter.Close
	return nil
}

// dropSqliteBackendBlobs empties the local Badger blob store in place.
//
// badger's DropAll is what makes this cheap: it discards every table without
// closing the store, so the blob half of Reset costs one call rather than a
// close, a directory removal, and a reopen.
func dropSqliteBackendBlobs(m *DingoStateManager) error {
	if m.db == nil {
		return nil
	}
	store := m.db.Blob()
	if store == nil {
		return nil
	}
	badgerStore, ok := store.(*badgerblob.BlobStoreBadger)
	if !ok {
		return fmt.Errorf(
			"conformance: sqlite reset expects a badger blob store, got %T",
			store,
		)
	}
	if err := badgerStore.DB().DropAll(); err != nil {
		return fmt.Errorf("drop badger blob store for reset: %w", err)
	}
	return nil
}
