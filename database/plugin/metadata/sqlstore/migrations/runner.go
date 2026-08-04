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

package migrations

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

type Runner struct {
	DB       *sql.DB
	Dialect  string
	Registry []Migration
	Locker   Locker
	Logger   *slog.Logger
	Now      func() time.Time
}

type state struct {
	version     int
	name        string
	checksum    string
	phase       Phase
	cursor      string
	dirty       bool
	completedAt sql.NullInt64
}

// Run holds the backend migration lock for the entire compatibility check and
// upgrade. A failure prevents the caller from advertising store readiness.
func (r *Runner) Run(ctx context.Context) (runErr error) {
	if r.DB == nil {
		return errors.New("metadata migrations: database is required")
	}
	if err := validateRegistry(r.Registry, r.Dialect); err != nil {
		return err
	}
	if r.Locker == nil {
		return errors.New("metadata migrations: locker is required")
	}
	if r.Logger == nil {
		r.Logger = slog.Default()
	}
	if r.Now == nil {
		r.Now = time.Now
	}
	conn, err := r.DB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("metadata migrations: reserve connection: %w", err)
	}
	defer conn.Close()
	release, err := r.Locker.Acquire(ctx, conn)
	if err != nil {
		return fmt.Errorf("metadata migrations: acquire lock: %w", err)
	}
	defer func() {
		runErr = errors.Join(runErr, release())
	}()

	legacyTables, err := r.userTables(ctx, conn)
	if err != nil {
		return err
	}
	if err := r.ensureStateTable(ctx, conn); err != nil {
		return err
	}
	states, err := r.readStates(ctx, conn)
	if err != nil {
		return err
	}
	if err := r.validateDatabase(states); err != nil {
		return err
	}

	if len(states) == 0 && len(legacyTables) > 0 {
		return fmt.Errorf(
			"%w: existing metadata tables are from an unsupported database version; delete the metadata database and resync from genesis",
			ErrLegacySchema,
		)
	}
	target := r.Registry[len(r.Registry)-1].Version
	current := 0
	for _, migrationState := range states {
		if migrationState.phase == PhaseComplete && migrationState.version > current {
			current = migrationState.version
		}
	}
	r.Logger.Info(
		"metadata schema upgrade check",
		"current_version", current,
		"target_version", target,
	)

	for _, migration := range r.Registry {
		migrationState, exists := states[migration.Version]
		if exists && migrationState.phase == PhaseComplete {
			continue
		}
		if err := r.runMigration(ctx, conn, migration, migrationState, exists); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) runMigration(
	ctx context.Context,
	conn *sql.Conn,
	migration Migration,
	current state,
	exists bool,
) error {
	checksum := migration.checksum()
	if !exists {
		current = state{
			version:  migration.Version,
			name:     migration.Name,
			checksum: checksum,
			phase:    PhaseExpand,
			dirty:    true,
		}
		if err := r.insertState(ctx, conn, current); err != nil {
			return r.upgradeError(migration, PhaseExpand, err)
		}
	}
	sqlPhases := migration.SQL[r.Dialect]
	if current.phase == PhaseExpand {
		if err := r.setDirty(ctx, conn, migration.Version, PhaseExpand); err != nil {
			return r.upgradeError(migration, PhaseExpand, err)
		}
		if err := execDDL(ctx, conn, sqlPhases.Expand, r.Dialect); err != nil {
			return r.upgradeError(migration, PhaseExpand, err)
		}
		if err := r.setPhase(
			ctx,
			conn,
			migration.Version,
			PhaseBackfill,
			current.cursor,
			false,
		); err != nil {
			return r.upgradeError(migration, PhaseExpand, err)
		}
		current.phase = PhaseBackfill
	}
	if current.phase == PhaseBackfill {
		cursor, err := r.runBackfill(ctx, conn, migration, current.cursor)
		if err != nil {
			return r.upgradeError(migration, PhaseBackfill, err)
		}
		current.cursor = cursor
		current.phase = PhaseContract
	}
	if current.phase == PhaseContract {
		if err := r.setDirty(ctx, conn, migration.Version, PhaseContract); err != nil {
			return r.upgradeError(migration, PhaseContract, err)
		}
		if err := execDDL(ctx, conn, sqlPhases.Contract, r.Dialect); err != nil {
			return r.upgradeError(migration, PhaseContract, err)
		}
		if err := r.complete(ctx, conn, migration.Version, current.cursor); err != nil {
			return r.upgradeError(migration, PhaseContract, err)
		}
	}
	r.Logger.Info(
		"metadata schema migration complete",
		"version", migration.Version,
		"name", migration.Name,
	)
	return nil
}

func (r *Runner) runBackfill(
	ctx context.Context,
	conn *sql.Conn,
	migration Migration,
	cursor string,
) (string, error) {
	if migration.Backfill == nil {
		if err := r.setPhase(
			ctx,
			conn,
			migration.Version,
			PhaseContract,
			cursor,
			false,
		); err != nil {
			return cursor, err
		}
		return cursor, nil
	}
	limit := migration.BatchSize
	if limit == 0 {
		limit = DefaultBatchSize
	}
	for {
		if err := ctx.Err(); err != nil {
			return cursor, err
		}
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return cursor, err
		}
		result, err := migration.Backfill(
			ctx,
			Batch{Tx: tx, Cursor: cursor, Limit: limit},
		)
		if err != nil {
			_ = tx.Rollback()
			return cursor, err
		}
		if !result.Done && result.Cursor == cursor {
			_ = tx.Rollback()
			return cursor, errors.New(
				"backfill did not finish or advance its cursor",
			)
		}
		nextPhase := PhaseBackfill
		if result.Done {
			nextPhase = PhaseContract
		}
		if err := r.setPhaseTx(
			ctx,
			tx,
			migration.Version,
			nextPhase,
			result.Cursor,
			!result.Done,
		); err != nil {
			_ = tx.Rollback()
			return cursor, err
		}
		if err := tx.Commit(); err != nil {
			return cursor, err
		}
		cursor = result.Cursor
		r.Logger.Info(
			"metadata migration backfill batch",
			"version", migration.Version,
			"rows", result.Rows,
			"cursor", boundedCursor(cursor),
			"done", result.Done,
		)
		if result.Done {
			return cursor, nil
		}
	}
}

func (r *Runner) validateDatabase(states map[int]state) error {
	target := len(r.Registry)
	versions := make([]bool, target+1)
	for version, migrationState := range states {
		if version > target {
			return fmt.Errorf(
				"%w: database version %d, binary version %d",
				ErrNewerSchema,
				version,
				target,
			)
		}
		if version < 1 {
			return fmt.Errorf(
				"metadata migration state contains invalid version %d",
				version,
			)
		}
		versions[version] = true
		migration := r.Registry[version-1]
		checksum := migration.checksum()
		if migrationState.name != migration.Name ||
			migrationState.checksum != checksum {
			return fmt.Errorf(
				"%w at version %d (%s)",
				ErrChecksumDrift,
				version,
				migration.Name,
			)
		}
		switch migrationState.phase {
		case PhaseExpand, PhaseBackfill, PhaseContract, PhaseComplete:
		default:
			return fmt.Errorf(
				"metadata migration version %d has unknown phase %q",
				version,
				migrationState.phase,
			)
		}
		if migrationState.phase == PhaseComplete &&
			(migrationState.dirty || !migrationState.completedAt.Valid) {
			return fmt.Errorf(
				"metadata migration version %d has inconsistent "+
					"completed state",
				version,
			)
		}
	}
	seenGap := false
	for version := 1; version <= target; version++ {
		if !versions[version] {
			seenGap = true
			continue
		}
		if seenGap {
			return fmt.Errorf(
				"metadata migration state is noncontiguous at version %d",
				version,
			)
		}
	}
	return nil
}

func (r *Runner) userTables(
	ctx context.Context,
	conn *sql.Conn,
) ([]string, error) {
	var query string
	switch r.Dialect {
	case "sqlite":
		query = "SELECT name FROM sqlite_master WHERE type = 'table' " +
			"AND name NOT LIKE 'sqlite_%' AND name <> 'schema_migrations'"
	case "postgres":
		query = "SELECT table_name FROM information_schema.tables " +
			"WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' " +
			"AND table_name <> 'schema_migrations'"
	case "mysql":
		query = "SELECT table_name FROM information_schema.tables " +
			"WHERE table_schema = DATABASE() AND table_name <> 'schema_migrations'"
	default:
		return nil, fmt.Errorf("unsupported metadata dialect %q", r.Dialect)
	}
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("inspect metadata tables: %w", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, rows.Err()
}

func (r *Runner) ensureStateTable(
	ctx context.Context,
	conn *sql.Conn,
) error {
	var statement string
	switch r.Dialect {
	case "sqlite", "postgres":
		statement = `CREATE TABLE IF NOT EXISTS schema_migrations (
			version BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			checksum TEXT NOT NULL,
			phase TEXT NOT NULL,
			cursor TEXT NOT NULL DEFAULT '',
			dirty BOOLEAN NOT NULL,
			started_at BIGINT NOT NULL,
			updated_at BIGINT NOT NULL,
			completed_at BIGINT NULL
		)`
	case "mysql":
		statement = `CREATE TABLE IF NOT EXISTS schema_migrations (
			version BIGINT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			checksum VARCHAR(64) NOT NULL,
			phase VARCHAR(16) NOT NULL,
			` + r.cursorColumn() + ` TEXT NOT NULL,
			dirty BOOLEAN NOT NULL,
			started_at BIGINT NOT NULL,
			updated_at BIGINT NOT NULL,
			completed_at BIGINT NULL
		)`
	default:
		return fmt.Errorf("unsupported metadata dialect %q", r.Dialect)
	}
	if _, err := conn.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	return nil
}

func (r *Runner) readStates(
	ctx context.Context,
	conn *sql.Conn,
) (map[int]state, error) {
	rows, err := conn.QueryContext(
		ctx,
		strings.ReplaceAll(
			"SELECT version, name, checksum, phase, __cursor__, dirty, completed_at "+
				"FROM schema_migrations ORDER BY version",
			"__cursor__",
			r.cursorColumn(),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	defer rows.Close()
	states := make(map[int]state)
	for rows.Next() {
		var migrationState state
		if err := rows.Scan(
			&migrationState.version,
			&migrationState.name,
			&migrationState.checksum,
			&migrationState.phase,
			&migrationState.cursor,
			&migrationState.dirty,
			&migrationState.completedAt,
		); err != nil {
			return nil, err
		}
		states[migrationState.version] = migrationState
	}
	return states, rows.Err()
}

func (r *Runner) insertState(
	ctx context.Context,
	conn *sql.Conn,
	migrationState state,
) error {
	now := r.Now().UnixMilli()
	query := r.rebind(
		"INSERT INTO schema_migrations " +
			"(version, name, checksum, phase, " + r.cursorColumn() + ", dirty, started_at, updated_at) " +
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
	)
	_, err := conn.ExecContext(
		ctx,
		query,
		migrationState.version,
		migrationState.name,
		migrationState.checksum,
		migrationState.phase,
		migrationState.cursor,
		migrationState.dirty,
		now,
		now,
	)
	return err
}

func (r *Runner) setDirty(
	ctx context.Context,
	conn *sql.Conn,
	version int,
	phase Phase,
) error {
	_, err := conn.ExecContext(
		ctx,
		r.rebind(
			"UPDATE schema_migrations SET phase = ?, dirty = ?, updated_at = ? "+
				"WHERE version = ?",
		),
		phase,
		true,
		r.Now().UnixMilli(),
		version,
	)
	return err
}

func (r *Runner) setPhase(
	ctx context.Context,
	conn *sql.Conn,
	version int,
	phase Phase,
	cursor string,
	dirty bool,
) error {
	_, err := conn.ExecContext(
		ctx,
		r.rebind(
			"UPDATE schema_migrations SET phase = ?, "+r.cursorColumn()+" = ?, dirty = ?, "+
				"updated_at = ? WHERE version = ?",
		),
		phase,
		cursor,
		dirty,
		r.Now().UnixMilli(),
		version,
	)
	return err
}

func (r *Runner) setPhaseTx(
	ctx context.Context,
	tx *sql.Tx,
	version int,
	phase Phase,
	cursor string,
	dirty bool,
) error {
	_, err := tx.ExecContext(
		ctx,
		r.rebind(
			"UPDATE schema_migrations SET phase = ?, "+r.cursorColumn()+" = ?, dirty = ?, "+
				"updated_at = ? WHERE version = ?",
		),
		phase,
		cursor,
		dirty,
		r.Now().UnixMilli(),
		version,
	)
	return err
}

func (r *Runner) complete(
	ctx context.Context,
	conn *sql.Conn,
	version int,
	cursor string,
) error {
	now := r.Now().UnixMilli()
	_, err := conn.ExecContext(
		ctx,
		r.rebind(
			"UPDATE schema_migrations SET phase = ?, "+r.cursorColumn()+" = ?, dirty = ?, "+
				"updated_at = ?, completed_at = ? WHERE version = ?",
		),
		PhaseComplete,
		cursor,
		false,
		now,
		now,
		version,
	)
	return err
}

func (r *Runner) cursorColumn() string {
	if r.Dialect == "mysql" {
		return "`cursor`"
	}
	return "cursor"
}

func (r *Runner) rebind(query string) string {
	if r.Dialect != "postgres" {
		return query
	}
	var result strings.Builder
	parameter := 0
	for _, character := range query {
		if character == '?' {
			parameter++
			fmt.Fprintf(&result, "$%d", parameter)
		} else {
			result.WriteRune(character)
		}
	}
	return result.String()
}

func (r *Runner) upgradeError(
	migration Migration,
	phase Phase,
	err error,
) error {
	return &UpgradeError{
		Version: migration.Version,
		Name:    migration.Name,
		Phase:   phase,
		Err:     err,
	}
}

func execDDL(
	ctx context.Context,
	conn *sql.Conn,
	statements []string,
	dialect string,
) error {
	for index, statement := range statements {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			if dialect == "mysql" && isMySQLDDLAlreadyAppliedOnConn(ctx, conn, statement, err) {
				continue
			}
			return fmt.Errorf("statement %d: %w", index+1, err)
		}
	}
	return nil
}

func boundedCursor(cursor string) string {
	const maxCursorLogBytes = 256
	if len(cursor) <= maxCursorLogBytes {
		return cursor
	}
	return cursor[:maxCursorLogBytes] + "…"
}
