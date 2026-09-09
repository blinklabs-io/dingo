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
	"regexp"
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
	// Rebind converts ? placeholders to the dialect's own form for data-driven
	// backfills. Leave nil to use the runner's own Dialect-derived rebinder;
	// an identity default would feed ? straight to a dialect that rejects it.
	Rebind func(string) string
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

	hasUserTables, err := r.hasUserTables(ctx, conn)
	if err != nil {
		return err
	}
	stateTableExists, err := r.stateTableExists(ctx, conn)
	if err != nil {
		return err
	}
	if hasUserTables && !stateTableExists {
		return fmt.Errorf(
			"%w: existing metadata tables are from an unsupported database version; delete the data directory (metadata and blob stores) and resync from genesis",
			ErrLegacySchema,
		)
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

	target := r.Registry[len(r.Registry)-1].Version
	current := 0
	for _, migrationState := range states {
		if migrationState.phase == PhaseComplete &&
			migrationState.version > current {
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
		rebind := r.backfillRebind()
		result, err := migration.Backfill(
			ctx,
			Batch{
				Tx:     tx,
				Cursor: cursor,
				Limit:  limit,
				Rebind: rebind,
			},
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
		if err := r.setPhase(
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

func (r *Runner) hasUserTables(
	ctx context.Context,
	conn *sql.Conn,
) (bool, error) {
	var query string
	switch r.Dialect {
	case "sqlite":
		query = "SELECT 1 FROM sqlite_master WHERE type = 'table' " +
			"AND name NOT LIKE 'sqlite_%' AND name <> 'schema_migrations' LIMIT 1"
	case "postgres":
		query = "SELECT 1 FROM information_schema.tables " +
			"WHERE table_schema = current_schema() AND table_type = 'BASE TABLE' " +
			"AND table_name <> 'schema_migrations' LIMIT 1"
	case "mysql":
		query = "SELECT 1 FROM information_schema.tables " +
			"WHERE table_schema = DATABASE() AND table_name <> 'schema_migrations' LIMIT 1"
	default:
		return false, fmt.Errorf("unsupported metadata dialect %q", r.Dialect)
	}
	var found int
	err := conn.QueryRowContext(ctx, query).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect metadata tables: %w", err)
	}
	return found != 0, nil
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

func (r *Runner) stateTableExists(
	ctx context.Context,
	conn *sql.Conn,
) (bool, error) {
	var query string
	switch r.Dialect {
	case "sqlite":
		query = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'"
	case "postgres":
		query = "SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = 'schema_migrations'"
	case "mysql":
		query = "SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'schema_migrations'"
	default:
		return false, fmt.Errorf("unsupported metadata dialect %q", r.Dialect)
	}
	var found int
	err := conn.QueryRowContext(ctx, query).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspect schema_migrations: %w", err)
	}
	return found != 0, nil
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

type stateExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (r *Runner) setPhase(
	ctx context.Context,
	exec stateExecer,
	version int,
	phase Phase,
	cursor string,
	dirty bool,
) error {
	_, err := exec.ExecContext(
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

// backfillRebind returns the placeholder rewriter a data-driven backfill must
// use. An explicit Rebind wins; otherwise the runner's own Dialect-derived
// rebinder applies, because an identity default would feed ? placeholders to a
// dialect that rejects them and fail the backfill.
func (r *Runner) backfillRebind() func(string) string {
	if r.Rebind != nil {
		return r.Rebind
	}
	return r.rebind
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
			if dialect == "mysql" &&
				isMySQLDDLAlreadyAppliedOnConn(ctx, conn, statement, err) {
				continue
			}
			if dialect == "sqlite" &&
				isSQLiteDDLAlreadyAppliedOnConn(ctx, conn, statement, err) {
				continue
			}
			if dialect == "postgres" &&
				isPostgresDDLAlreadyAppliedOnConn(ctx, conn, statement, err) {
				continue
			}
			return fmt.Errorf("statement %d: %w", index+1, err)
		}
	}
	return nil
}

// parseAddColumnStatement extracts the table and column named by an
// ALTER TABLE <table> ADD COLUMN <column> ... statement. Identifier quoting
// differs per dialect, so every supported quote character is trimmed.
func parseAddColumnStatement(statement string) (string, string, string, bool) {
	fields := strings.Fields(strings.TrimSuffix(strings.TrimSpace(statement), ";"))
	if len(fields) < 6 ||
		!strings.EqualFold(fields[0], "ALTER") ||
		!strings.EqualFold(fields[1], "TABLE") ||
		!strings.EqualFold(fields[3], "ADD") ||
		!strings.EqualFold(fields[4], "COLUMN") {
		return "", "", "", false
	}
	table := strings.Trim(fields[2], "`\"")
	column := strings.Trim(fields[5], "`\"")
	if table == "" || column == "" {
		return "", "", "", false
	}
	return table, column, strings.TrimSpace(strings.Join(fields[6:], " ")), true
}

var columnConstraintKeywords = map[string]struct{}{
	"as": {}, "auto_increment": {}, "check": {}, "collate": {},
	"comment": {}, "constraint": {}, "default": {}, "generated": {},
	"not": {}, "null": {}, "primary": {}, "references": {}, "unique": {},
}

var columnTypeAliases = map[string]string{
	"character varying":           "varchar",
	"timestamp with time zone":    "timestamptz",
	"timestamp without time zone": "timestamp",
}

var columnTypeArgsPattern = regexp.MustCompile(`\s*\([^)]*\)`)

func declaredColumnType(definition string) string {
	fields := strings.Fields(definition)
	end := len(fields)
	for index, field := range fields {
		name, _, _ := strings.Cut(field, "(")
		if _, stop := columnConstraintKeywords[strings.ToLower(name)]; stop {
			end = index
			break
		}
	}
	return strings.Join(fields[:end], " ")
}

func normalizeColumnType(value string) string {
	normalized := columnTypeArgsPattern.ReplaceAllString(strings.ToLower(value), "")
	normalized = strings.Join(strings.Fields(normalized), " ")
	if alias, ok := columnTypeAliases[normalized]; ok {
		return alias
	}
	return normalized
}

func addColumnTypeMatches(reported sql.NullString, definition string) bool {
	return reported.Valid && normalizeColumnType(reported.String) ==
		normalizeColumnType(declaredColumnType(definition))
}

func mysqlColumnTypeMatches(reported sql.NullString, definition string) bool {
	if !reported.Valid {
		return false
	}
	actual := normalizeColumnType(reported.String)
	declared := normalizeColumnType(declaredColumnType(definition))
	if declared == "boolean" {
		declared = "tinyint"
	}
	return actual == declared
}

// isPostgresDDLAlreadyAppliedOnConn reports whether an ADD COLUMN statement
// failed only because a previous run of the same expand phase already added
// the column.
//
// The runner records PhaseExpand before running the DDL and advances to
// PhaseBackfill after, and each statement commits in autocommit, so a process
// that dies between the two replays the whole expand phase on the next start.
// PostgreSQL has no ADD COLUMN IF NOT EXISTS that can be used here: the
// migration checksum covers the dialect-translated statements, so rewriting
// released SQL would trip checksum drift on every existing database.
//
// The column is confirmed present before the error is treated as benign, so an
// unrelated "already exists" failure is never silently swallowed. Matching is
// on message text rather than a pgconn error type because the postgres driver
// is only linked under the dingo_extra_plugins build tag.
func isPostgresDDLAlreadyAppliedOnConn(
	ctx context.Context,
	conn *sql.Conn,
	statement string,
	err error,
) bool {
	if !strings.Contains(strings.ToLower(err.Error()), "already exists") {
		return false
	}
	if conn == nil {
		// Without a connection the column cannot be confirmed; never turn an
		// unrelated duplicate-definition error into a no-op.
		return false
	}
	table, column, definition, ok := parseAddColumnStatement(statement)
	if !ok {
		return false
	}
	var reported sql.NullString
	if queryErr := conn.QueryRowContext(
		ctx,
		`SELECT data_type FROM information_schema.columns
WHERE table_name = $1 AND column_name = $2`,
		table,
		column,
	).Scan(&reported); queryErr != nil {
		return false
	}
	return addColumnTypeMatches(reported, definition)
}

func isSQLiteDDLAlreadyAppliedOnConn(
	ctx context.Context,
	conn *sql.Conn,
	statement string,
	err error,
) bool {
	if !strings.Contains(strings.ToLower(err.Error()), "duplicate column name") {
		return false
	}
	table, column, definition, ok := parseAddColumnStatement(statement)
	if !ok {
		return false
	}
	var reported sql.NullString
	if queryErr := conn.QueryRowContext(
		ctx,
		"SELECT type FROM pragma_table_info(?) WHERE name = ?",
		table,
		column,
	).Scan(&reported); queryErr != nil {
		return false
	}
	return addColumnTypeMatches(reported, definition)
}

func boundedCursor(cursor string) string {
	const maxCursorLogBytes = 256
	if len(cursor) <= maxCursorLogBytes {
		return cursor
	}
	return cursor[:maxCursorLogBytes] + "…"
}
