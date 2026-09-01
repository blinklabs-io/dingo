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
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"github.com/stretchr/testify/require"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(
		"sqlite",
		"file:"+filepath.Join(t.TempDir(), "metadata.sqlite")+
			"?_pragma=foreign_keys(1)",
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	return db
}

func TestMySQLLockTimeoutSeconds(t *testing.T) {
	t.Parallel()
	require.Equal(t, int64(-1), mysqlLockTimeoutSeconds(0))
	require.Equal(t, int64(-1), mysqlLockTimeoutSeconds(-time.Second))
	require.Equal(t, int64(1), mysqlLockTimeoutSeconds(time.Millisecond))
	require.Equal(t, int64(30), mysqlLockTimeoutSeconds(30*time.Second))
}

func testMigration(backfill Backfill) Migration {
	return Migration{
		Version:          1,
		Name:             "initial_schema",
		BackfillRevision: "1",
		BatchSize:        2,
		SQL: map[string]SQL{
			"sqlite": {
				Expand: []string{
					`CREATE TABLE IF NOT EXISTS item (
						id INTEGER PRIMARY KEY,
						done BOOLEAN NOT NULL DEFAULT FALSE
					)`,
					"INSERT OR IGNORE INTO item (id) VALUES (1), (2), (3), (4)",
				},
				Contract: []string{
					"CREATE INDEX IF NOT EXISTS idx_item_done ON item (done)",
				},
			},
		},
		Backfill: backfill,
	}
}

func itemBackfill(
	beforeBatch func() error,
) Backfill {
	return func(
		ctx context.Context,
		batch Batch,
	) (BatchResult, error) {
		if beforeBatch != nil {
			if err := beforeBatch(); err != nil {
				return BatchResult{}, err
			}
		}
		cursor := 0
		if batch.Cursor != "" {
			parsed, err := strconv.Atoi(batch.Cursor)
			if err != nil {
				return BatchResult{}, err
			}
			cursor = parsed
		}
		rows, err := batch.Tx.QueryContext(
			ctx,
			"SELECT id FROM item WHERE id > ? ORDER BY id LIMIT ?",
			cursor,
			batch.Limit,
		)
		if err != nil {
			return BatchResult{}, err
		}
		var ids []int
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				return BatchResult{}, err
			}
			ids = append(ids, id)
		}
		if err := rows.Close(); err != nil {
			return BatchResult{}, err
		}
		if len(ids) == 0 {
			return BatchResult{
				Cursor: batch.Cursor,
				Done:   true,
			}, nil
		}
		for _, id := range ids {
			if _, err := batch.Tx.ExecContext(
				ctx,
				"UPDATE item SET done = TRUE WHERE id = ?",
				id,
			); err != nil {
				return BatchResult{}, err
			}
		}
		return BatchResult{
			Cursor: strconv.Itoa(ids[len(ids)-1]),
			Rows:   int64(len(ids)),
		}, nil
	}
}

func testRunner(db *sql.DB, migration Migration) *Runner {
	return &Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: []Migration{migration},
		Locker:   NewProcessLocker(),
	}
}

func TestRunnerFreshDatabaseAndIdempotentRerun(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	runner := testRunner(db, testMigration(itemBackfill(nil)))
	require.NoError(t, runner.Run(context.Background()))
	require.NoError(t, runner.Run(context.Background()))

	var done, total int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FILTER (WHERE done), COUNT(*) FROM item",
	).Scan(&done, &total))
	require.Equal(t, 4, done)
	require.Equal(t, 4, total)

	var phase string
	var dirty bool
	var completed sql.NullInt64
	require.NoError(t, db.QueryRow(
		"SELECT phase, dirty, completed_at FROM schema_migrations WHERE version = 1",
	).Scan(&phase, &dirty, &completed))
	require.Equal(t, string(PhaseComplete), phase)
	require.False(t, dirty)
	require.True(t, completed.Valid)
}

func TestRatificationHistoryMigrationBackfillsExistingMarker(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	registry, err := SQLiteRegistry()
	require.NoError(t, err)
	runner := &Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry[:5],
		Locker:   NewProcessLocker(),
	}
	require.NoError(t, runner.Run(context.Background()))
	_, err = db.Exec(`
INSERT INTO governance_proposal (
    tx_hash, action_index, action_type, proposed_epoch, expires_epoch,
    ratified_epoch, ratified_slot, anchor_url, anchor_hash, deposit,
    return_address, added_slot
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		[]byte("pre-history-proposal"),
		0,
		6,
		1,
		100,
		5,
		550,
		"https://example.invalid/governance",
		[]byte("pre-history-anchor"),
		0,
		[]byte("pre-history-return-address"),
		500,
	)
	require.NoError(t, err)

	runner.Registry = registry
	require.NoError(t, runner.Run(context.Background()))
	var transitionSlot, ratifiedEpoch, ratifiedSlot uint64
	require.NoError(t, db.QueryRow(`
SELECT transition_slot, ratified_epoch, ratified_slot
FROM governance_proposal_ratification_history`).Scan(
		&transitionSlot,
		&ratifiedEpoch,
		&ratifiedSlot,
	))
	require.Equal(t, uint64(550), transitionSlot)
	require.Equal(t, uint64(5), ratifiedEpoch)
	require.Equal(t, uint64(550), ratifiedSlot)
	_, err = db.Exec(registry[5].SQL["sqlite"].Expand[3])
	require.NoError(t, err)
	var count int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM governance_proposal_ratification_history",
	).Scan(&count))
	require.Equal(t, 1, count, "migration backfill must be re-runnable")
}

func TestRunnerResumesBackfillCursor(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	calls := 0
	fault := errors.New("injected batch failure")
	failingBackfill := itemBackfill(func() error {
		calls++
		if calls == 2 {
			return fault
		}
		return nil
	})
	runner := testRunner(db, testMigration(failingBackfill))
	err := runner.Run(context.Background())
	require.ErrorIs(t, err, fault)

	var cursor, phase string
	var dirty bool
	require.NoError(t, db.QueryRow(
		"SELECT cursor, phase, dirty FROM schema_migrations WHERE version = 1",
	).Scan(&cursor, &phase, &dirty))
	require.Equal(t, "2", cursor)
	require.Equal(t, string(PhaseBackfill), phase)
	require.True(t, dirty)

	runner.Registry[0].Backfill = itemBackfill(nil)
	require.NoError(t, runner.Run(context.Background()))
	var done int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM item WHERE done = TRUE",
	).Scan(&done))
	require.Equal(t, 4, done)
}

func TestRunnerRejectsChecksumDrift(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	runner := testRunner(db, testMigration(nil))
	require.NoError(t, runner.Run(context.Background()))
	runner.Registry[0].SQL["sqlite"] = SQL{
		Expand: []string{"CREATE TABLE changed (id INTEGER)"},
	}
	require.ErrorIs(
		t,
		runner.Run(context.Background()),
		ErrChecksumDrift,
	)
}

func TestRunnerRejectsUnversionedDatabase(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	_, err := db.Exec("CREATE TABLE mystery (id INTEGER)")
	require.NoError(t, err)
	err = testRunner(db, testMigration(nil)).Run(context.Background())
	require.ErrorIs(t, err, ErrLegacySchema)
	if err != nil {
		require.Contains(t, err.Error(), "delete the data directory")
	}
	var stateTables int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations'",
	).Scan(&stateTables))
	require.Zero(t, stateTables)
}

func TestValidateRegistryRequiresContiguousVersions(t *testing.T) {
	t.Parallel()
	migration := testMigration(nil)
	migration.Version = 2
	require.ErrorIs(
		t,
		validateRegistry([]Migration{migration}, "sqlite"),
		ErrInvalidRegistry,
	)
}

func TestProcessLockerCancellation(t *testing.T) {
	t.Parallel()
	locker := NewProcessLocker()
	release, err := locker.Acquire(context.Background(), nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = locker.Acquire(ctx, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, release())
}

func TestRunnerRejectsNewerSchema(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	runner := testRunner(db, testMigration(nil))
	require.NoError(t, runner.Run(context.Background()))
	_, err := db.Exec(
		`INSERT INTO schema_migrations
			(version, name, checksum, phase, cursor, dirty, started_at, updated_at, completed_at)
		 VALUES (2, 'future', ?, 'complete', '', FALSE, 1, 1, 1)`,
		fmt.Sprintf("%064d", 0),
	)
	require.NoError(t, err)
	require.ErrorIs(t, runner.Run(context.Background()), ErrNewerSchema)
}

// addColumnMigration adds a column to a table an earlier statement in the same
// expand phase creates, the shape versions 2, 5, and 7 all use.
func addColumnMigration() Migration {
	return Migration{
		Version:          1,
		Name:             "add_column",
		BackfillRevision: "1",
		SQL: map[string]SQL{
			"sqlite": {
				Expand: []string{
					"CREATE TABLE IF NOT EXISTS item (id INTEGER PRIMARY KEY)",
					"ALTER TABLE `item` ADD COLUMN `note` text",
					"INSERT INTO item (id, note) VALUES (1, 'seeded') " +
						"ON CONFLICT (id) DO NOTHING",
				},
			},
		},
	}
}

// An expand phase runs each statement in its own autocommit and the phase
// advance is a separate write, so a process that stops in between replays the
// whole phase. An ALTER TABLE ADD COLUMN cannot carry its own IF NOT EXISTS
// guard -- migrations are authored in SQLite syntax, which has no such form --
// so the runner has to recognize the already-added column; otherwise the replay
// fails and the migration never reaches the statements after it.
func TestRunnerReplaysExpandPhaseWithAppliedAddColumn(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	runner := testRunner(db, addColumnMigration())
	require.NoError(t, runner.Run(context.Background()))

	_, err := db.Exec(`
UPDATE schema_migrations
SET phase = 'expand', dirty = 1, completed_at = NULL
WHERE version = 1`)
	require.NoError(t, err)
	_, err = db.Exec("DELETE FROM item")
	require.NoError(t, err)

	require.NoError(t, runner.Run(context.Background()))

	var note string
	require.NoError(t, db.QueryRow(
		"SELECT note FROM item WHERE id = 1",
	).Scan(&note))
	require.Equal(t, "seeded", note)
}

// The replay tolerance is verified against the live schema, so an ALTER TABLE
// ADD COLUMN that fails for any reason other than the column already being
// there still fails the migration.
func TestRunnerReportsAddColumnFailureOnMissingTable(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	migration := Migration{
		Version:          1,
		Name:             "add_column_missing_table",
		BackfillRevision: "1",
		SQL: map[string]SQL{
			"sqlite": {
				Expand: []string{
					"ALTER TABLE `absent` ADD COLUMN `note` text",
				},
			},
		},
	}
	err := testRunner(db, migration).Run(context.Background())
	require.ErrorContains(t, err, "failed in "+string(PhaseExpand))
}

// Every ALTER TABLE ADD COLUMN the shipped registries produce has to be
// recognizable to the replay guard in each dialect's quoting, or an upgrade
// interrupted between the committed DDL and its phase advance would still fail
// on a duplicate column. The registry translates SQLite-authored statements per
// dialect, so this pins the guard against that translation drifting.
func TestAddColumnPatternMatchesShippedMigrations(t *testing.T) {
	t.Parallel()
	// v2 adds four columns, v5 two, and v7 one.
	const shippedAddColumns = 7
	for _, dialect := range []struct {
		name string
		load func() ([]Migration, error)
	}{
		{name: "sqlite", load: SQLiteRegistry},
		{name: "postgres", load: PostgresRegistry},
		{name: "mysql", load: MySQLRegistry},
	} {
		registry, err := dialect.load()
		require.NoError(t, err)
		matched := 0
		for _, migration := range registry {
			phases := migration.SQL[dialect.name]
			for _, statement := range append(
				append([]string{}, phases.Expand...),
				phases.Contract...,
			) {
				if !strings.Contains(
					strings.ToUpper(statement),
					"ADD COLUMN",
				) {
					continue
				}
				match := addColumnPattern.FindStringSubmatch(statement)
				require.Len(
					t,
					match,
					3,
					"%s: unrecognized ADD COLUMN: %s",
					dialect.name,
					statement,
				)
				require.NotEmpty(t, match[1], dialect.name)
				require.NotEmpty(t, match[2], dialect.name)
				matched++
			}
		}
		require.Equal(t, shippedAddColumns, matched, dialect.name)
	}

	// SQLite and MySQL keep backticks and PostgreSQL is requoted to double
	// quotes, but nothing forces a future migration to quote at all, and the
	// resource may wrap the statement across lines.
	for _, statement := range []string{
		"ALTER TABLE pool_registration ADD COLUMN deposit_held text",
		"alter table `x` add column `y` blob",
		`ALTER TABLE "x" ADD COLUMN "y" BYTEA`,
		"ALTER TABLE `x` ADD COLUMN `y` text NOT NULL DEFAULT '0'",
		"ALTER TABLE  `x`   ADD   COLUMN   `y`  text",
		"ALTER TABLE `x`\n  ADD COLUMN `y` text",
	} {
		require.Len(
			t,
			addColumnPattern.FindStringSubmatch(statement),
			3,
			"must recognize: %s",
			statement,
		)
	}

	// The guard must not claim any other failing DDL, or a real failure would
	// be skipped whenever the named column happens to exist.
	for _, statement := range []string{
		"ALTER TABLE `x` ADD CONSTRAINT `c` FOREIGN KEY (`a`) " +
			"REFERENCES `b`(`i`)",
		"ALTER TABLE `x` DROP COLUMN `y`",
		"ALTER TABLE `x` RENAME COLUMN `y` TO `z`",
		"CREATE TABLE `x` (`y` text)",
		"CREATE INDEX `i` ON `x` (`y`)",
		"UPDATE `x` SET `y` = 1",
	} {
		require.Empty(
			t,
			addColumnPattern.FindStringSubmatch(statement),
			"must not recognize: %s",
			statement,
		)
	}
}

// The skip is gated on the statement being an ALTER TABLE ADD COLUMN, so a
// different statement that fails against a table already carrying that column
// still fails the migration.
func TestRunnerDoesNotSkipNonAddColumnFailure(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	migration := Migration{
		Version:          1,
		Name:             "non_add_column_failure",
		BackfillRevision: "1",
		SQL: map[string]SQL{
			"sqlite": {
				Expand: []string{
					"CREATE TABLE IF NOT EXISTS item (id INTEGER PRIMARY KEY)",
					"ALTER TABLE `item` ADD COLUMN `note` text",
					// `note` exists, but this statement is not an ADD COLUMN
					// and fails on its own terms.
					"UPDATE item SET note = no_such_column",
				},
			},
		},
	}
	err := testRunner(db, migration).Run(context.Background())
	require.ErrorContains(t, err, "failed in "+string(PhaseExpand))
	require.ErrorContains(t, err, "statement 3")

	// The version must stay unfinished rather than being marked complete.
	var phase string
	require.NoError(t, db.QueryRow(
		"SELECT phase FROM schema_migrations WHERE version = 1",
	).Scan(&phase))
	require.Equal(t, string(PhaseExpand), phase)
}

// execDDL runs each statement in its own autocommit and the phase advance is a
// separate write, so any version can be replayed from its expand phase after an
// interrupted upgrade. Replaying expand also replays backfill and contract, so
// this covers every phase of every shipped version against a database that
// already has the version's full effect applied.
func TestRunnerReplaysEveryShippedVersionFromExpand(t *testing.T) {
	t.Parallel()
	registry, err := SQLiteRegistry()
	require.NoError(t, err)
	for _, migration := range registry {
		t.Run(migration.Name, func(t *testing.T) {
			t.Parallel()
			db := openTestDB(t)
			runner := &Runner{
				DB:       db,
				Dialect:  "sqlite",
				Registry: registry,
				Locker:   NewProcessLocker(),
			}
			require.NoError(t, runner.Run(context.Background()))

			_, err := db.Exec(`
UPDATE schema_migrations
SET phase = 'expand', dirty = 1, completed_at = NULL
WHERE version = ?`, migration.Version)
			require.NoError(t, err)

			require.NoError(
				t,
				runner.Run(context.Background()),
				"version %d must replay its expand phase",
				migration.Version,
			)

			var phase string
			var dirty bool
			var completed sql.NullInt64
			require.NoError(t, db.QueryRow(`
SELECT phase, dirty, completed_at FROM schema_migrations WHERE version = ?`,
				migration.Version,
			).Scan(&phase, &dirty, &completed))
			require.Equal(t, string(PhaseComplete), phase)
			require.False(t, dirty)
			require.True(t, completed.Valid)
		})
	}
}
