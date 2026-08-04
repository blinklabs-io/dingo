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
