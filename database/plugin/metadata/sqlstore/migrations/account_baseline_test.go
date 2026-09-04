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

package migrations_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	_ "github.com/glebarez/go-sqlite"
	"github.com/stretchr/testify/require"
)

// The v4 backfill has to give an already-bootstrapped Mithril database its
// account baselines, because those accounts have no certificate history a
// replay could rebuild them from. It covers exactly the imported and
// genesis-delegated rows (`created_slot = 0`) and records them as registered,
// which is the state both importers write.
func TestAccountImportBaselineBackfill(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, runTo := baselineBackfillDB(t)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)

	imported := []byte{0x11, 0x22}
	certCreated := []byte{0x33, 0x44}
	// A snapshot-imported row that a rolled-back deregistration already left
	// inactive with its delegation cleared.
	_, err = db.ExecContext(ctx, `
INSERT INTO account (
    staking_key, credential_tag, pool, drep, drep_type, added_slot,
    created_slot, active
) VALUES (?, 0, ?, ?, 1, 200, 0, 0)`,
		imported,
		[]byte{0xaa, 0xaa},
		[]byte{0xbb, 0xbb},
	)
	require.NoError(t, err)
	// A certificate-created row, whose own history rebuilds its state.
	_, err = db.ExecContext(ctx, `
INSERT INTO account (
    staking_key, credential_tag, added_slot, created_slot, active
) VALUES (?, 0, 300, 300, 1)`,
		certCreated,
	)
	require.NoError(t, err)

	runTo(registry[:4])

	var (
		pool, drep []byte
		drepType   int64
		active     bool
		addedSlot  int64
	)
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT pool, drep, drep_type, active, added_slot
FROM account_import_baseline
WHERE credential_tag = 0 AND staking_key = ?`,
		imported,
	).Scan(&pool, &drep, &drepType, &active, &addedSlot))
	require.Equal(t, []byte{0xaa, 0xaa}, pool)
	require.Equal(t, []byte{0xbb, 0xbb}, drep)
	require.Equal(t, int64(1), drepType)
	require.True(t, active)
	require.Equal(t, int64(200), addedSlot)

	require.Equal(t, 1, baselineRowCount(t, db))
	replayBaselineExpand(t, db)
	require.Equal(t, 1, baselineRowCount(t, db))
}

// baselineBackfillDB brings a database up to the schema that predates the
// baseline table and returns it with a runner for the remaining versions, so a
// test can seed the legacy rows the backfill reads.
func baselineBackfillDB(
	t *testing.T,
) (*sql.DB, func(versions []migrations.Migration)) {
	t.Helper()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(registry), 4)
	runTo := func(versions []migrations.Migration) {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: versions,
			Locker: migrations.NewFileLocker(
				databasePath + ".migrate.lock",
			),
		}
		require.NoError(t, runner.Run(ctx))
	}
	runTo(registry[:3])
	return db, runTo
}

// replayBaselineExpand re-executes the v4 expand statements the way an upgrade
// interrupted after the backfill committed but before its phase row advanced
// would.
func replayBaselineExpand(t *testing.T, db *sql.DB) {
	t.Helper()
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	for _, statement := range registry[3].SQL["sqlite"].Expand {
		_, err := db.ExecContext(context.Background(), statement)
		require.NoError(t, err)
	}
}

func baselineRowCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var rows int
	require.NoError(t, db.QueryRowContext(context.Background(), `
SELECT COUNT(*) FROM account_import_baseline`).Scan(&rows))
	return rows
}

func TestAccountImportDepositMigrationKeepsLegacyBaselineUnknown(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, runTo := baselineBackfillDB(t)
	key := []byte{0x44, 0x55}
	_, err := db.ExecContext(ctx, `
INSERT INTO account (
    staking_key, credential_tag, added_slot, created_slot, active
) VALUES (?, 0, 100, 0, 1)`, key)
	require.NoError(t, err)

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runTo(registry[:5])
	require.Equal(t, 1, baselineRowCount(t, db))
	// The deposit migration is v7; main's governance history migration took v6.
	runTo(registry[:7])

	var deposit sql.NullString
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT deposit_amount
FROM account_import_baseline
WHERE credential_tag = 0 AND staking_key = ?`, key).Scan(&deposit))
	require.False(t, deposit.Valid)
}

// A legacy account row with a NULL staking key is skipped rather than
// backfilled. Its baseline could never be read back -- credential equality
// matches no NULL -- and inserting it would break re-runnability, because the
// LEFT JOIN that suppresses an already-backfilled row cannot match NULL
// either, so every interrupted-upgrade retry would add another row.
func TestAccountImportBaselineBackfillSkipsNullStakingKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, runTo := baselineBackfillDB(t)

	_, err := db.ExecContext(ctx, `
INSERT INTO account (
    staking_key, credential_tag, added_slot, created_slot, active
) VALUES (NULL, 0, 200, 0, 1)`)
	require.NoError(t, err)

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runTo(registry[:4])

	require.Equal(t, 0, baselineRowCount(t, db))
	replayBaselineExpand(t, db)
	require.Equal(t, 0, baselineRowCount(t, db))
}

// An imported account that already carried certificate history when the
// baseline table arrived gets no baseline. Its live pool, DRep, and added_slot
// describe that certificate rather than the import, so recording them would
// claim a provenance the row does not have: a rollback to before the
// certificate would restore its delegation, and a baseline slot bumped past an
// earlier deregistration would outrank it and mark a deregistered credential
// active. Leaving the row alone keeps the derivation from its real certificate
// history.
func TestAccountImportBaselineBackfillSkipsCertificateHistory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, runTo := baselineBackfillDB(t)

	delegated := []byte{0x55, 0x66}
	untouched := []byte{0x77, 0x88}
	for _, key := range [][]byte{delegated, untouched} {
		_, err := db.ExecContext(ctx, `
INSERT INTO account (
    staking_key, credential_tag, pool, added_slot, created_slot, active
) VALUES (?, 0, ?, 400, 0, 1)`,
			key,
			[]byte{0xbb, 0xbb},
		)
		require.NoError(t, err)
	}
	// The delegation that moved the account off its imported pool, as block
	// application recorded it.
	_, err := db.ExecContext(ctx, `
INSERT INTO stake_delegation (
    staking_key, credential_tag, pool_key_hash, added_slot
) VALUES (?, 0, ?, 400)`,
		delegated,
		[]byte{0xbb, 0xbb},
	)
	require.NoError(t, err)

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runTo(registry[:4])

	require.Equal(t, 1, baselineRowCount(t, db))
	var key []byte
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT staking_key FROM account_import_baseline`).Scan(&key))
	require.Equal(t, untouched, key)

	replayBaselineExpand(t, db)
	require.Equal(t, 1, baselineRowCount(t, db))
}

// Every account certificate table the restore path reads has to suppress the
// backfill, not just the delegation table: any of them proves the live row's
// state came from a certificate rather than from the import.
func TestAccountImportBaselineBackfillSkipsEveryCertificateTable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tables := []string{
		"stake_registration",
		"stake_registration_delegation",
		"stake_vote_registration_delegation",
		"vote_registration_delegation",
		"registration",
		"stake_deregistration",
		"deregistration",
		"stake_delegation",
		"stake_vote_delegation",
		"vote_delegation",
	}
	for _, table := range tables {
		t.Run(table, func(t *testing.T) {
			t.Parallel()
			db, runTo := baselineBackfillDB(t)
			key := []byte{0x99, 0xaa}
			_, err := db.ExecContext(ctx, `
INSERT INTO account (
    staking_key, credential_tag, added_slot, created_slot, active
) VALUES (?, 0, 400, 0, 1)`,
				key,
			)
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, `
INSERT INTO `+table+` (staking_key, credential_tag, added_slot)
VALUES (?, 0, 400)`,
				key,
			)
			require.NoError(t, err)

			registry, err := migrations.SQLiteRegistry()
			require.NoError(t, err)
			runTo(registry[:4])

			require.Equal(t, 0, baselineRowCount(t, db))
		})
	}
}
