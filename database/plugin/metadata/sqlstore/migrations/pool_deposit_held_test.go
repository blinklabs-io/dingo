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

// depositHeldBackfillDB returns a database migrated to the version before the
// pool deposit-held column exists, so a test can seed the legacy registration
// rows the v12 backfill reads.
func depositHeldBackfillDB(
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
	require.Len(t, registry, 12)
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
	runTo(registry[:10])
	return db, runTo
}

func seedLegacyPoolRegistration(
	t *testing.T,
	db *sql.DB,
	keyHash []byte,
	slot uint64,
	deposit any,
) {
	t.Helper()
	ctx := context.Background()
	result, err := db.ExecContext(ctx, `
INSERT INTO pool (pool_key_hash, latest_op_cert_sequence, pledge, cost,
    reward_account_credential_tag)
VALUES (?, 0, '0', '0', 0)`,
		keyHash,
	)
	require.NoError(t, err)
	poolID, err := result.LastInsertId()
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `
INSERT INTO pool_registration (
    pool_id, pool_key_hash, added_slot, deposit_amount
) VALUES (?, ?, ?, ?)`,
		poolID, keyHash, slot, deposit,
	)
	require.NoError(t, err)
}

func depositHeldValue(t *testing.T, db *sql.DB, keyHash []byte) sql.NullString {
	t.Helper()
	var held sql.NullString
	require.NoError(t, db.QueryRowContext(context.Background(), `
SELECT deposit_held FROM pool_registration WHERE pool_key_hash = ?`,
		keyHash,
	).Scan(&held))
	return held
}

// A registration written before the deposit-held column existed is credited
// with its own recorded deposit. That is exactly the value the pre-change
// refund path read from the latest registration, so the migration reproduces
// the refund the node would already have applied.
func TestDepositHeldBackfillCreditsRecordedDeposit(t *testing.T) {
	t.Parallel()
	db, runTo := depositHeldBackfillDB(t)
	keyHash := []byte("legacy-pool-key-hash-0000001")
	seedLegacyPoolRegistration(t, db, keyHash, 100, "500000000")

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runTo(registry)

	held := depositHeldValue(t, db, keyHash)
	require.True(t, held.Valid)
	require.Equal(t, "500000000", held.String)
}

// A legacy registration with no recorded deposit -- what the genesis and
// Mithril-import paths write -- is credited with zero rather than left NULL, so
// the refund reads a definite amount.
func TestDepositHeldBackfillCreditsZeroForNullDeposit(t *testing.T) {
	t.Parallel()
	db, runTo := depositHeldBackfillDB(t)
	keyHash := []byte("legacy-pool-key-hash-0000002")
	seedLegacyPoolRegistration(t, db, keyHash, 100, nil)

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runTo(registry)

	held := depositHeldValue(t, db, keyHash)
	require.True(t, held.Valid)
	require.Equal(t, "0", held.String)
}

// A re-registration does not pay a second deposit. The backfill must carry
// forward the first registration's retained amount when the protocol deposit
// changed before the later registration.
func TestDepositHeldBackfillCarriesInitialDepositAcrossReregistration(t *testing.T) {
	t.Parallel()
	db, runTo := depositHeldBackfillDB(t)
	keyHash := []byte("legacy-pool-key-hash-0000005")
	seedLegacyPoolRegistration(t, db, keyHash, 100, "500000000")
	var poolID int64
	require.NoError(t, db.QueryRowContext(context.Background(),
		"SELECT id FROM pool WHERE pool_key_hash = ?", keyHash).Scan(&poolID))
	_, err := db.ExecContext(context.Background(), `
INSERT INTO pool_registration (pool_id, pool_key_hash, added_slot, deposit_amount)
VALUES (?, ?, ?, ?)`, poolID, keyHash, 200, "800000000")
	require.NoError(t, err)

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runTo(registry)

	var held string
	require.NoError(t, db.QueryRowContext(context.Background(), `
SELECT deposit_held FROM pool_registration
WHERE pool_key_hash = ? ORDER BY added_slot DESC LIMIT 1`, keyHash).Scan(&held))
	require.Equal(t, "500000000", held)
}

// The backfill statement is re-runnable: an upgrade interrupted after the
// backfill committed but before its phase row advanced replays it, and it must
// not overwrite a held amount that carry-forward has since written.
func TestDepositHeldBackfillReplayKeepsCarriedForwardAmount(t *testing.T) {
	t.Parallel()
	db, runTo := depositHeldBackfillDB(t)
	keyHash := []byte("legacy-pool-key-hash-0000003")
	seedLegacyPoolRegistration(t, db, keyHash, 100, "900000000")

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runTo(registry)

	// Stand in for a later re-registration whose held amount was carried
	// forward from an earlier, cheaper registration.
	_, err = db.ExecContext(context.Background(), `
UPDATE pool_registration SET deposit_held = '500000000'
WHERE pool_key_hash = ?`,
		keyHash,
	)
	require.NoError(t, err)

	_, err = db.ExecContext(context.Background(), `
UPDATE schema_migrations
SET phase = 'backfill', cursor = '', dirty = 1, completed_at = NULL
WHERE version = 12`)
	require.NoError(t, err)
	runTo(registry)

	held := depositHeldValue(t, db, keyHash)
	require.True(t, held.Valid)
	require.Equal(t, "500000000", held.String)
}

// An upgrade interrupted after the expand phase's ALTER TABLE committed but
// before the phase row advanced replays the whole expand phase on the next
// start. SQLite has no ADD COLUMN IF NOT EXISTS, so without the runner
// tolerating an already-present column that replay would fail with a duplicate
// column error and the migration would never reach its backfill.
func TestDepositHeldExpandPhaseReplaysAfterInterruptedUpgrade(t *testing.T) {
	t.Parallel()
	db, runTo := depositHeldBackfillDB(t)
	keyHash := []byte("legacy-pool-key-hash-0000004")
	seedLegacyPoolRegistration(t, db, keyHash, 100, "700000000")

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runTo(registry)

	ctx := context.Background()
	// Rewind version 12 to the durable state such an interruption leaves: the
	// column exists because its ALTER committed, the phase row still says
	// expand, and the backfill has not run.
	_, err = db.ExecContext(ctx, `
UPDATE schema_migrations
SET phase = 'expand', cursor = '', dirty = 1, completed_at = NULL
	WHERE version = 12`)
	require.NoError(t, err)
	_, err = db.ExecContext(
		ctx,
		"UPDATE pool_registration SET deposit_held = NULL",
	)
	require.NoError(t, err)

	runTo(registry)

	held := depositHeldValue(t, db, keyHash)
	require.True(t, held.Valid)
	require.Equal(
		t,
		"700000000",
		held.String,
		"the replayed expand phase must still run its backfill",
	)

	var phase string
	var dirty bool
	var completed sql.NullInt64
	require.NoError(t, db.QueryRowContext(ctx, `
	SELECT phase, dirty, completed_at FROM schema_migrations WHERE version = 12`,
	).Scan(&phase, &dirty, &completed))
	require.Equal(t, "complete", phase)
	require.False(t, dirty)
	require.True(t, completed.Valid)
}

// A re-registration after a retirement needs the epoch history to determine
// whether the retirement was already reaped. The migration must not infer a
// new held amount from the later protocol parameters when that history is
// missing; fail closed so the database can be resynced from chain data.
func TestDepositHeldBackfillFailsClosedWithoutEpochHistory(t *testing.T) {
	t.Parallel()
	db, _ := depositHeldBackfillDB(t)
	keyHash := []byte("legacy-pool-key-hash-0000006")
	seedLegacyPoolRegistration(t, db, keyHash, 100, "500000000")
	var poolID int64
	require.NoError(t, db.QueryRowContext(context.Background(),
		"SELECT id FROM pool WHERE pool_key_hash = ?", keyHash).Scan(&poolID))
	_, err := db.ExecContext(context.Background(), `
INSERT INTO pool_retirement (pool_id, pool_key_hash, certificate_id, epoch, added_slot)
VALUES (?, ?, 0, ?, ?)`, poolID, keyHash, 1, 200)
	require.NoError(t, err)
	_, err = db.ExecContext(context.Background(), `
INSERT INTO pool_registration (pool_id, pool_key_hash, added_slot, deposit_amount)
VALUES (?, ?, ?, ?)`, poolID, keyHash, 300, "800000000")
	require.NoError(t, err)

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   migrations.NewProcessLocker(),
	}
	err = runner.Run(context.Background())
	require.ErrorContains(t, err, "resync required")
}
