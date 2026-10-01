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

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
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
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
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

func alonzoPParamsUnitBackfillDB(
	t *testing.T,
) (*sql.DB, func([]migrations.Migration)) {
	t.Helper()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 32)
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
	runTo(registry[:19])
	return db, runTo
}

func alonzoPParamsUnitMarker(t *testing.T, db *sql.DB) string {
	t.Helper()
	var got string
	require.NoError(t, db.QueryRowContext(context.Background(), `
SELECT value FROM node_settings_gate WHERE name = ?`,
		nodesettings.AlonzoPParamsUnitGateName,
	).Scan(&got))
	return got
}

func seedPParamsEra(t *testing.T, db *sql.DB, eraID uint) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
INSERT INTO pparams (id, cbor, added_slot, epoch, era_id)
VALUES (?, X'80', 0, 0, ?)`, eraID, eraID)
	require.NoError(t, err)
}

func TestAlonzoPParamsUnitBackfillMarksFreshDatabaseWordV1(t *testing.T) {
	t.Parallel()
	db, runTo := alonzoPParamsUnitBackfillDB(t)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)

	runTo(registry)

	require.Equal(t, nodesettings.AlonzoPParamsUnitWordV1,
		alonzoPParamsUnitMarker(t, db))
}

func TestAlonzoPParamsUnitBackfillMarksLegacyAlonzoRows(t *testing.T) {
	t.Parallel()
	db, runTo := alonzoPParamsUnitBackfillDB(t)
	// Seeded through gouroboros' own era id, so a renumbering upstream
	// diverges from the era_id the backfill hardcodes and fails here rather
	// than silently leaving Alonzo rows unclassified.
	seedPParamsEra(t, db, alonzo.EraIdAlonzo)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)

	runTo(registry)

	require.Equal(t, nodesettings.AlonzoPParamsUnitLegacyByteV0,
		alonzoPParamsUnitMarker(t, db))
}

func TestAlonzoPParamsUnitBackfillDoesNotMisclassifyBabbageRows(t *testing.T) {
	t.Parallel()
	db, runTo := alonzoPParamsUnitBackfillDB(t)
	seedPParamsEra(t, db, 5)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)

	runTo(registry)

	require.Equal(t, nodesettings.AlonzoPParamsUnitWordV1,
		alonzoPParamsUnitMarker(t, db))
}

func TestAlonzoPParamsUnitBackfillPreservesExistingMarker(t *testing.T) {
	t.Parallel()
	db, runTo := alonzoPParamsUnitBackfillDB(t)
	_, err := db.ExecContext(context.Background(), `
INSERT INTO node_settings_gate (name, value, recorded_epoch, recorded_slot)
VALUES (?, ?, 7, 9)`,
		nodesettings.AlonzoPParamsUnitGateName,
		nodesettings.AlonzoPParamsUnitWordV1,
	)
	require.NoError(t, err)
	seedPParamsEra(t, db, 4)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)

	runTo(registry)

	require.Equal(t, nodesettings.AlonzoPParamsUnitWordV1,
		alonzoPParamsUnitMarker(t, db))
}

func TestAlonzoPParamsUnitBackfillReplayIsIdempotent(t *testing.T) {
	t.Parallel()
	db, runTo := alonzoPParamsUnitBackfillDB(t)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runTo(registry)
	_, err = db.ExecContext(context.Background(), `
UPDATE schema_migrations
SET phase = 'backfill', cursor = '', dirty = 1, completed_at = NULL
WHERE version = 20`)
	require.NoError(t, err)

	runTo(registry)

	require.Equal(t, nodesettings.AlonzoPParamsUnitWordV1,
		alonzoPParamsUnitMarker(t, db))
}

// depositHeldBackfillDB returns a database migrated to the version before the
// pool deposit-held column exists, so a test can seed the legacy registration
// rows the v12 backfill reads.
func depositHeldBackfillDB(
	t *testing.T,
) (*sql.DB, func(versions []migrations.Migration)) {
	t.Helper()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 32)
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
// Mithril-import paths write -- remains unknown rather than being rewritten as
// an authoritative zero.
func TestDepositHeldBackfillLeavesUnknownDepositUnpopulated(t *testing.T) {
	t.Parallel()
	db, runTo := depositHeldBackfillDB(t)
	keyHash := []byte("legacy-pool-key-hash-0000002")
	seedLegacyPoolRegistration(t, db, keyHash, 100, nil)

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runTo(registry)

	held := depositHeldValue(t, db, keyHash)
	require.False(t, held.Valid)
}

// A re-registration does not pay a second deposit. The backfill must carry
// forward the first registration's retained amount when the protocol deposit
// changed before the later registration.
func TestDepositHeldBackfillCarriesInitialDepositAcrossReregistration(
	t *testing.T,
) {
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

func TestDepositHeldBackfillLeavesUnknownReregistrationUnpopulated(
	t *testing.T,
) {
	t.Parallel()
	db, runTo := depositHeldBackfillDB(t)
	keyHash := []byte("legacy-pool-key-hash-0000006")
	seedLegacyPoolRegistration(t, db, keyHash, 100, nil)
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

	var held, amount sql.NullString
	require.NoError(t, db.QueryRowContext(context.Background(), `
SELECT deposit_held, deposit_amount FROM pool_registration
WHERE pool_key_hash = ? ORDER BY added_slot DESC LIMIT 1`, keyHash).Scan(&held, &amount))
	require.False(t, held.Valid)
	require.True(t, amount.Valid)
	require.Equal(t, "800000000", amount.String)
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
