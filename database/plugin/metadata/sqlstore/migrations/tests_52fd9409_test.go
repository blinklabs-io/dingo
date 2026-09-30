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
	"errors"
	"fmt"
	"path/filepath"
	"sort"
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
	require.Len(t, registry, 31)
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

// TestAssetAmountFingerprintIndexDropRemovesIndexes proves migration v21
// (asset-amount-fingerprint-index-drop, dingo#4598) drops idx_asset_amount
// and idx_asset_fingerprint from a database migrated all the way through,
// while leaving the asset.amount and asset.fingerprint columns themselves
// untouched -- unlike dingo#4482's asset.name_hex, both columns are still
// genuinely read and returned via the blockfrost/mesh API adapters. Before
// this migration existed, a fully migrated (through v20) database still
// carried both indexes: a real WAL-frame-churn measurement during genesis
// sync found neither backs any WHERE/JOIN/ORDER BY predicate anywhere in the
// tree (dingo#4464).
func TestAssetAmountFingerprintIndexDropRemovesIndexes(t *testing.T) {
	t.Parallel()

	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   migrations.NewFileLocker(databasePath + ".migrate.lock"),
	}
	require.NoError(t, runner.Run(context.Background()))

	wantColumns := map[string]bool{"amount": false, "fingerprint": false}
	rows, err := db.Query(`PRAGMA table_info("asset")`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt any
		require.NoError(
			t,
			rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk),
		)
		if _, ok := wantColumns[name]; ok {
			wantColumns[name] = true
		}
	}
	require.NoError(t, rows.Err())
	require.True(
		t,
		wantColumns["amount"],
		"asset.amount must survive migration v21 -- only its index is dropped",
	)
	require.True(
		t,
		wantColumns["fingerprint"],
		"asset.fingerprint must survive migration v21 -- only its index is dropped",
	)

	idxRows, err := db.Query(`PRAGMA index_list("asset")`)
	require.NoError(t, err)
	defer idxRows.Close()
	for idxRows.Next() {
		var seq int
		var name, origin string
		var unique, partial int
		require.NoError(
			t,
			idxRows.Scan(&seq, &name, &unique, &origin, &partial),
		)
		require.NotEqual(
			t,
			"idx_asset_amount",
			name,
			"idx_asset_amount must not survive migration v21",
		)
		require.NotEqual(
			t,
			"idx_asset_fingerprint",
			name,
			"idx_asset_fingerprint must not survive migration v21",
		)
	}
	require.NoError(t, idxRows.Err())
}

// TestAssetNameHexColumnDropRemovesColumnAndIndex proves migration v19
// (asset-name-hex-column-drop, dingo#4464) drops both asset.name_hex and
// idx_asset_name_hex from a database migrated all the way through. Before
// this migration existed, a fully migrated (through v18) database still
// carried both: name_hex was a write-only column nothing filtered on.
func TestAssetNameHexColumnDropRemovesColumnAndIndex(t *testing.T) {
	t.Parallel()

	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   migrations.NewFileLocker(databasePath + ".migrate.lock"),
	}
	require.NoError(t, runner.Run(context.Background()))

	rows, err := db.Query(`PRAGMA table_info("asset")`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt any
		require.NoError(
			t,
			rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk),
		)
		require.NotEqual(
			t,
			"name_hex",
			name,
			"asset.name_hex must not survive migration v19",
		)
	}
	require.NoError(t, rows.Err())

	idxRows, err := db.Query(`PRAGMA index_list("asset")`)
	require.NoError(t, err)
	defer idxRows.Close()
	for idxRows.Next() {
		var seq int
		var name, origin string
		var unique, partial int
		require.NoError(
			t,
			idxRows.Scan(&seq, &name, &unique, &origin, &partial),
		)
		require.NotEqual(
			t,
			"idx_asset_name_hex",
			name,
			"idx_asset_name_hex must not survive migration v19",
		)
	}
	require.NoError(t, idxRows.Err())
}

func TestCollateralInputMigrationBackfillsLegacyMarker(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+dbPath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	run := func(registry []migrations.Migration) {
		runner := migrations.Runner{DB: db, Dialect: "sqlite", Registry: registry, Locker: migrations.NewProcessLocker()}
		require.NoError(t, runner.Run(context.Background()))
	}
	// Build a real v13 database, as an older installation would have existed.
	run(registry[:13])
	_, err = db.Exec(`INSERT INTO utxo (tx_id, output_idx, credential_tag, amount,
collateral_by_tx_id) VALUES (X'01', 0, 0, '1', X'02')`)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	db, err = sql.Open("sqlite", "file:"+dbPath+"?"+testDBPragmas)
	// Reopen the same file and run the real pending migration.
	require.NoError(t, err)
	run(registry[:14])
	require.NoError(t, db.Close())
	db, err = sql.Open("sqlite", "file:"+dbPath+"?"+testDBPragmas)
	require.NoError(t, err)
	// Force v14 back to pending while retaining its schema/data so the runner
	// re-executes the backfill on restart.
	_, err = db.Exec("DELETE FROM schema_migrations WHERE version = 14")
	require.NoError(t, err)
	run(registry[:14])
	var count int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM utxo_collateral_input WHERE transaction_hash = X'02'",
	).Scan(&count))
	require.Equal(t, 1, count)
}

func TestCommitteeCredentialMigrationPreservesExistingRows(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 31)
	runTo := func(versions []migrations.Migration) {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: versions,
			Locker: migrations.NewFileLocker(
				databasePath + ".migrate.lock",
			),
		}
		require.NoError(t, runner.Run(context.Background()))
	}

	runTo(registry[:6])
	cold := []byte{0x11, 0x22}
	hot := []byte{0x33, 0x44}
	_, err = db.Exec(
		"INSERT INTO committee_member "+
			"(cold_cred_hash, expires_epoch, added_slot) VALUES (?, ?, ?)",
		cold,
		41,
		17,
	)
	require.NoError(t, err)
	_, err = db.Exec(
		"INSERT INTO auth_committee_hot "+
			"(cold_credential, host_credential, added_slot) VALUES (?, ?, ?)",
		cold,
		hot,
		18,
	)
	require.NoError(t, err)
	_, err = db.Exec(
		"INSERT INTO resign_committee_cold "+
			"(cold_credential, added_slot) VALUES (?, ?)",
		cold,
		19,
	)
	require.NoError(t, err)

	runTo(registry[:8])
	explicitZeroCold := []byte{0x55, 0x66}
	_, err = db.Exec(
		"INSERT INTO committee_member "+
			"(cold_credential_tag, cold_cred_hash, expires_epoch, "+
			"term_start_slot, added_slot) VALUES (?, ?, ?, ?, ?)",
		1,
		explicitZeroCold,
		42,
		0,
		23,
	)
	require.NoError(t, err)

	runTo(registry)
	var coldTag, termStart uint64
	var termStartSet bool
	require.NoError(t, db.QueryRow(
		"SELECT cold_credential_tag, term_start_slot, term_start_slot_set "+
			"FROM committee_member WHERE cold_cred_hash = ?",
		cold,
	).Scan(&coldTag, &termStart, &termStartSet))
	require.Zero(t, coldTag)
	require.Equal(t, uint64(17), termStart)
	require.True(t, termStartSet)

	var explicitTermStart uint64
	var explicitTermStartSet bool
	require.NoError(t, db.QueryRow(
		"SELECT term_start_slot, term_start_slot_set "+
			"FROM committee_member WHERE cold_cred_hash = ?",
		explicitZeroCold,
	).Scan(&explicitTermStart, &explicitTermStartSet))
	require.Zero(t, explicitTermStart)
	require.True(t, explicitTermStartSet)

	var authColdTag, authHotTag uint64
	require.NoError(t, db.QueryRow(
		"SELECT cold_credential_tag, hot_credential_tag "+
			"FROM auth_committee_hot WHERE cold_credential = ?",
		cold,
	).Scan(&authColdTag, &authHotTag))
	require.Zero(t, authColdTag)
	require.Zero(t, authHotTag)

	var resignColdTag uint64
	require.NoError(t, db.QueryRow(
		"SELECT cold_credential_tag FROM resign_committee_cold "+
			"WHERE cold_credential = ?",
		cold,
	).Scan(&resignColdTag))
	require.Zero(t, resignColdTag)

	_, err = db.Exec(
		"INSERT INTO committee_member "+
			"(cold_credential_tag, cold_cred_hash, expires_epoch, "+
			"term_start_slot, added_slot) VALUES (?, ?, ?, ?, ?)",
		1,
		cold,
		42,
		17,
		17,
	)
	require.NoError(
		t,
		err,
		"the migrated uniqueness constraint must preserve the credential tag",
	)
}

func TestCommitteeTermStartBackfillResumesAfterInterruption(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)

	run := func(versions []migrations.Migration) error {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: versions,
			Locker: migrations.NewFileLocker(
				databasePath + ".migrate.lock",
			),
		}
		return runner.Run(context.Background())
	}

	require.NoError(t, run(registry[:8]))
	for slot := int64(1); slot <= 3; slot++ {
		_, err = db.Exec(
			"INSERT INTO committee_member (cold_cred_hash, expires_epoch, added_slot) VALUES (?, ?, ?)",
			[]byte{byte(slot)},
			41,
			slot,
		)
		require.NoError(t, err)
	}

	interrupted := registry[8]
	interrupted.BatchSize = 1
	originalBackfill := interrupted.Backfill
	backfillCalls := 0
	interrupted.Backfill = func(ctx context.Context, batch migrations.Batch) (migrations.BatchResult, error) {
		backfillCalls++
		if backfillCalls == 2 {
			return migrations.BatchResult{}, errors.New(
				"intentional interruption",
			)
		}
		return originalBackfill(ctx, batch)
	}
	interruptedRegistry := append(
		append([]migrations.Migration{}, registry[:8]...),
		interrupted,
	)
	require.Error(t, run(interruptedRegistry))
	require.NoError(t, run(registry))

	var incomplete int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM committee_member WHERE NOT term_start_slot_set",
	).Scan(&incomplete))
	require.Zero(t, incomplete)
}

func TestCommitteeZeroQuorumMigrationConvertsLegacyClearMarkers(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	run := func(versions []migrations.Migration) {
		runner := migrations.Runner{
			DB: db, Dialect: "sqlite", Registry: versions,
			Locker: migrations.NewFileLocker(databasePath + ".migrate.lock"),
		}
		require.NoError(t, runner.Run(context.Background()))
	}
	run(registry[:22])
	_, err = db.Exec(
		"INSERT INTO committee_quorum (quorum, added_slot) VALUES ('0', 10)",
	)
	require.NoError(t, err)
	run(registry)

	var quorum sql.NullString
	require.NoError(t, db.QueryRow(
		"SELECT quorum FROM committee_quorum WHERE added_slot = 10",
	).Scan(&quorum))
	require.False(t, quorum.Valid, "legacy zero was a clear marker")
}

// testDBPragmas relaxes durability for throwaway per-test SQLite databases:
// each one is created, migrated, asserted against, and deleted inside a
// single test, so an fsync'd rollback journal buys nothing and is expensive
// on a contended CI runner (dingo#4171). No test in this package kills a
// connection mid-transaction, simulates crash recovery, or inspects a
// journal/WAL file, so relaxing durability does not change what any
// assertion observes. This is a twin of the identical constant in package
// migrations (runner_test.go); Go test files in the internal and external
// test packages for one directory compile separately and cannot share it.
const testDBPragmas = "_pragma=journal_mode(MEMORY)&_pragma=synchronous(OFF)"

// TestGovernanceProposalDropBackfillMarksAlreadyRefundedProposals covers
// dingo#4411's upgrade path. Before v17 the epoch tick refunded an expired
// proposal's deposit in the tick that marked it expired, so on an upgraded
// database every expired_epoch row has already been refunded. The drop step
// selects on `dropped_epoch IS NULL`, so without the v17 backfill it would
// match every historically expired proposal and refund each a second time at
// the first boundary after the upgrade.
func TestGovernanceProposalDropBackfillMarksAlreadyRefundedProposals(
	t *testing.T,
) {
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 31)
	runTo := func(versions []migrations.Migration) {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: versions,
			Locker: migrations.NewFileLocker(
				databasePath + ".migrate.lock",
			),
		}
		require.NoError(t, runner.Run(context.Background()))
	}

	// Pre-v17 schema, holding one proposal already expired (and, under that
	// schema's code, already refunded) and one still active.
	runTo(registry[:16])
	expiredHash := []byte{0xaa, 0xbb}
	activeHash := []byte{0xcc, 0xdd}
	_, err = db.Exec(
		"INSERT INTO governance_proposal (tx_hash, action_index, "+
			"action_type, proposed_epoch, expires_epoch, deposit, "+
			"expired_epoch, expired_slot, added_slot) "+
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		expiredHash, 0, 6, 670, 673, 100000000000, 674, 58100000, 57000000,
	)
	require.NoError(t, err)
	_, err = db.Exec(
		"INSERT INTO governance_proposal (tx_hash, action_index, "+
			"action_type, proposed_epoch, expires_epoch, deposit, "+
			"added_slot) VALUES (?, ?, ?, ?, ?, ?, ?)",
		activeHash, 0, 6, 676, 682, 100000000000, 58600000,
	)
	require.NoError(t, err)

	runTo(registry)

	var droppedEpoch, droppedSlot uint64
	require.NoError(t, db.QueryRow(
		"SELECT dropped_epoch, dropped_slot FROM governance_proposal_drop "+
			"JOIN governance_proposal "+
			"ON governance_proposal.id = "+
			"governance_proposal_drop.proposal_id "+
			"WHERE governance_proposal.tx_hash = ?",
		expiredHash,
	).Scan(&droppedEpoch, &droppedSlot),
		"an already-refunded expired proposal must be recorded as dropped")
	require.Equal(t, uint64(674), droppedEpoch)
	require.Equal(t, uint64(58100000), droppedSlot)

	// A proposal that never expired must not gain a drop row, or its
	// eventual expiry would never refund the deposit at all.
	var activeDropRows int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM governance_proposal_drop "+
			"JOIN governance_proposal "+
			"ON governance_proposal.id = "+
			"governance_proposal_drop.proposal_id "+
			"WHERE governance_proposal.tx_hash = ?",
		activeHash,
	).Scan(&activeDropRows))
	require.Zero(t, activeDropRows)
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
	require.Len(t, registry, 31)
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

// TestRewardAdaPotsImportedEpochFeesColumnIsAdditive covers dingo#3975's v24
// migration: a row written before the column existed must read back as NULL,
// not fail the migration or silently coerce to zero (zero is a legitimate
// "imported nothing before the anchor" value and must stay distinguishable
// from "no epoch was imported into this row").
func TestRewardAdaPotsImportedEpochFeesColumnIsAdditive(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 31)
	runTo := func(versions []migrations.Migration) {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: versions,
			Locker: migrations.NewFileLocker(
				databasePath + ".migrate.lock",
			),
		}
		require.NoError(t, runner.Run(context.Background()))
	}

	// Pre-v24 schema: write a row the way a live boundary always has, with
	// no imported_epoch_fees column to fill in.
	runTo(registry[:23])
	_, err = db.Exec(
		"INSERT INTO reward_ada_pots "+
			"(epoch, treasury, reserves, fees, rewards, captured_slot) "+
			"VALUES (?, ?, ?, ?, ?, ?)",
		100, "1000", "2000", "300", "0", 50000,
	)
	require.NoError(t, err)

	runTo(registry)

	var imported sql.NullString
	require.NoError(t, db.QueryRow(
		"SELECT imported_epoch_fees FROM reward_ada_pots WHERE epoch = ?",
		100,
	).Scan(&imported))
	require.False(t, imported.Valid,
		"a row written before the column existed must read back as NULL, "+
			"not as an implicit zero")
	var pending string
	require.ErrorIs(t, db.QueryRow(
		"SELECT value FROM sync_state WHERE sync_key = ?",
		"mithril_reward_repair_pending",
	).Scan(&pending), sql.ErrNoRows,
		"a legacy live database must not be marked for Mithril repair")

	// A row written after v24, the way seedImportedRewardBasis does, must
	// round-trip a real value including zero.
	_, err = db.Exec(
		"INSERT INTO reward_ada_pots "+
			"(epoch, treasury, reserves, fees, rewards, captured_slot, "+
			"imported_epoch_fees) VALUES (?, ?, ?, ?, ?, ?, ?)",
		101, "1000", "2000", "300", "0", 50100, "0",
	)
	require.NoError(t, err)
	require.NoError(t, db.QueryRow(
		"SELECT imported_epoch_fees FROM reward_ada_pots WHERE epoch = ?",
		101,
	).Scan(&imported))
	require.True(t, imported.Valid)
	require.Equal(t, "0", imported.String)
}

func TestRewardAdaPotsImportedFeesMigrationSchedulesLegacyMithrilRepair(
	t *testing.T,
) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runTo := func(versions []migrations.Migration) {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: versions,
			Locker: migrations.NewFileLocker(
				databasePath + ".migrate.lock",
			),
		}
		require.NoError(t, runner.Run(context.Background()))
	}
	runTo(registry[:23])
	_, err = db.Exec(`INSERT INTO sync_state (sync_key, value)
VALUES ('mithril_ledger_slot', '121763516')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO reward_ada_pots
(epoch, treasury, reserves, fees, rewards, captured_slot)
VALUES (1409, '1000', '2000', '300', '0', 121763516)`)
	require.NoError(t, err)

	runTo(registry)

	var pending string
	require.NoError(t, db.QueryRow(
		"SELECT value FROM sync_state WHERE sync_key = ?",
		"mithril_reward_repair_pending",
	).Scan(&pending))
	require.Equal(t, "1", pending)
}

func TestRewardRepairCoverageMigrationFindsMissingAnchorPotRow(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runTo := func(versions []migrations.Migration) {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: versions,
			Locker: migrations.NewFileLocker(
				databasePath + ".migrate.lock",
			),
		}
		require.NoError(t, runner.Run(context.Background()))
	}
	runTo(registry[:23])
	_, err = db.Exec(`INSERT INTO sync_state (sync_key, value)
VALUES ('mithril_ledger_slot', '121763516')`)
	require.NoError(t, err)
	// Older import paths could leave the durable anchor without a reward-pot
	// row. Version 24's row-based check cannot classify that database.
	runTo(registry[:24])
	var pending string
	require.ErrorIs(t, db.QueryRow(
		"SELECT value FROM sync_state WHERE sync_key = ?",
		"mithril_reward_repair_pending",
	).Scan(&pending), sql.ErrNoRows)

	// The follow-up backfill must schedule repair from the anchor itself so the
	// database cannot keep serving incomplete reward state.
	runTo(registry)
	require.NoError(t, db.QueryRow(
		"SELECT value FROM sync_state WHERE sync_key = ?",
		"mithril_reward_repair_pending",
	).Scan(&pending))
	require.Equal(t, "1", pending)
}

func TestRewardRepairCoverageMigrationKeepsCurrentMithrilImport(t *testing.T) {
	t.Parallel()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runTo := func(versions []migrations.Migration) {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: versions,
			Locker: migrations.NewFileLocker(
				databasePath + ".migrate.lock",
			),
		}
		require.NoError(t, runner.Run(context.Background()))
	}
	runTo(registry[:24])
	_, err = db.Exec(`INSERT INTO sync_state (sync_key, value)
VALUES ('mithril_ledger_slot', '121763516')`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO reward_ada_pots
(epoch, treasury, reserves, fees, rewards, captured_slot, imported_epoch_fees)
VALUES (1409, '1000', '2000', '300', '0', 121763516, '0')`)
	require.NoError(t, err)
	runTo(registry)

	var pending string
	require.ErrorIs(t, db.QueryRow(
		"SELECT value FROM sync_state WHERE sync_key = ?",
		"mithril_reward_repair_pending",
	).Scan(&pending), sql.ErrNoRows,
		"a Mithril anchor with an imported fee basis needs no repair")
}

// restampBackfillDB returns a database migrated through the version before
// the reward-stake calculation-version restamp backfill runs, so a test can
// seed legacy (version-1) snapshot rows for it to see.
func restampBackfillDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 31)
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry[:13],
		Locker: migrations.NewFileLocker(
			databasePath + ".migrate.lock",
		),
	}
	require.NoError(t, runner.Run(ctx))
	runRestamp := func() {
		full := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: registry,
			Locker: migrations.NewFileLocker(
				databasePath + ".migrate.lock",
			),
		}
		require.NoError(t, full.Run(ctx))
	}
	return db, runRestamp
}

func seedEpochSummary(
	t *testing.T,
	db *sql.DB,
	epoch uint64,
	totalActiveStake string,
) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
INSERT INTO epoch_summary (
    epoch, total_active_stake, total_pool_count, total_delegators,
    boundary_slot, snapshot_ready
) VALUES (?, ?, 0, 0, 0, TRUE)`,
		epoch, totalActiveStake,
	)
	require.NoError(t, err)
}

func seedLegacyPoolStakeSnapshot(
	t *testing.T,
	db *sql.DB,
	epoch uint64,
	poolKeyHash []byte,
) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
INSERT INTO pool_stake_snapshot (
    epoch, snapshot_type, pool_key_hash, total_stake, stake_denominator,
    delegator_count, captured_slot, calculation_version
) VALUES (?, 'mark', ?, '0', '0', 0, 0, 1)`,
		epoch, poolKeyHash,
	)
	require.NoError(t, err)
}

func seedLegacyRewardSnapshot(
	t *testing.T,
	db *sql.DB,
	epoch uint64,
	totalActiveStake string,
	authoritative bool,
) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
INSERT INTO reward_snapshot (
    epoch, snapshot_type, total_active_stake, total_pool_count,
    total_delegators, captured_slot, boundary_slot, protocol_version,
    authoritative, calculation_version
) VALUES (?, 'mark', ?, 0, 0, 0, 0, 9, ?, 1)`,
		epoch, totalActiveStake, authoritative,
	)
	require.NoError(t, err)
}

func calculationVersion(
	t *testing.T,
	db *sql.DB,
	table string,
	epoch uint64,
) int {
	t.Helper()
	var version int
	require.NoError(t, db.QueryRowContext(context.Background(),
		"SELECT calculation_version FROM "+table+" WHERE epoch = ?",
		epoch,
	).Scan(&version))
	return version
}

// A stale pool_stake_snapshot row is always safe to re-stamp: its stored
// totals never depended on calculation version (dingo #4026).
func TestRewardStakeVersionRestampAlwaysFixesPoolStakeSnapshot(t *testing.T) {
	t.Parallel()
	db, runRestamp := restampBackfillDB(t)
	seedLegacyPoolStakeSnapshot(t, db, 100, []byte("legacy-pool-key-hash-01"))

	runRestamp()

	require.Equal(t, 2, calculationVersion(t, db, "pool_stake_snapshot", 100))
}

// A stale Mark reward_snapshot row whose total_active_stake already agrees
// with the epoch's epoch_summary is re-stamped: that agreement is exactly
// what identifies an epoch the version bump did not actually change (dingo
// #4026 finding 1).
func TestRewardStakeVersionRestampFixesAgreeingRewardSnapshot(t *testing.T) {
	t.Parallel()
	db, runRestamp := restampBackfillDB(t)
	seedEpochSummary(t, db, 200, "1000")
	seedLegacyRewardSnapshot(t, db, 200, "1000", true)

	runRestamp()

	require.Equal(t, 2, calculationVersion(t, db, "reward_snapshot", 200))
}

// A stale Mark reward_snapshot row whose total_active_stake disagrees with
// epoch_summary names an epoch the version bump actually changed. The
// backfill must not paper over that with a re-stamp; it is left for
// StaleConsensusStakeSnapshotsExist to keep failing closed on.
func TestRewardStakeVersionRestampLeavesDisagreeingRewardSnapshotStale(
	t *testing.T,
) {
	t.Parallel()
	db, runRestamp := restampBackfillDB(t)
	seedEpochSummary(t, db, 300, "1000")
	seedLegacyRewardSnapshot(t, db, 300, "900", true)

	runRestamp()

	require.Equal(t, 1, calculationVersion(t, db, "reward_snapshot", 300))
}

// The non-authoritative fallback row is restamped by the same rule as an
// authoritative one: agreement with epoch_summary, not the authoritative
// flag, is what the backfill keys on.
func TestRewardStakeVersionRestampFixesAgreeingFallbackRewardSnapshot(
	t *testing.T,
) {
	t.Parallel()
	db, runRestamp := restampBackfillDB(t)
	seedEpochSummary(t, db, 400, "1000")
	seedLegacyRewardSnapshot(t, db, 400, "1000", false)

	runRestamp()

	require.Equal(t, 2, calculationVersion(t, db, "reward_snapshot", 400))
}

func TestSQLiteVersionOneSchemaIsDeterministic(t *testing.T) {
	t.Parallel()
	firstDB, err := migratedSQLiteV1(t)
	require.NoError(t, err)
	secondDB, err := migratedSQLiteV1(t)
	require.NoError(t, err)

	first, err := normalizedSQLiteSchema(firstDB)
	require.NoError(t, err)
	second, err := normalizedSQLiteSchema(secondDB)
	require.NoError(t, err)
	require.NotEmpty(t, first)
	require.Equal(t, first, second)
}

func migratedSQLiteV1(t *testing.T) (*sql.DB, error) {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	registry, err := migrations.SQLiteRegistry()
	if err != nil {
		return nil, err
	}
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker: migrations.NewFileLocker(
			databasePath + ".migrate.lock",
		),
	}
	if err := runner.Run(context.Background()); err != nil {
		return nil, err
	}
	return db, nil
}

func normalizedSQLiteSchema(db *sql.DB) ([]string, error) {
	rows, err := db.Query(
		`SELECT name FROM sqlite_master
		 WHERE type = 'table'
		   AND name NOT LIKE 'sqlite_%'
		   AND name <> 'schema_migrations'
		 ORDER BY name`,
	)
	if err != nil {
		return nil, err
	}
	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			_ = rows.Close()
			return nil, err
		}
		tables = append(tables, table)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var schema []string
	for _, table := range tables {
		schema = append(schema, "table:"+table)
		columns, err := db.Query(
			"PRAGMA table_info(" + quoteSQLite(table) + ")",
		)
		if err != nil {
			return nil, err
		}
		for columns.Next() {
			var (
				cid        int
				name       string
				columnType string
				notNull    int
				defaultVal sql.NullString
				primaryKey int
			)
			if err := columns.Scan(
				&cid,
				&name,
				&columnType,
				&notNull,
				&defaultVal,
				&primaryKey,
			); err != nil {
				_ = columns.Close()
				return nil, err
			}
			schema = append(schema, fmt.Sprintf(
				"column:%s:%d:%s:%s:%d:%s:%t:%d",
				table,
				cid,
				name,
				columnType,
				notNull,
				defaultVal.String,
				defaultVal.Valid,
				primaryKey,
			))
		}
		if err := columns.Close(); err != nil {
			return nil, err
		}
		if err := columns.Err(); err != nil {
			return nil, err
		}
		indexes, err := db.Query(
			"PRAGMA index_list(" + quoteSQLite(table) + ")",
		)
		if err != nil {
			return nil, err
		}
		type indexMetadata struct {
			name    string
			unique  int
			origin  string
			partial int
		}
		var tableIndexes []indexMetadata
		for indexes.Next() {
			var (
				sequence int
				name     string
				unique   int
				origin   string
				partial  int
			)
			if err := indexes.Scan(
				&sequence,
				&name,
				&unique,
				&origin,
				&partial,
			); err != nil {
				_ = indexes.Close()
				return nil, err
			}
			schema = append(schema, fmt.Sprintf(
				"index:%s:%s:%d:%s:%d",
				table,
				name,
				unique,
				origin,
				partial,
			))
			tableIndexes = append(tableIndexes, indexMetadata{
				name:    name,
				unique:  unique,
				origin:  origin,
				partial: partial,
			})
		}
		if err := indexes.Close(); err != nil {
			return nil, err
		}
		if err := indexes.Err(); err != nil {
			return nil, err
		}
		for _, index := range tableIndexes {
			indexColumns, err := db.Query(
				"PRAGMA index_info(" + quoteSQLite(index.name) + ")",
			)
			if err != nil {
				return nil, err
			}
			for indexColumns.Next() {
				var indexSequence, cid int
				var column string
				if err := indexColumns.Scan(
					&indexSequence,
					&cid,
					&column,
				); err != nil {
					_ = indexColumns.Close()
					return nil, err
				}
				schema = append(schema, fmt.Sprintf(
					"index-column:%s:%s:%d:%d:%s",
					table,
					index.name,
					indexSequence,
					cid,
					column,
				))
			}
			if err := indexColumns.Close(); err != nil {
				return nil, err
			}
			if err := indexColumns.Err(); err != nil {
				return nil, err
			}
		}
		foreignKeys, err := db.Query(
			"PRAGMA foreign_key_list(" + quoteSQLite(table) + ")",
		)
		if err != nil {
			return nil, err
		}
		for foreignKeys.Next() {
			var id, sequence int
			var parent, from, to, onUpdate, onDelete, match string
			if err := foreignKeys.Scan(
				&id,
				&sequence,
				&parent,
				&from,
				&to,
				&onUpdate,
				&onDelete,
				&match,
			); err != nil {
				_ = foreignKeys.Close()
				return nil, err
			}
			schema = append(schema, fmt.Sprintf(
				"foreign-key:%s:%s:%s:%s:%s:%s:%s",
				table,
				parent,
				from,
				to,
				onUpdate,
				onDelete,
				match,
			))
		}
		if err := foreignKeys.Close(); err != nil {
			return nil, err
		}
		if err := foreignKeys.Err(); err != nil {
			return nil, err
		}
	}
	sort.Strings(schema)
	return schema, nil
}

func quoteSQLite(identifier string) string {
	quoted := `"`
	for _, character := range identifier {
		quoted += string(character)
		if character == '"' {
			quoted += `"`
		}
	}
	return quoted + `"`
}
