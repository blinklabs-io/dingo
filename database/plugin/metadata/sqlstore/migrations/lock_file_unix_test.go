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
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// TestAssetAmountFingerprintIndexDropRemovesIndexes proves migration v21
// drops idx_asset_amount
// and idx_asset_fingerprint from a database migrated all the way through,
// while leaving the asset.amount and asset.fingerprint columns themselves
// untouched -- unlike asset.name_hex, both columns are still
// genuinely read and returned via the blockfrost/mesh API adapters. Before
// this migration existed, a fully migrated (through v20) database still
// carried both indexes: a real WAL-frame-churn measurement during genesis
// sync found neither backs any WHERE/JOIN/ORDER BY predicate anywhere in the
// tree.
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
// drops both asset.name_hex and
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

func TestCommitteeCredentialMigrationPreservesExistingRows(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 34)
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
// on a contended CI runner. No test in this package kills a
// connection mid-transaction, simulates crash recovery, or inspects a
// journal/WAL file, so relaxing durability does not change what any
// assertion observes. This is a twin of the identical constant in package
// migrations (runner_test.go); Go test files in the internal and external
// test packages for one directory compile separately and cannot share it.
const testDBPragmas = "_pragma=journal_mode(MEMORY)&_pragma=synchronous(OFF)"

// TestGovernanceProposalDropBackfillMarksAlreadyRefundedProposals covers
// upgrade path. Before v17 the epoch tick refunded an expired
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
	require.Len(t, registry, 34)
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

// TestRewardAdaPotsImportedEpochFeesColumnIsAdditive covers v24
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
	require.Len(t, registry, 34)
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
