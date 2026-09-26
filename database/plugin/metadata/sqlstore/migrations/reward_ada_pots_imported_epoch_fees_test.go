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
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

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
	require.Len(t, registry, 25)
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
