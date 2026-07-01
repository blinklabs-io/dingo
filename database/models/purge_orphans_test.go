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

package models

import (
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// openForeignKeyMemoryDB opens an in-memory SQLite database with foreign
// key enforcement enabled, matching the production sqlite plugin. FK
// enforcement is required to reproduce the AutoMigrate table-rebuild
// failure from issue #2696.
func openForeignKeyMemoryDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf(
		"file:mem_%s?mode=memory&cache=shared&_pragma=foreign_keys(1)",
		t.Name(),
	)
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	return db
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func tableCount(t *testing.T, db *gorm.DB, table string) int64 {
	t.Helper()
	var n int64
	require.NoError(
		t,
		db.Table(table).Count(&n).Error,
		"counting rows in %s", table,
	)
	return n
}

// TestPurgeOrphanedCascadeRows_NoTables verifies the helper is a no-op on a
// database with none of the cascade-FK tables present.
func TestPurgeOrphanedCascadeRows_NoTables(t *testing.T) {
	db := openForeignKeyMemoryDB(t)
	require.NoError(t, PurgeOrphanedCascadeRows(db, discardLogger()))
}

// TestAutoMigrateFailsOnOrphansWithoutPurge documents the bug from #2696: a
// legacy table without the cascade FK that holds orphaned rows cannot be
// migrated because the table rebuild enforces the new constraint.
func TestAutoMigrateFailsOnOrphansWithoutPurge(t *testing.T) {
	db := openForeignKeyMemoryDB(t)
	require.NoError(t, db.Exec(
		`CREATE TABLE utxo (id integer PRIMARY KEY AUTOINCREMENT, transaction_id integer)`,
	).Error)
	require.NoError(t, db.Exec(
		`CREATE TABLE asset (id integer PRIMARY KEY AUTOINCREMENT, utxo_id integer)`,
	).Error)
	// Orphaned asset: its utxo does not exist.
	require.NoError(t, db.Exec(
		`INSERT INTO asset (id, utxo_id) VALUES (1, 999)`,
	).Error)

	err := db.AutoMigrate(&Utxo{}, &Asset{})
	require.Error(t, err, "AutoMigrate should fail while orphaned rows exist")
}

// TestPurgeOrphanedCascadeRows_UtxoAssetChain covers the reported case plus
// the two-level transaction -> utxo -> asset chain: orphaned utxos are
// removed, and assets belonging to those just-removed utxos are removed too.
// Rows with a NULL foreign key (e.g. genesis utxos) and valid rows survive,
// and AutoMigrate then succeeds.
func TestPurgeOrphanedCascadeRows_UtxoAssetChain(t *testing.T) {
	db := openForeignKeyMemoryDB(t)
	require.NoError(t, db.Exec(
		`CREATE TABLE "transaction" (id integer PRIMARY KEY AUTOINCREMENT)`,
	).Error)
	require.NoError(t, db.Exec(
		`CREATE TABLE utxo (id integer PRIMARY KEY AUTOINCREMENT, transaction_id integer, collateral_return_for_tx_id integer)`,
	).Error)
	require.NoError(t, db.Exec(
		`CREATE TABLE asset (id integer PRIMARY KEY AUTOINCREMENT, utxo_id integer)`,
	).Error)

	require.NoError(t, db.Exec(`INSERT INTO "transaction" (id) VALUES (1)`).Error)
	// utxo 1: valid; 2: orphan via transaction_id; 3: NULL fk (kept);
	// 4: orphan via collateral_return_for_tx_id.
	require.NoError(t, db.Exec(`INSERT INTO utxo (id, transaction_id, collateral_return_for_tx_id) VALUES (1, 1, NULL)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO utxo (id, transaction_id, collateral_return_for_tx_id) VALUES (2, 999, NULL)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO utxo (id, transaction_id, collateral_return_for_tx_id) VALUES (3, NULL, NULL)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO utxo (id, transaction_id, collateral_return_for_tx_id) VALUES (4, NULL, 999)`).Error)
	// asset 1: valid; 2: child of orphan utxo 2; 3: direct orphan;
	// 4: child of orphan utxo 4.
	require.NoError(t, db.Exec(`INSERT INTO asset (id, utxo_id) VALUES (1, 1)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO asset (id, utxo_id) VALUES (2, 2)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO asset (id, utxo_id) VALUES (3, 888)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO asset (id, utxo_id) VALUES (4, 4)`).Error)

	require.NoError(t, PurgeOrphanedCascadeRows(db, discardLogger()))

	require.EqualValues(t, 2, tableCount(t, db, "utxo"), "utxo 1 and 3 kept")
	require.EqualValues(t, 1, tableCount(t, db, "asset"), "only asset 1 kept")

	// The NULL-fk utxo must survive.
	var keptNull int64
	require.NoError(t, db.Table("utxo").
		Where("id = 3 AND transaction_id IS NULL").Count(&keptNull).Error)
	require.EqualValues(t, 1, keptNull, "NULL transaction_id utxo must be kept")

	require.NoError(
		t,
		db.AutoMigrate(&Transaction{}, &Utxo{}, &Asset{}),
		"AutoMigrate should succeed once orphans are purged",
	)
}

// TestPurgeOrphanedCascadeRows_PoolChain covers the pool -> registration ->
// owner/relay cascade chain and pool -> retirement.
func TestPurgeOrphanedCascadeRows_PoolChain(t *testing.T) {
	db := openForeignKeyMemoryDB(t)
	require.NoError(t, db.Exec(`CREATE TABLE pool (id integer PRIMARY KEY AUTOINCREMENT)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE pool_registration (id integer PRIMARY KEY AUTOINCREMENT, pool_id integer)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE pool_retirement (id integer PRIMARY KEY AUTOINCREMENT, pool_id integer)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE pool_registration_owner (id integer PRIMARY KEY AUTOINCREMENT, pool_registration_id integer, pool_id integer)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE pool_registration_relay (id integer PRIMARY KEY AUTOINCREMENT, pool_registration_id integer, pool_id integer)`).Error)

	require.NoError(t, db.Exec(`INSERT INTO pool (id) VALUES (1)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO pool_registration (id, pool_id) VALUES (1, 1)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO pool_registration (id, pool_id) VALUES (2, 999)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO pool_retirement (id, pool_id) VALUES (1, 1)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO pool_retirement (id, pool_id) VALUES (2, 999)`).Error)
	// owner/relay 1: valid; 2: child of orphan registration 2; 3: direct orphan.
	require.NoError(t, db.Exec(`INSERT INTO pool_registration_owner (id, pool_registration_id, pool_id) VALUES (1, 1, 1)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO pool_registration_owner (id, pool_registration_id, pool_id) VALUES (2, 2, 1)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO pool_registration_owner (id, pool_registration_id, pool_id) VALUES (3, 777, 1)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO pool_registration_relay (id, pool_registration_id, pool_id) VALUES (1, 1, 1)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO pool_registration_relay (id, pool_registration_id, pool_id) VALUES (2, 2, 1)`).Error)

	require.NoError(t, PurgeOrphanedCascadeRows(db, discardLogger()))

	require.EqualValues(t, 1, tableCount(t, db, "pool_registration"), "orphan registration purged")
	require.EqualValues(t, 1, tableCount(t, db, "pool_retirement"), "orphan retirement purged")
	require.EqualValues(t, 1, tableCount(t, db, "pool_registration_owner"), "orphan owners purged")
	require.EqualValues(t, 1, tableCount(t, db, "pool_registration_relay"), "orphan relays purged")

	require.NoError(t, db.AutoMigrate(
		&Pool{}, &PoolRegistration{}, &PoolRetirement{},
		&PoolRegistrationOwner{}, &PoolRegistrationRelay{},
	))
}

// TestPurgeOrphanedCascadeRows_MirRewards covers the MIR -> reward chain.
func TestPurgeOrphanedCascadeRows_MirRewards(t *testing.T) {
	db := openForeignKeyMemoryDB(t)
	require.NoError(t, db.Exec(`CREATE TABLE move_instantaneous_rewards (id integer PRIMARY KEY AUTOINCREMENT)`).Error)
	require.NoError(t, db.Exec(`CREATE TABLE move_instantaneous_rewards_reward (id integer PRIMARY KEY AUTOINCREMENT, mir_id integer)`).Error)

	require.NoError(t, db.Exec(`INSERT INTO move_instantaneous_rewards (id) VALUES (1)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO move_instantaneous_rewards_reward (id, mir_id) VALUES (1, 1)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO move_instantaneous_rewards_reward (id, mir_id) VALUES (2, 999)`).Error)

	require.NoError(t, PurgeOrphanedCascadeRows(db, discardLogger()))

	require.EqualValues(t, 1, tableCount(t, db, "move_instantaneous_rewards_reward"), "orphan reward purged")

	require.NoError(t, db.AutoMigrate(
		&MoveInstantaneousRewards{}, &MoveInstantaneousRewardsReward{},
	))
}

// TestPurgeOrphanedCascadeRows_AlreadyMigratedIsNoOp verifies that on a DB
// where the cascade FK already exists, the helper skips the purge scan and
// leaves valid rows untouched.
func TestPurgeOrphanedCascadeRows_AlreadyMigratedIsNoOp(t *testing.T) {
	db := openForeignKeyMemoryDB(t)
	require.NoError(t, db.AutoMigrate(&Transaction{}, &Utxo{}, &Asset{}))

	require.NoError(t, db.Exec(`INSERT INTO "transaction" (id) VALUES (1)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO utxo (id, transaction_id) VALUES (1, 1)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO asset (id, utxo_id) VALUES (1, 1)`).Error)

	require.NoError(t, PurgeOrphanedCascadeRows(db, discardLogger()))

	require.EqualValues(t, 1, tableCount(t, db, "utxo"))
	require.EqualValues(t, 1, tableCount(t, db, "asset"))
}
