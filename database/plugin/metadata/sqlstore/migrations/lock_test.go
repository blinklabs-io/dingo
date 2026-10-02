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
