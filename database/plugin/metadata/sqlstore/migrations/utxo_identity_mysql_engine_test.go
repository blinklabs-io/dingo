//go:build dingo_extra_plugins

// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package migrations_test

import (
	"context"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

// TestUtxoIdentityMySQLSwapResumesAfterRename leaves the migration on the
// post-rename side of its atomic swap and requires a restart to finish without
// dropping the migrated outputs or reusing the legacy autoincrement range.
func TestUtxoIdentityMySQLSwapResumesAfterRename(t *testing.T) {
	// Not t.Parallel: the MySQL engine fixtures create a fixed database name.
	e := newMySQLAssetSplitEngine(t)
	e.run(t, e.registry[:identityMigrationVersion-1])
	seedMySQLIdentityUpgradeRows(t, e)
	require.NoError(
		t,
		execMySQLIdentity(t, e, "ALTER TABLE `utxo` AUTO_INCREMENT = 50"),
	)

	stop := errors.New("stop before swap")
	registry := append(
		[]migrations.Migration(nil),
		e.registry[:identityMigrationVersion]...,
	)
	last := &registry[len(registry)-1]
	last.Backfill = migrations.UtxoIdentityBackfillForTest(
		"mysql", func() error { return stop },
	)
	err := (&migrations.Runner{
		DB: e.db, Dialect: "mysql", Registry: registry,
		Locker: migrations.NewProcessLocker(),
	}).Run(context.Background())
	require.ErrorIs(t, err, stop)
	require.True(t, e.tableExists(t, "utxo_identity_legacy"))
	require.False(t, e.tableExists(t, "utxo_identity"))

	require.NoError(t, (&migrations.Runner{
		DB: e.db, Dialect: "mysql",
		Registry: e.registry[:identityMigrationVersion],
		Locker:   migrations.NewProcessLocker(),
	}).Run(context.Background()), "restart after the atomic utxo rename")
	require.False(t, e.tableExists(t, "utxo_identity"))
	require.False(t, e.tableExists(t, "utxo_identity_legacy"))
	require.Equal(t, 2, queryInt(t, e.db, "SELECT COUNT(*) FROM utxo"))
	require.Equal(t, 2, queryInt(t, e.db,
		"SELECT COUNT(*) FROM utxo u JOIN `transaction` t ON t.id = u.tx_id"))

	var next int64
	require.NoError(t, e.db.QueryRow(`
SELECT auto_increment FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_name = 'utxo'`).Scan(&next))
	require.GreaterOrEqual(t, next, int64(50))
}

func seedMySQLIdentityUpgradeRows(t *testing.T, e *assetSplitEngine) {
	t.Helper()
	for i := 1; i <= 2; i++ {
		_, err := e.db.Exec(
			"INSERT INTO `transaction` "+
				"(id, hash, slot, block_index, fee, valid, block_hash) "+
				"VALUES (?, ?, ?, 0, '5', 1, ?)",
			i, identityHash(byte(i)), 100+i, identityHash(0xB0),
		)
		require.NoError(t, err)
	}
	_, err := e.db.Exec(`
INSERT INTO utxo (
    transaction_id, tx_id, output_idx, added_slot, deleted_slot,
    spent_at_tx_id, amount
) VALUES (1, ?, 0, 101, 200, ?, '10')`, identityHash(1), identityHash(2))
	require.NoError(t, err)
	_, err = e.db.Exec(`
INSERT INTO utxo (
    transaction_id, tx_id, output_idx, added_slot, deleted_slot, amount
) VALUES (2, ?, 0, 102, 0, '20')`, identityHash(2))
	require.NoError(t, err)
}

func execMySQLIdentity(
	t *testing.T,
	e *assetSplitEngine,
	statement string,
) error {
	t.Helper()
	_, err := e.db.Exec(statement)
	return err
}
