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

func checkpointIdentityUpgradeDB(
	t *testing.T,
) (*sql.DB, func()) {
	t.Helper()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 52)
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
	runTo(registry[:51])
	return db, func() { runTo(registry) }
}

func TestBackfillCheckpointIdentityUpgradeResetsIncompleteMetadata(t *testing.T) {
	t.Parallel()
	db, upgrade := checkpointIdentityUpgradeDB(t)
	_, err := db.Exec(`
INSERT INTO backfill_checkpoint (
    phase, last_slot, total_slots, completed
) VALUES
    ('metadata', 90, 100, NULL),
    ('midnight', 70, 80, 0)`)
	require.NoError(t, err)

	upgrade()

	var (
		lastSlot      int64
		totalSlots    int64
		lastIndexID   sql.NullInt64
		lastBlockHash []byte
	)
	require.NoError(t, db.QueryRow(`
SELECT last_slot, total_slots, last_index_id, last_block_hash
FROM backfill_checkpoint
WHERE phase = 'metadata'`).Scan(
		&lastSlot, &totalSlots, &lastIndexID, &lastBlockHash,
	))
	require.Zero(t, lastSlot)
	require.Zero(t, totalSlots)
	require.False(t, lastIndexID.Valid)
	require.Nil(t, lastBlockHash)

	require.NoError(t, db.QueryRow(`
SELECT last_slot, total_slots, last_index_id, last_block_hash
FROM backfill_checkpoint
WHERE phase = 'midnight'`).Scan(
		&lastSlot, &totalSlots, &lastIndexID, &lastBlockHash,
	))
	require.Equal(t, int64(70), lastSlot)
	require.Equal(t, int64(80), totalSlots)
	require.False(t, lastIndexID.Valid)
	require.Nil(t, lastBlockHash)
}

func TestBackfillCheckpointIdentityUpgradePreservesCompletedMetadata(t *testing.T) {
	t.Parallel()
	db, upgrade := checkpointIdentityUpgradeDB(t)
	_, err := db.Exec(`
INSERT INTO backfill_checkpoint (
    phase, last_slot, total_slots, completed
) VALUES ('metadata', 90, 100, 1)`)
	require.NoError(t, err)

	upgrade()

	var (
		lastSlot   int64
		totalSlots int64
	)
	require.NoError(t, db.QueryRow(`
SELECT last_slot, total_slots
FROM backfill_checkpoint
WHERE phase = 'metadata'`).Scan(&lastSlot, &totalSlots))
	require.Equal(t, int64(90), lastSlot)
	require.Equal(t, int64(100), totalSlots)
}
