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

package sqlstore

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestPoolRegistrationIdentityMigrationKeepsSyntheticReplayIdempotent(
	t *testing.T,
) {
	t.Parallel()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open(
		"sqlite",
		"file:"+databasePath+"?_pragma=foreign_keys(1)",
	)
	require.NoError(t, err)

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 42)
	run := func(versions []migrations.Migration) {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: versions,
			Locker:   migrations.NewProcessLocker(),
		}
		require.NoError(t, runner.Run(ctx))
	}
	run(registry[:41])

	poolKeyHash := []byte{0x42}
	result, err := db.ExecContext(ctx, `
INSERT INTO pool (pool_key_hash) VALUES (?)`, poolKeyHash)
	require.NoError(t, err)
	poolID, err := result.LastInsertId()
	require.NoError(t, err)
	result, err = db.ExecContext(ctx, `
INSERT INTO pool_registration (
    pool_id, pool_key_hash, added_slot, certificate_id
) VALUES (?, ?, 1000, NULL)`, poolID, poolKeyHash)
	require.NoError(t, err)
	registrationID, err := result.LastInsertId()
	require.NoError(t, err)

	run(registry)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(ctx))
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	registration := &models.PoolRegistration{
		PoolKeyHash: poolKeyHash,
		AddedSlot:   1000,
	}
	require.NoError(t, store.ImportPool(
		&models.Pool{PoolKeyHash: poolKeyHash},
		registration,
		nil,
	))
	require.Equal(t, uint(registrationID), registration.ID)

	var count int
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM pool_registration
WHERE pool_id = ? AND added_slot = 1000`, poolID).Scan(&count))
	require.Equal(t, 1, count)
}
