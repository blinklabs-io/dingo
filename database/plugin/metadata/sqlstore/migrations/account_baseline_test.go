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

// The v4 backfill has to give an already-bootstrapped Mithril database its
// account baselines, because those accounts have no certificate history a
// replay could rebuild them from. It covers exactly the imported and
// genesis-delegated rows (`created_slot = 0`) and records them as registered,
// which is the state both importers write.
func TestAccountImportBaselineBackfill(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 4)

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

	// Bring the database up to the schema that predates the baseline table.
	runTo(registry[:3])

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

	runTo(registry)

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

	var rows int
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM account_import_baseline`).Scan(&rows))
	require.Equal(t, 1, rows)

	// An upgrade interrupted after the backfill committed but before its phase
	// row advanced replays the same statements, so they have to be
	// re-runnable.
	for _, statement := range registry[3].SQL["sqlite"].Expand {
		_, err := db.ExecContext(ctx, statement)
		require.NoError(t, err)
	}
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM account_import_baseline`).Scan(&rows))
	require.Equal(t, 1, rows)
}
