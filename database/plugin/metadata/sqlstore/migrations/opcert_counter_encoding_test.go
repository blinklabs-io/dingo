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
	"math"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestOpCertCounterEncodingMigrationPreservesLegacyValues(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 51)
	run := func(selected []migrations.Migration) {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: selected,
			Locker: migrations.NewFileLocker(
				databasePath + ".migrate.lock",
			),
		}
		require.NoError(t, runner.Run(ctx))
	}
	run(registry[:48])

	poolKeyHash := []byte("legacy-opcert-pool-key-hash")
	_, err = db.ExecContext(ctx, `
INSERT INTO pool (pool_key_hash, latest_op_cert_sequence)
VALUES (?, ?)`, poolKeyHash, int64(math.MaxInt64))
	require.NoError(t, err)
	for slot, sequence := range []int64{0, math.MaxInt64} {
		_, err = db.ExecContext(ctx, `
INSERT INTO pool_opcert_sequence (pool_key_hash, slot, sequence)
VALUES (?, ?, ?)`, poolKeyHash, slot+1, sequence)
		require.NoError(t, err)
	}

	run(registry)

	var latest int64
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT latest_op_cert_sequence FROM pool WHERE pool_key_hash = ?`,
		poolKeyHash,
	).Scan(&latest))
	require.Equal(t, int64(-1), latest)

	rows, err := db.QueryContext(ctx, `
SELECT sequence FROM pool_opcert_sequence
WHERE pool_key_hash = ? ORDER BY sequence`, poolKeyHash)
	require.NoError(t, err)
	defer rows.Close()
	want := []int64{math.MinInt64, -1}
	for _, expected := range want {
		require.True(t, rows.Next())
		var sequence int64
		require.NoError(t, rows.Scan(&sequence))
		require.Equal(t, expected, sequence)
	}
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	opcertMigration := registry[48]
	require.Equal(t, 49, opcertMigration.Version)
	require.NotEmpty(t, opcertMigration.SQL["sqlite"].Expand)
	for _, statement := range opcertMigration.SQL["sqlite"].Expand {
		_, err = db.ExecContext(ctx, statement)
		require.NoError(t, err)
	}
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT latest_op_cert_sequence FROM pool WHERE pool_key_hash = ?`,
		poolKeyHash,
	).Scan(&latest))
	require.Equal(t, int64(-1), latest)
	rows, err = db.QueryContext(ctx, `
SELECT sequence FROM pool_opcert_sequence
WHERE pool_key_hash = ? ORDER BY sequence`, poolKeyHash)
	require.NoError(t, err)
	var replayed []int64
	for rows.Next() {
		var sequence int64
		require.NoError(t, rows.Scan(&sequence))
		replayed = append(replayed, sequence)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, want, replayed)
}
