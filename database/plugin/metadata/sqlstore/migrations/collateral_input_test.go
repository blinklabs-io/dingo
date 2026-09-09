// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

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

func TestCollateralInputMigrationBackfillsLegacyMarker(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	run := func(registry []migrations.Migration) {
		runner := migrations.Runner{DB: db, Dialect: "sqlite", Registry: registry, Locker: migrations.NewProcessLocker()}
		require.NoError(t, runner.Run(context.Background()))
	}
	// Build a real v12 database, as an older installation would have existed.
	run(registry[:12])
	_, err = db.Exec(`INSERT INTO utxo (tx_id, output_idx, credential_tag, amount,
collateral_by_tx_id) VALUES (X'01', 0, 0, '1', X'02')`)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	db, err = sql.Open("sqlite", "file:"+dbPath)
	// Reopen the same file and run the real pending migration.
	require.NoError(t, err)
	run(registry)
	require.NoError(t, db.Close())
	db, err = sql.Open("sqlite", "file:"+dbPath)
	require.NoError(t, err)
	// A restart must be able to rerun the completed registry without changing
	// the migrated association or unrelated legacy rows.
	run(registry)
	var count int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM utxo_collateral_input WHERE transaction_hash = X'02'",
	).Scan(&count))
	require.Equal(t, 1, count)
}
